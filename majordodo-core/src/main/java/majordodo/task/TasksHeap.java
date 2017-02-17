/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package majordodo.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import majordodo.utils.IntCounter;
import majordodo.utils.IntCounter;

/**
 * Heap of tasks to be executed. Tasks are not arranged in a queue but in an heap.<br>
 * Important cases:<br>
 * <ul>
 * <li>A worker needs a task to be executed
 * <li>A clients submits a new task
 * <li>Compaction of the heap (removes empty slots)
 * <li>Readonly access for monitoring
 * </ul>
 *
 * @author enrico.olivelli
 */
public class TasksHeap {

    private static final Logger LOGGER = Logger.getLogger(TasksHeap.class.getName());

    private static final int TASKTYPE_ANYTASK = 0;

    private int actualsize;
    private int fragmentation;
    private int maxFragmentation;
    private int minValidPosition;
    private int autoGrowPercent = 25;
    private int size;
    private TaskEntry[] actuallist;
    private final TaskPropertiesMapperFunction resourceMapper;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<String, Integer> resourceMappings = new HashMap<>();
    private final Map<Integer, String> resourceIdMappings = new HashMap<>();
    private final Map<String, int[]> resourcesListPool = new HashMap<>();
    private final Map<int[], String[]> resourcesIdsListPool = new HashMap<>();
    private final Map<int[], String> resourcesStringListPool = new HashMap<>();

    public int getAutoGrowPercent() {
        return autoGrowPercent;
    }

    public void setAutoGrowPercent(int autoGrowPercent) {
        if (autoGrowPercent <= 0) {
            throw new IllegalArgumentException(autoGrowPercent + "");
        }
        this.autoGrowPercent = autoGrowPercent;
    }

    public int getActualsize() {
        return actualsize;
    }

    public int getFragmentation() {
        return fragmentation;
    }

    public int getSize() {
        return size;
    }

    public TasksHeap(int size, TaskPropertiesMapperFunction tenantAssigner) {
        this.size = size;
        this.resourceMapper = tenantAssigner;
        this.actuallist = new TaskEntry[size];
        for (int i = 0; i < size; i++) {
            this.actuallist[i] = new TaskEntry(0, 0, null, 0, null);
        }
        this.maxFragmentation = size / 4;
    }

    public int getMaxFragmentation() {
        return maxFragmentation;
    }

    public void setMaxFragmentation(int maxFragmentation) {
        this.maxFragmentation = maxFragmentation;
    }

    public void removeExpiredTasks(Set<Long> taskid) {
        lock.writeLock().lock();
        try {
            for (TaskEntry entry : actuallist) {
                if (taskid.contains(entry.taskid)) {
                    entry.taskid = 0;
                    entry.tasktype = 0;
                    entry.userid = null;
                    entry.groupid = 0;
                    entry.resources = null;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private final Map<String, Integer> taskTypesIds = new HashMap<>();
    private final Map<Integer, String> taskTypes = new HashMap<>();
    private int newIdtaskType = 0;

    private void doAutoGrow() {
        int delta = (int) (((actuallist.length * 1L * autoGrowPercent)) / 100);
        if (delta <= 0) {
            // be sure taht we always increment by one, in tore to have space for a new task
            delta = 1;
        }
        int newSize = actuallist.length + delta;
        LOGGER.log(Level.SEVERE, "doAutoGrow size {0}, newsize {1}", new Object[]{size, newSize});
        TaskEntry[] newList = new TaskEntry[newSize];
        System.arraycopy(actuallist, 0, newList, 0, actuallist.length);
        for (int i = actuallist.length; i < newList.length; i++) {
            newList[i] = new TaskEntry(0, 0, null, 0, null);
        }
        this.size = newList.length;
        this.actuallist = newList;
    }

    public void insertTask(long taskid, String tasktype, String userid) {
        TaskProperties taskProperties = resourceMapper.getTaskProperties(taskid, tasktype, userid);
        int groupid = taskProperties.groupId;
        String[] resourceIds = taskProperties.resources;
        lock.writeLock().lock();
        try {
            int[] resources = convertResourceList(resourceIds);
            if (actualsize == size) {
                doAutoGrow();
            }
            Integer taskTypeId = taskTypesIds.get(tasktype);
            if (taskTypeId == null) {
                taskTypeId = ++newIdtaskType;
                taskTypesIds.put(tasktype, taskTypeId);
                taskTypes.put(taskTypeId, tasktype);
            }
            TaskEntry entry = this.actuallist[actualsize++];
            entry.taskid = taskid;
            entry.tasktype = taskTypeId;
            entry.userid = userid;
            entry.groupid = groupid;
            entry.resources = resources;
        } finally {
            lock.writeLock().unlock();
        }
    }

    String resolveTaskType(int tasktype) {
        return taskTypes.get(tasktype);
    }

    private String[] convertResourceListToIds(int[] resourceIds) {
        return resourcesIdsListPool.get(resourceIds);
    }

    private String convertResourceListString(int[] resourceIds) {
        return resourcesStringListPool.get(resourceIds);
    }

    private int[] convertResourceList(String[] resourceIds) {
        // this method must be invoked inside a writeLock
        if (resourceIds == null || resourceIds.length == 0) {
            return null;
        }
        String key = Arrays.toString(resourceIds);
        int[] pooled = resourcesListPool.get(key);
        if (pooled != null) {
            return pooled;
        }
        int[] result = new int[resourceIds.length];
        int i = 0;
        for (String s : resourceIds) {
            Integer id = resolveResourceId(s);
            result[i++] = id;
        }
        /* we are going to pool the array for two reasons:
         - limit memory usage
         - have the ability to compare arrays using reference comparisons
         */
        resourcesListPool.put(key, result);
        resourcesIdsListPool.put(result, resourceIds);
        resourcesStringListPool.put(result, Stream.of(resourceIds).collect(Collectors.joining(",")));
        return result;
    }

    public static final class TaskEntry {

        public long taskid;
        public int tasktype;
        public String userid;
        public int groupid;
        public int[] resources;

        TaskEntry(long taskid, int tasktype, String userid, int groupid, int[] resources) {
            this.taskid = taskid;
            this.tasktype = tasktype;
            this.userid = userid;
            this.groupid = groupid;
            this.resources = resources;
        }

        @Override
        public String toString() {
            return "TaskEntry{" + "taskid=" + taskid + ", tasktype=" + tasktype + ", userid=" + userid + '}';
        }

    }

    public void scan(Consumer<TaskEntry> consumer) {
        lock.readLock().lock();
        try {
            for (int i = minValidPosition; i < actualsize; i++) {
                TaskEntry entry = this.actuallist[i];
                if (entry.taskid > 0) {
                    consumer.accept(entry);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void scanFull(Consumer<TaskEntry> consumer) {
        lock.readLock().lock();
        try {
            for (int i = 0; i < actualsize; i++) {
                TaskEntry entry = this.actuallist[i];
                consumer.accept(entry);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void recomputeGroups() {
        lock.writeLock().lock();
        try {
            for (int i = minValidPosition; i < actualsize; i++) {
                TaskEntry entry = this.actuallist[i];
                if (entry.taskid > 0) {
                    TaskProperties taskProperties = resourceMapper.getTaskProperties(entry.taskid, taskTypes.get(entry.tasktype), entry.userid);
                    int newGroup = taskProperties.groupId;
                    int[] resources = convertResourceList(taskProperties.resources);
                    // we can compare the "resources" array using the reference because we are pooling them
                    if (entry.groupid != newGroup || entry.resources != resources) {
                        // let's limit writes on memory, most often group/resources does not change
                        entry.groupid = newGroup;
                        entry.resources = resources;
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void runCompaction() {
        LOGGER.log(Level.FINEST, "running compaction,"
            + "fragmentation " + fragmentation + ", actualsize " + actualsize
            + ", size " + size + ", minValidPosition " + minValidPosition);
        lock.writeLock().lock();
        try {
            int[] nonemptypositions = new int[size];
            int insertpos = 0;
            int pos = 0;
            for (TaskEntry entry : actuallist) {
                if (entry.taskid > 0) {
                    nonemptypositions[insertpos++] = pos + 1; // NOTE_A: 0 means "empty", so we are going to add "+1" to every position
                }
                pos++;
            }
            int writepos = 0;
            for (int nonemptyindex = 0; nonemptyindex < size; nonemptyindex++) {
                int nextnotempty = nonemptypositions[nonemptyindex];
                if (nextnotempty == 0) {
                    break;
                }
                nextnotempty = nextnotempty - 1; // see NOTE_A
                actuallist[writepos].taskid = actuallist[nextnotempty].taskid;
                actuallist[writepos].tasktype = actuallist[nextnotempty].tasktype;
                actuallist[writepos].userid = actuallist[nextnotempty].userid;
                actuallist[writepos].groupid = actuallist[nextnotempty].groupid;
                actuallist[writepos].resources = actuallist[nextnotempty].resources;
                writepos++;
            }
            for (int j = writepos; j < size; j++) {
                actuallist[j].taskid = 0;
                actuallist[j].tasktype = 0;
                actuallist[j].userid = null;
                actuallist[j].groupid = 0;
                actuallist[j].resources = null;
            }

            minValidPosition = 0;
            actualsize = writepos + 1;
            fragmentation = 0;
            LOGGER.log(Level.SEVERE, "after compaction, fragmentation " + fragmentation + ", actualsize " + actualsize + ", size " + size + ", minValidPosition " + minValidPosition);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<AssignedTask> takeTasks(int max, List<Integer> groups, Set<Integer> excludedGroups, Map<String, Integer> availableSpace,
        Map<String, Integer> workerResourceLimits, ResourceUsageCounters workerResourceUsageCounters,
        Map<String, Integer> globalResourceLimits, ResourceUsageCounters globalResourceUsageCounters,
        Map<TaskTypeUser, IntCounter> availableSpacePerUser) {
        Map<Integer, Integer> availableSpaceByTaskTaskId = new HashMap<>();
        Integer forAny = availableSpace.get(Task.TASKTYPE_ANY);
        if (forAny != null) {
            availableSpaceByTaskTaskId.put(TasksHeap.TASKTYPE_ANYTASK, forAny);
        }

        Map<Integer, IntCounter> availableResourcesCounters = new HashMap<>();

        // takeTasks for a single worker is guaranteed to be executed not concurrenly, we can run this code out of the lock
        workerResourceUsageCounters.updateResourceCounters();
        if (workerResourceLimits != null && !workerResourceLimits.isEmpty()) {
            computeAvailableResources(workerResourceLimits, availableResourcesCounters, workerResourceUsageCounters);
        }

        lock.writeLock().lock();
        try {

            // global counters but be modified only inside the "global" lock
            globalResourceUsageCounters.updateResourceCounters();
            if (globalResourceLimits != null && !globalResourceLimits.isEmpty()) {
                computeAvailableResources(globalResourceLimits, availableResourcesCounters, globalResourceUsageCounters);
            }

            for (Map.Entry<String, Integer> entry : availableSpace.entrySet()) {
                Integer typeId = taskTypesIds.get(entry.getKey());
                if (typeId != null) {
                    availableSpaceByTaskTaskId.put(typeId, entry.getValue());
                }
            }
            Map<TasksChooser.IntTaskTypeUser, IntCounter> _availableSpacePerUser = null;
            if (availableSpacePerUser != null) {
                _availableSpacePerUser = new HashMap<>(availableSpacePerUser.size());
                for (Map.Entry<TaskTypeUser, IntCounter> entry : availableSpacePerUser.entrySet()) {
                    TaskTypeUser taskTypeUser = entry.getKey();
                    Integer typeId = taskTypesIds.get(taskTypeUser.taskType);
                    if (typeId != null) {
                        _availableSpacePerUser.put(new TasksChooser.IntTaskTypeUser(typeId, taskTypeUser.userId), entry.getValue());
                    }
                }
            }

            TasksChooser chooser = new TasksChooser(groups, excludedGroups, availableSpaceByTaskTaskId, availableResourcesCounters, max,
                _availableSpacePerUser);
            for (int i = minValidPosition; i < actualsize; i++) {
                TaskEntry entry = this.actuallist[i];
                if (entry.taskid > 0) {
                    chooser.accept(i, entry);
                }
            }
            List<TasksChooser.Entry> choosen = chooser.getChoosenTasks();
            if (choosen.isEmpty()) {
                return Collections.emptyList();
            }
            List<AssignedTask> result = new ArrayList<>();
            for (TasksChooser.Entry choosenentry : choosen) {
                int pos = choosenentry.position;
                TaskEntry entry = this.actuallist[pos];
                if (entry.taskid == choosenentry.taskid) {
                    entry.taskid = 0;
                    entry.tasktype = 0;
                    entry.userid = null;
                    this.fragmentation++;
                    result.add(new AssignedTask(choosenentry.taskid, convertResourceListToIds(entry.resources), convertResourceListString(entry.resources)));
                    if (pos == minValidPosition) {
                        minValidPosition++;
                    }
                }
            }
            if (this.fragmentation > maxFragmentation) {
                runCompaction();
            }
            return result;
        } finally {
            lock.writeLock().unlock();
        }

    }

    private void computeAvailableResources(
        Map<String, Integer> limitsConfigurations,
        Map<Integer, IntCounter> availableResourcesCounters,
        ResourceUsageCounters actualUsages) {
        for (Map.Entry<String, Integer> limitFromConfiguration : limitsConfigurations.entrySet()) {
            String resourceId = limitFromConfiguration.getKey();
            int limitOnResource = limitFromConfiguration.getValue();
            if (limitOnResource > 0) {
                Integer idresource = resolveResourceId(resourceId);
                IntCounter availableForResource = availableResourcesCounters.get(idresource);
                IntCounter actualUsage = actualUsages.counters.get(resourceId);
                if (availableForResource != null) {
                    // resource already limited
                    if (actualUsage != null) {
                        int newLimit = limitOnResource - actualUsage.count;
                        availableForResource.count = Math.min(availableForResource.count, newLimit);
                    } else {
                        availableForResource.count = Math.min(availableForResource.count, limitOnResource);
                    }
                } else {
                    // new resource, limit is the configured one minus the actual usage
                    availableForResource = new IntCounter(limitOnResource);
                    availableResourcesCounters.put(idresource, availableForResource);
                    if (actualUsage != null) {
                        availableForResource.count -= actualUsage.count;
                    }
                }
            }

        }
    }

    private Integer resolveResourceId(String resourceId) {
        Integer idresource = resourceMappings.get(resourceId);
        if (idresource == null) {
            idresource = resourceMappings.size() + 1;
            resourceMappings.put(resourceId, idresource);
            resourceIdMappings.put(idresource, resourceId);
        }
        return idresource;
    }

}

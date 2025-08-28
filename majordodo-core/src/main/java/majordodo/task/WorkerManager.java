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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime status manager for a Node
 *
 * @author enrico.olivelli
 */
public class WorkerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerManager.class);

    private final String workerId;
    private final Broker broker;
    private BrokerSideConnection connection;
    private final int maxWorkerIdleTime;
    private final ResourceUsageCounters resourceUsageCounters;

    private int maxThreads = 0;
    private int maxThreadPerUserPerTaskTypePercent = 0;
    private Map<String, Integer> maxThreadsByTaskType = Collections.emptyMap();
    private List<Integer> groups = Collections.emptyList();
    private Set<Integer> excludedGroups = Collections.emptySet();
    private Map<String, Integer> resourceLimis = Collections.emptyMap();

    private long lastActivity = System.currentTimeMillis();

    public WorkerManager(String workerId, Broker broker) {
        if (workerId == null) {
            throw new NullPointerException();
        }
        this.workerId = workerId;
        this.broker = broker;
        this.maxWorkerIdleTime = broker.getConfiguration().getMaxWorkerIdleTime();
        this.resourceUsageCounters = new ResourceUsageCounters("worker-" + workerId + "-" + broker.getBrokerId());
    }

    public void applyConfiguration(int maxThreads,
                                   Map<String, Integer> maxThreadsByTaskType,
                                   List<Integer> groups,
                                   Set<Integer> excludedGroups,
                                   Map<String, Integer> resourceLimis,
                                   int maxThreadPerUserPerTaskTypePercent) {
        LOGGER.debug("{} applyConfiguration maxThreads {} maxThreadPerUserPerTaskTypePercent {} ", workerId, maxThreads);
        this.maxThreads = maxThreads;
        this.maxThreadPerUserPerTaskTypePercent = maxThreadPerUserPerTaskTypePercent;
        Map<String, Integer> maxThreadsByTaskTypeNoZero = new HashMap<>(maxThreadsByTaskType);
        for (Iterator<Map.Entry<String, Integer>> it = maxThreadsByTaskTypeNoZero.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Integer> entry = it.next();
            if (entry.getValue() == null || entry.getValue() <= 0) {
                it.remove();
            }
        }
        this.maxThreadsByTaskType = maxThreadsByTaskTypeNoZero;
        this.groups = groups;
        this.excludedGroups = excludedGroups;
        this.resourceLimis = resourceLimis;
    }

    private void requestNewTasks() {
        long _start = System.currentTimeMillis();
        int max = this.maxThreads;
        try {
            Map<String, Integer> availableSpace = new HashMap<>(this.maxThreadsByTaskType);
            int actuallyRunning = broker.getBrokerStatus().applyRunningTasksFilterToAssignTasksRequest(workerId, availableSpace);
            LOGGER.debug("{} requestNewTasks actuallyRunning {} max {} groups {},excludedGroups {} availableSpace {}, maxThreadsByTaskType {}, maxThreadPerUserPerTaskTypePercent {} ", workerId, availableSpace + "", actuallyRunning, max, groups, excludedGroups, maxThreadsByTaskType, maxThreadPerUserPerTaskTypePercent);
            max = max - actuallyRunning;
            List<AssignedTask> tasks;
            if (max > 0 && !availableSpace.isEmpty()) {
                tasks = broker.assignTasksToWorker(max, availableSpace, groups, excludedGroups, workerId,
                        resourceLimis, resourceUsageCounters, maxThreadPerUserPerTaskTypePercent);
                tasks.forEach(this::taskAssigned);
            } else {
                tasks = Collections.emptyList();
            }
            long _stop = System.currentTimeMillis();

            if (!tasks.isEmpty()) {
                LOGGER.debug("{} assigned {} tasks, time {} ms", workerId, tasks.size(), _stop - _start);
            }
        } catch (Exception error) {
            LOGGER.error("error assigning tasks", error);
        }
    }

    public Broker getBroker() {
        return broker;
    }

    private volatile boolean threadAssigned;

    public boolean isThreadAssigned() {
        return threadAssigned;
    }

    public void threadAssigned() {
        threadAssigned = true;
    }

    public Runnable operation() {
        return new Runnable() {
            @Override
            public void run() {
                String name = Thread.currentThread().getName();
                try {
                    Thread.currentThread().setName(name + "_" + workerId);
                    manageWorker();
                } finally {
                    Thread.currentThread().setName(name);
                    threadAssigned = false;
                }
            }
        };
    }

    private void manageWorker() {
        if (broker.isStopped() || !broker.isWritable()) {
            return;
        }
        WorkerStatus status = broker.getBrokerStatus().getWorkerStatus(workerId);
        if (status == null) {
            LOGGER.error("wakeup {} -> no status?", workerId);
            return;
        }
        if (status.getStatus() == WorkerStatus.STATUS_DEAD) {
            return;
        }
        long now = System.currentTimeMillis();
        connectionLock.lock();
        try {
            if (connection != null && !connection.validate()) {
                connection.close();
                deactivateConnection(connection);
                connection = null;
            }
            if (connection == null) {
                LOGGER.debug("wakeup {} -> no connection", workerId);
                try {
                    if (status.getStatus() == WorkerStatus.STATUS_CONNECTED) {
                        LOGGER.debug("wakeup {} -> no connection -> setting STATUS_DISCONNECTED", workerId);
                        broker.declareWorkerDisconnected(workerId, now);
                    } else {
                        long delta = now - lastActivity;
                        if (delta > maxWorkerIdleTime) {
                            LOGGER.error("wakeup {} -> declaring dead (connection did not reestabilish in time)", workerId);
                            broker.declareWorkerDead(workerId, now);

                            LOGGER.error("wakeup {} -> requesting recovery for tasks {}", workerId, tasksRunningOnRemoteWorker);
                            broker.tasksNeedsRecoveryDueToWorkerDeath(tasksRunningOnRemoteWorker, workerId);
                            tasksRunningOnRemoteWorker.clear();
                        }
                    }
                } catch (LogNotAvailableException err) {
                    LOGGER.error("wakeup " + workerId + " -> worker lifecycle error", err);
                }
            } else {
                if (lastActivity < connection.getLastReceivedMessageTs()) {
                    lastActivity = connection.getLastReceivedMessageTs();
                }
                LOGGER.debug("wakeup {}, lastActivity {}  taskToBeSubmittedToRemoteWorker {} tasksRunningOnRemoteWorker {}", workerId, new java.util.Date(lastActivity), taskToBeSubmittedToRemoteWorker, tasksRunningOnRemoteWorker);
                requestNewTasks();
                int max = 100;
                while (max-- > 0) {
                    AssignedTask taskToBeSubmitted = taskToBeSubmittedToRemoteWorker.poll();
                    if (taskToBeSubmitted != null) {
                        long taskId = taskToBeSubmitted.taskid;
                        LOGGER.debug("wakeup {} -> assign task {}", workerId, taskId);
                        Task task = broker.getBrokerStatus().getTask(taskId);
                        if (task == null) {
                            // task disappeared ?
                            LOGGER.error("wakeup {} -> assign task {}, task disappeared?", workerId, taskToBeSubmitted);
                        } else {
                            if (tasksRunningOnRemoteWorker.contains(taskToBeSubmitted.taskid)) {
                                LOGGER.error("wakeup {} -> assign task {}, task {} is already running on worker", workerId, taskToBeSubmitted, task);
                                return;
                            }
                            if (task.getStatus() == Task.STATUS_RUNNING && task.getWorkerId().equals(workerId)) {
                                connection.sendTaskAssigned(task, (Void result, Throwable error) -> {
                                    if (error != null) {
                                        // the write failed
                                        LOGGER.error("wakeup {} -> assign task {}, task {} network failure, rescheduling for retry:{}", workerId, taskToBeSubmitted, task, error);
                                        taskToBeSubmittedToRemoteWorker.add(taskToBeSubmitted);
                                    } else {
                                        tasksRunningOnRemoteWorker.add(taskToBeSubmitted.taskid);
                                    }
                                });
                            } else {
                                LOGGER.error("wakeup {} -> assign task {}, task {} not in running status for this worker", workerId, taskToBeSubmitted, task);
                            }
                        }
                    } else {
                        break;
                    }
                }

            }
        } finally {
            connectionLock.unlock();
        }
    }

    private final Set<Long> tasksRunningOnRemoteWorker = new ConcurrentSkipListSet<>();
    private final BlockingQueue<AssignedTask> taskToBeSubmittedToRemoteWorker = new LinkedBlockingDeque<>();

    public void activateConnection(BrokerSideConnection connection) {
        LOGGER.info("activateConnection {}", connection);
        connectionLock.lock();
        try {
            lastActivity = System.currentTimeMillis();
            this.connection = connection;
        } finally {
            connectionLock.unlock();
        }
    }

    void taskRunningDuringBrokerBoot(AssignedTask takenTask) {
        long taskId = takenTask.taskid;
        LOGGER.debug("{} taskShouldBeRunning {}, resources {}", workerId, taskId, takenTask.resources);
        tasksRunningOnRemoteWorker.add(taskId);
        resourceUsageCounters.useResources(takenTask.resourceIds);
        taskToBeSubmittedToRemoteWorker.remove(takenTask);
    }

    private void taskAssigned(AssignedTask takenTask) {
        long taskId = takenTask.taskid;
        taskToBeSubmittedToRemoteWorker.add(takenTask);
        resourceUsageCounters.useResources(takenTask.resourceIds);
        if (tasksRunningOnRemoteWorker.contains(taskId)) {
            LOGGER.error("taskAssigned {}, the task is already running on remote worker?", taskId);
        }
    }

    private final ReentrantLock connectionLock = new ReentrantLock();

    public void deactivateConnection(BrokerSideConnection aThis) {
        connectionLock.lock();
        try {
            LOGGER.info("deactivateConnection {}", connection);
            if (this.connection == aThis) {
                this.connection = null;
            }
        } finally {
            connectionLock.unlock();
        }
    }

    void taskFinished(long taskId) {
        tasksRunningOnRemoteWorker.remove(taskId);
    }

    void releaseResources(String[] resourceIds) {
        resourceUsageCounters.releaseResources(resourceIds);
    }

    public ResourceUsageCounters getResourceUsageCounters() {
        return resourceUsageCounters;
    }

    public Map<String, Integer> getResourceLimis() {
        return resourceLimis;
    }

}

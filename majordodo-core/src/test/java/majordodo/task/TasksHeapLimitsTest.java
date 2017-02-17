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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import majordodo.utils.IntCounter;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;

public class TasksHeapLimitsTest {

    private static final String TASKTYPE_MYTASK1 = "MYTASK1";
    private static final String TASKTYPE_MYTASK2 = "MYTASK2";
    private static final String USERID1 = "myuser1";
    private static final String USERID2 = "myuser2";
    private static final int GROUPID1 = 9713;
    private static final int GROUPID2 = 972;
    private static final String RESOURCE1 = "db1";
    private static final String RESOURCE2 = "db2";

    private final TaskPropertiesMapperFunction DEFAULT_FUNCTION = new TaskPropertiesMapperFunction() {
        @Override
        public TaskProperties getTaskProperties(long taskid, String taskType, String userid) {
            int groupId;
            switch (userid) {
                case USERID1:
                    groupId = GROUPID1;
                    break;
                case USERID2:
                    groupId = GROUPID2;
                    break;
                default:
                    groupId = -1;
            }
            return new TaskProperties(groupId, new String[]{RESOURCE1, RESOURCE2});
        }

    };

    @Test
    public void test_worker_limits() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 10000);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        }
        ResourceUsageCounters globalCounters = new ResourceUsageCounters();
        ResourceUsageCounters workerCounters = new ResourceUsageCounters();
        Map<String, Integer> workerLimitsConfiguration = new HashMap<>();
        workerLimitsConfiguration.put(RESOURCE1, 5);
        workerLimitsConfiguration.put(RESOURCE2, 10);
        Map<String, Integer> globalLimitsConfiguration = new HashMap<>();
        List<AssignedTask> taskids = instance.takeTasks(1000, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace, workerLimitsConfiguration, workerCounters, globalLimitsConfiguration, globalCounters, null);
        assertEquals(5, taskids.size());
    }

    @Test
    public void test_global_limits() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 10000);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        }
        ResourceUsageCounters globalCounters = new ResourceUsageCounters();
        ResourceUsageCounters workerCounters = new ResourceUsageCounters();
        Map<String, Integer> workerLimitsConfiguration = new HashMap<>();

        Map<String, Integer> globalLimitsConfiguration = new HashMap<>();
        globalLimitsConfiguration.put(RESOURCE1, 5);
        globalLimitsConfiguration.put(RESOURCE2, 10);
        List<AssignedTask> taskids = instance.takeTasks(1000, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace, workerLimitsConfiguration, workerCounters, globalLimitsConfiguration, globalCounters, null);
        assertEquals(5, taskids.size());
    }

    @Test
    public void test_worker_and_global_limits() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 10000);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        }
        ResourceUsageCounters globalCounters = new ResourceUsageCounters();
        ResourceUsageCounters workerCounters = new ResourceUsageCounters();
        Map<String, Integer> workerLimitsConfiguration = new HashMap<>();
        workerLimitsConfiguration.put(RESOURCE1, 5);
        workerLimitsConfiguration.put(RESOURCE2, 10);

        Map<String, Integer> globalLimitsConfiguration = new HashMap<>();
        globalLimitsConfiguration.put(RESOURCE1, 7);
        globalLimitsConfiguration.put(RESOURCE2, 10);
        List<AssignedTask> taskids = instance.takeTasks(1000, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace, workerLimitsConfiguration, workerCounters, globalLimitsConfiguration, globalCounters, null);
        assertEquals(5, taskids.size());
    }

    @Test
    public void test_global_and_worker_limits() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 10000);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        }
        ResourceUsageCounters globalCounters = new ResourceUsageCounters();
        ResourceUsageCounters workerCounters = new ResourceUsageCounters();
        Map<String, Integer> workerLimitsConfiguration = new HashMap<>();
        workerLimitsConfiguration.put(RESOURCE1, 7);
        workerLimitsConfiguration.put(RESOURCE2, 10);

        Map<String, Integer> globalLimitsConfiguration = new HashMap<>();
        globalLimitsConfiguration.put(RESOURCE1, 5);
        globalLimitsConfiguration.put(RESOURCE2, 10);
        List<AssignedTask> taskids = instance.takeTasks(1000, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace, workerLimitsConfiguration, workerCounters, globalLimitsConfiguration, globalCounters, null);
        assertEquals(5, taskids.size());
    }

    @Test
    public void test_used_resource_1() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 10000);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        }
        ResourceUsageCounters globalCounters = new ResourceUsageCounters();
        ResourceUsageCounters workerCounters = new ResourceUsageCounters();
        workerCounters.counters.put(RESOURCE1, new IntCounter(2));
        globalCounters.counters.put(RESOURCE1, new IntCounter(3));
        Map<String, Integer> workerLimitsConfiguration = new HashMap<>();
        workerLimitsConfiguration.put(RESOURCE1, 7);
        workerLimitsConfiguration.put(RESOURCE2, 10);

        Map<String, Integer> globalLimitsConfiguration = new HashMap<>();
        globalLimitsConfiguration.put(RESOURCE1, 5);
        globalLimitsConfiguration.put(RESOURCE2, 10);
        List<AssignedTask> taskids = instance.takeTasks(1000, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace, workerLimitsConfiguration, workerCounters, globalLimitsConfiguration, globalCounters, null);
        assertEquals(2, taskids.size());
    }

    @Test
    public void test_used_resource_2() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 10000);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        }
        ResourceUsageCounters globalCounters = new ResourceUsageCounters();
        ResourceUsageCounters workerCounters = new ResourceUsageCounters();
        workerCounters.counters.put(RESOURCE1, new IntCounter(7));
        globalCounters.counters.put(RESOURCE1, new IntCounter(8));
        Map<String, Integer> workerLimitsConfiguration = new HashMap<>();
        workerLimitsConfiguration.put(RESOURCE1, 7);
        workerLimitsConfiguration.put(RESOURCE2, 10);

        Map<String, Integer> globalLimitsConfiguration = new HashMap<>();
        globalLimitsConfiguration.put(RESOURCE1, 10);
        globalLimitsConfiguration.put(RESOURCE2, 10);
        List<AssignedTask> taskids = instance.takeTasks(1000, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(),
            availableSpace, workerLimitsConfiguration, workerCounters, globalLimitsConfiguration, globalCounters, null);
        assertEquals(0, taskids.size());
    }

    @Test
    public void test_limit_on_user() throws Exception {
        TasksHeap instance = new TasksHeap(10000, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 10);
        availableSpace.put(TASKTYPE_MYTASK2, 20);
        AtomicLong newTaskId = new AtomicLong(987);
        for (int i = 0; i < 1000; i++) {
            instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
            if (i % 2 == 0) {
                instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK2, USERID1);
            }
        }

        Map<TaskTypeUser, IntCounter> availableSpacePerUser = new HashMap<>();
        availableSpacePerUser.put(new TaskTypeUser(TASKTYPE_MYTASK1, USERID1), new IntCounter(5));
        availableSpacePerUser.put(new TaskTypeUser(TASKTYPE_MYTASK2, USERID1), new IntCounter(5));
        List<AssignedTask> taskids = instance.takeTasks(1000,
            Arrays.asList(Task.GROUP_ANY),
            Collections.emptySet(), availableSpace, Collections.emptyMap(),
            new ResourceUsageCounters(),
            Collections.emptyMap(),
            new ResourceUsageCounters(), availableSpacePerUser);
        assertEquals(10, taskids.size());
    }

}

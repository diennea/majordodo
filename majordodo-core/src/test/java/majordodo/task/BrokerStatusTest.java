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
import java.util.List;
import java.util.Map;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.SubmitTaskResult;
import org.junit.Test;
import static org.junit.Assert.*;

public class BrokerStatusTest {

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            return new TaskProperties(group, null);
        };
    }

    @Test
    public void testCollectMaxAvailableSpacePerUserOnWorker() throws Exception {

        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();

            // 5 tasks to user
            for (int i = 0; i < 5; i++) {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, "", 1, 0, 0, null, 0, null, null));
                assertTrue(res.getTaskId() > 0);
            }

            int maxThreadPerTaskType1 = 10;
            Map<String, Integer> maxThreadsPerTaskType = Collections.singletonMap(TASKTYPE_MYTYPE, maxThreadPerTaskType1);
            int maxThreadPerUserPerTaskTypePercent = 0; // unlimited
            {

                List<AssignedTask> assignTasksToWorker = broker.assignTasksToWorker(100, maxThreadsPerTaskType, Collections.singletonList(group),
                    Collections.emptySet(), "myworker", new HashMap<>(), new ResourceUsageCounters(), maxThreadPerUserPerTaskTypePercent);
                assertEquals(5, assignTasksToWorker.size());
            }

            // add other 10 tasks for the same user
            for (int i = 0; i < 10; i++) {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, "", 1, 0, 0, null, 0, null, null));
                assertTrue(res.getTaskId() > 0);
            }

            maxThreadPerUserPerTaskTypePercent = 80;
            // max 80% of total capacity, so maxThreadForTaskType1 for user 'user1' will be maxThreadPerTaskType1 * 20 / 100 = 8
            {
                List<AssignedTask> assignTasksToWorker = broker.assignTasksToWorker(100, maxThreadsPerTaskType, Collections.singletonList(group),
                    Collections.emptySet(), "myworker", new HashMap<>(), new ResourceUsageCounters(), maxThreadPerUserPerTaskTypePercent);
                assertEquals(3, assignTasksToWorker.size());
            }

        }
    }
    
    @Test
    public void testCollectMaxAvailableSpacePerUserOnWorkerMaxThreads1() throws Exception {
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();

            int maxThreadPerTaskType1 = 1;
            Map<String, Integer> maxThreadsPerTaskType2 = Collections.singletonMap(TASKTYPE_MYTYPE, maxThreadPerTaskType1);
            
            // add other 10 tasks for the same user
            for (int i = 0; i < 10; i++) {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, "", 1, 0, 0, null, 0, null, null));
                assertTrue(res.getTaskId() > 0);
            }

            int maxThreadPerUserPerTaskTypePercent = 80;
            // at least one should be assigned
            {
                List<AssignedTask> assignTasksToWorker = broker.assignTasksToWorker(100, maxThreadsPerTaskType2, Collections.singletonList(group),
                    Collections.emptySet(), "myworker", new HashMap<>(), new ResourceUsageCounters(), maxThreadPerUserPerTaskTypePercent);
                assertEquals(1, assignTasksToWorker.size());
            }

        }
    }

    @Test
    public void testCollectMaxAvailableSpacePerUserOnWorkerUsingTaskTypeAny() throws Exception {

        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();

            // 5 tasks to user
            for (int i = 0; i < 5; i++) {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, "", 1, 0, 0, null, 0, null, null));
                assertTrue(res.getTaskId() > 0);
            }

            int maxThreadPerTaskType1 = 10;
            Map<String, Integer> maxThreadsPerTaskType = Collections.singletonMap(Task.TASKTYPE_ANY, maxThreadPerTaskType1);
            int maxThreadPerUserPerTaskTypePercent = 0; // unlimited
            {

                List<AssignedTask> assignTasksToWorker = broker.assignTasksToWorker(100, maxThreadsPerTaskType, Collections.singletonList(group),
                    Collections.emptySet(), "myworker", new HashMap<>(), new ResourceUsageCounters(), maxThreadPerUserPerTaskTypePercent);
                assertEquals(5, assignTasksToWorker.size());
            }

            // add other 10 tasks for the same user
            for (int i = 0; i < 10; i++) {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, "", 1, 0, 0, null, 0, null, null));
                assertTrue(res.getTaskId() > 0);
            }

            maxThreadPerUserPerTaskTypePercent = 80;
            // max 80% of total capacity, so maxThreadForTaskType1 for user 'user1' will be maxThreadPerTaskType1 * 20 / 100 = 8
            {
                List<AssignedTask> assignTasksToWorker = broker.assignTasksToWorker(100, maxThreadsPerTaskType, Collections.singletonList(group),
                    Collections.emptySet(), "myworker", new HashMap<>(), new ResourceUsageCounters(), maxThreadPerUserPerTaskTypePercent);
                assertEquals(3, assignTasksToWorker.size());
            }

        }
    }

}

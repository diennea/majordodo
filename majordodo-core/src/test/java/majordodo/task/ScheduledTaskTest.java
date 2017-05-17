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

import majordodo.executors.TaskExecutor;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.SubmitTaskResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author francesco.caliumi
 */
@BrokerTestUtils.StartBroker
@BrokerTestUtils.LogLevel(level = "SEVERE")
public class ScheduledTaskTest extends BrokerTestUtils {

    @Before
    public void reduceCycleWaitTime() {
        broker.setCycleAwaitSeconds(1);
    }
    
    @Test
    public void scheduledTaskTest() throws Exception {
        
        String workerId = "abc";
        String taskParams = "param";
        String slot = "slot";
        
        long requestedStartTime = System.currentTimeMillis() + 3000;
        long taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, requestedStartTime, 0, slot, 0, null, null)).getTaskId();
        Map<String, Long> busySlots = broker.getSlotsStatusView().getBusySlots();
        assertEquals(1, busySlots.size());
        assertEquals(Long.valueOf(taskId), busySlots.get(slot));
        
        Task tmpTask = broker.getBrokerStatus().getTask(taskId);
        assertTrue(tmpTask != null);
        assertEquals(Task.STATUS_DELAYED, tmpTask.getStatus());
        
        // This should fail
        SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, requestedStartTime, 0, slot, 0, null, null));
        assertEquals("slot slot already assigned", res.getOutcome());
        
        try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
            CountDownLatch taskStartedLatch = new CountDownLatch(1);
            CountDownLatch taskFinishedLatch = new CountDownLatch(1);
            Map<String, Integer> tags = new HashMap<>();
            tags.put(TASKTYPE_MYTYPE, 1);

            WorkerCoreConfiguration config = new WorkerCoreConfiguration();
            config.setMaxPendingFinishedTaskNotifications(1);
            config.setWorkerId(workerId);
            config.setMaxThreadsByTaskType(tags);
            config.setGroups(Arrays.asList(group));
            config.setTasksRequestTimeout(1000);
            try (WorkerCore core = new WorkerCore(config, "process1", locator, null);) {
                core.start();
                core.setExecutorFactory(
                        (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {
                    @Override
                    public String executeTask(Map<String, Object> parameters) throws Exception {
                        taskStartedLatch.countDown();
                        System.out.println("executeTask: " + parameters);
                        if (System.currentTimeMillis() < requestedStartTime) {
                            System.out.println("task executed at " + System.currentTimeMillis() + " before requested " + requestedStartTime);
                            core.die();
                            return null;
                        }
                        taskFinishedLatch.countDown();
                        return "theresult";
                    }

                }
                );

                assertTrue(taskStartedLatch.await(30, TimeUnit.SECONDS));
                assertTrue(taskFinishedLatch.await(30, TimeUnit.SECONDS));
                
                checkTaskStatus(broker, taskId, Task.STATUS_FINISHED, "theresult", 30*1000, true);
            }
        }
    }
    
    @Test
    public void submitScheduledTaskAndRestartBrokerTest() throws Exception {
        
        String workerId = "abc";
        String taskParams = "param";
        String slot = "slot";
        Task tmpTask;
        
        long requestedStartTime = System.currentTimeMillis() + 5000;
        long taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, requestedStartTime, 0, slot, 0, null, null)).getTaskId();
        Map<String, Long> busySlots = broker.getSlotsStatusView().getBusySlots();
        assertEquals(1, busySlots.size());
        assertEquals(Long.valueOf(taskId), busySlots.get(slot));
        
        tmpTask = broker.getBrokerStatus().getTask(taskId);
        assertTrue(tmpTask != null);
        assertEquals(Task.STATUS_DELAYED, tmpTask.getStatus());
        
        // This should fail
        SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, requestedStartTime, 0, slot, 0, null, null));
        assertEquals("slot slot already assigned", res.getOutcome());
        
        System.out.println("RESTART!");
        restartSingleBroker(false);
        
        tmpTask = broker.getBrokerStatus().getTask(taskId);
        assertTrue(tmpTask != null);
        assertEquals(Task.STATUS_DELAYED, tmpTask.getStatus());
        
        try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
            CountDownLatch taskStartedLatch = new CountDownLatch(1);
            CountDownLatch taskFinishedLatch = new CountDownLatch(1);
            Map<String, Integer> tags = new HashMap<>();
            tags.put(TASKTYPE_MYTYPE, 1);

            WorkerCoreConfiguration config = new WorkerCoreConfiguration();
            config.setMaxPendingFinishedTaskNotifications(1);
            config.setWorkerId(workerId);
            config.setMaxThreadsByTaskType(tags);
            config.setGroups(Arrays.asList(group));
            config.setTasksRequestTimeout(1000);
            try (WorkerCore core = new WorkerCore(config, "process1", locator, null);) {
                core.start();
                core.setExecutorFactory(
                        (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {
                    @Override
                    public String executeTask(Map<String, Object> parameters) throws Exception {
                        taskStartedLatch.countDown();
                        System.out.println("executeTask: " + parameters);
                        if (System.currentTimeMillis() < requestedStartTime) {
                            System.out.println("task executed at " + System.currentTimeMillis() + " before requested " + requestedStartTime);
                            core.die();
                            return null;
                        }
                        taskFinishedLatch.countDown();
                        return "theresult";
                    }

                }
                );

                assertTrue(taskStartedLatch.await(30, TimeUnit.SECONDS));
                assertTrue(taskFinishedLatch.await(30, TimeUnit.SECONDS));
                
                checkTaskStatus(broker, taskId, Task.STATUS_FINISHED, "theresult", 30*1000, true);
            }
        }
    }
    
    @Test
    public void scheduledTaskInTransactionTest() throws Exception {
        
        String workerId = "abc";
        String taskParams1 = "param1";
        String taskParams2 = "param2";
        String slot = "slot";
        
        long requestedStartTime = System.currentTimeMillis() + 3000;
        
        // This transaction should fail
        long transaction_1 = broker.getClient().beginTransaction();
        long tmpTaskId1 = broker.getClient().submitTask(new AddTaskRequest(transaction_1, TASKTYPE_MYTYPE, userId, taskParams1, 0, requestedStartTime, 0, slot, 0, null, null)).getTaskId();
        assertTrue(tmpTaskId1 > 0);
        long tmpTaskId2 = broker.getClient().submitTask(new AddTaskRequest(transaction_1, TASKTYPE_MYTYPE, userId, taskParams2, 0, 0, 0, slot, 0, null, null)).getTaskId();
        assertEquals(0, tmpTaskId2);
        broker.getClient().rollbackTransaction(transaction_1);

        long transaction_2 = broker.getClient().beginTransaction();
        long taskId1 = broker.getClient().submitTask(new AddTaskRequest(transaction_2, TASKTYPE_MYTYPE, userId, taskParams1, 0, requestedStartTime, 0, slot, 0, null, null)).getTaskId();
        assertTrue(taskId1 > 0);
        long taskId2 = broker.getClient().submitTask(new AddTaskRequest(transaction_2, TASKTYPE_MYTYPE, userId, taskParams2, 0, 0, 0, null, 0, null, null)).getTaskId();
        assertTrue(taskId2 > 0);
        broker.getClient().commitTransaction(transaction_2);
        
        Map<String, Long> busySlots = broker.getSlotsStatusView().getBusySlots();
        assertEquals(1, busySlots.size());
        assertEquals(Long.valueOf(taskId1), busySlots.get(slot));
        
        Task tmpTask1 = broker.getBrokerStatus().getTask(taskId1);
        assertTrue(tmpTask1 != null);
        assertEquals(Task.STATUS_DELAYED, tmpTask1.getStatus());
        
        Task tmpTask2 = broker.getBrokerStatus().getTask(taskId2);
        assertTrue(tmpTask2 != null);
        assertEquals(Task.STATUS_WAITING, tmpTask2.getStatus());
        
        
        // This should fail
        SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams1, 0, requestedStartTime, 0, slot, 0, null, null));
        assertEquals("slot slot already assigned", res.getOutcome());
        
        try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
            CountDownLatch taskStartedLatch = new CountDownLatch(2);
            CountDownLatch taskFinishedLatch = new CountDownLatch(2);
            Map<String, Integer> tags = new HashMap<>();
            tags.put(TASKTYPE_MYTYPE, 1);

            WorkerCoreConfiguration config = new WorkerCoreConfiguration();
            config.setMaxPendingFinishedTaskNotifications(1);
            config.setWorkerId(workerId);
            config.setMaxThreadsByTaskType(tags);
            config.setGroups(Arrays.asList(group));
            config.setTasksRequestTimeout(1000);
            try (WorkerCore core = new WorkerCore(config, "process1", locator, null);) {
                core.start();
                core.setExecutorFactory(
                        (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {
                    @Override
                    public String executeTask(Map<String, Object> parameters) throws Exception {
                        taskStartedLatch.countDown();
                        System.out.println("executeTask: parameters=" + parameters + ", execTime=" + new java.sql.Timestamp(System.currentTimeMillis()));
                        String param = (String) parameters.get("parameter");
                        
                        if (param.equals(taskParams1) && System.currentTimeMillis() < requestedStartTime) {
                            System.out.println("task executed at " + System.currentTimeMillis() + " before requested " + requestedStartTime);
                            core.die();
                            return null;
                        }
                        taskFinishedLatch.countDown();
                        return "theresult";
                    }

                }
                );

                assertTrue(taskStartedLatch.await(30, TimeUnit.SECONDS));
                assertTrue(taskFinishedLatch.await(30, TimeUnit.SECONDS));
                
                checkTaskStatus(broker, taskId1, Task.STATUS_FINISHED, "theresult", 30*1000, true);
                checkTaskStatus(broker, taskId2, Task.STATUS_FINISHED, "theresult", 30*1000, true);
            }
        }
    }
}

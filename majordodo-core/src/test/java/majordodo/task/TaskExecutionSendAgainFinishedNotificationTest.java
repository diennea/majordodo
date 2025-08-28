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

import majordodo.clientfacade.TaskStatusView;
import majordodo.executors.TaskExecutor;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.worker.FinishedTaskNotification;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class TaskExecutionSendAgainFinishedNotificationTest {

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            return new TaskProperties(group1, RESOURCES);
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String[] RESOURCES = new String[]{"db1","db2"};
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Test
    public void continueTrySendNotificationOnConnectionFailureTest() throws Exception {

        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        AtomicBoolean sendNotificationsErrorDone = new AtomicBoolean();

        // startAsWritable a broker and do some work
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(),
            new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {

                    CountDownLatch connectedLatch = new CountDownLatch(1);
                    CountDownLatch disconnectedLatch = new CountDownLatch(1);
                    CountDownLatch allTaskExecuted = new CountDownLatch(1);
                    WorkerStatusListener listener = new WorkerStatusListener() {

                        @Override
                        public void connectionEvent(String event, WorkerCore core) {
                            if (event.equals(WorkerStatusListener.EVENT_CONNECTED)) {
                                connectedLatch.countDown();
                            }
                            if (event.equals(WorkerStatusListener.EVENT_DISCONNECTED)) {
                                disconnectedLatch.countDown();
                            }
                        }

                        @Override
                        public void beforeNotifyTasksFinished(List<FinishedTaskNotification> notifications, WorkerCore core) {
                            if (sendNotificationsErrorDone.compareAndSet(false, true)) {
                                core.disconnect();
                            }
                        }

                    };
                    Map<String, Integer> tags = new HashMap<>();
                    tags.put(TASKTYPE_MYTYPE, 1);

                    WorkerCoreConfiguration config = new WorkerCoreConfiguration();
                    config.setMaxPendingFinishedTaskNotifications(1);
                    config.setWorkerId(workerId);
                    config.setMaxThreadsByTaskType(tags);
                    config.setGroups(Arrays.asList(group));
                    try (WorkerCore core = new WorkerCore(config, workerId, locator, listener);) {
                        core.start();
                        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
                        core.setExecutorFactory(
                            (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

                            @Override
                            public String executeTask(Map<String, Object> parameters) throws Exception {
                                allTaskExecuted.countDown();
                                return "theresult";
                            }

                        }
                        );

                        taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

                        boolean okFinishedForBroker = false;
                        for (int i = 0; i < 100; i++) {
                            TaskStatusView task = broker.getClient().getTask(taskId);
                            if (task.getStatus() == Task.STATUS_FINISHED) {
                                okFinishedForBroker = true;
                                break;
                            }
                            Thread.sleep(1000);
                        }
                        assertTrue(okFinishedForBroker);
                    }
                    assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
                    assertTrue(sendNotificationsErrorDone.get());
                    
                    Map<String,Integer> counters = broker.getWorkers().getWorkerManager(workerId)
                            .getResourceUsageCounters().getCountersView();
                    
                    for (String resource: Arrays.asList(RESOURCES)) {
                        System.out.println("Counter resource="+resource+"; counter="+counters.get(resource));
                        assertEquals(0, counters.get(resource).intValue());
                    }
                }
            }
            
            Map<String,Integer> counters = broker.getGlobalResourceUsageCounters().getCountersView();
            for (String resource: Arrays.asList(RESOURCES)) {
                System.out.println("Counter resource="+resource+"; counter="+counters.get(resource));
                assertEquals(0, counters.get(resource).intValue());
            }
        }

    }

    @Test
    public void resSendNotificationOnErrorAckFromBrokerTest() throws Exception {

        long taskId;
        String workerId = "abc";
        String taskParams = "param";

        AtomicBoolean sendNotificationsErrorDone = new AtomicBoolean();

        MemoryCommitLog log = new MemoryCommitLog() {
            @Override
            public LogSequenceNumber logStatusEdit(StatusEdit action) throws LogNotAvailableException {
                if (action.editType == StatusEdit.TYPE_TASK_STATUS_CHANGE
                    && action.taskStatus == Task.STATUS_FINISHED) {
                    if (sendNotificationsErrorDone.compareAndSet(false, true)) {
                        throw new LogNotAvailableException("temporary error while writing status of task " + action.taskId);
                    }
                }
                return super.logStatusEdit(action);
            }
        };

        // startAsWritable a broker and do some work
        try (Broker broker = new Broker(new BrokerConfiguration(), log, new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {

                    CountDownLatch connectedLatch = new CountDownLatch(1);
                    CountDownLatch disconnectedLatch = new CountDownLatch(1);
                    CountDownLatch allTaskExecuted = new CountDownLatch(1);
                    WorkerStatusListener listener = new WorkerStatusListener() {

                        @Override
                        public void connectionEvent(String event, WorkerCore core) {
                            if (event.equals(WorkerStatusListener.EVENT_CONNECTED)) {
                                connectedLatch.countDown();
                            }
                            if (event.equals(WorkerStatusListener.EVENT_DISCONNECTED)) {
                                disconnectedLatch.countDown();
                            }
                        }

                    };
                    Map<String, Integer> tags = new HashMap<>();
                    tags.put(TASKTYPE_MYTYPE, 1);

                    WorkerCoreConfiguration config = new WorkerCoreConfiguration();
                    config.setMaxPendingFinishedTaskNotifications(1);
                    config.setWorkerId(workerId);
                    config.setMaxThreadsByTaskType(tags);
                    config.setGroups(Arrays.asList(group));
                    try (WorkerCore core = new WorkerCore(config, workerId, locator, listener);) {
                        core.start();
                        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
                        core.setExecutorFactory(
                            (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

                            @Override
                            public String executeTask(Map<String, Object> parameters) throws Exception {
                                allTaskExecuted.countDown();
                                return "theresult";
                            }

                        }
                        );

                        taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

                        boolean okFinishedForBroker = false;
                        for (int i = 0; i < 100; i++) {
                            TaskStatusView task = broker.getClient().getTask(taskId);
                            if (task.getStatus() == Task.STATUS_FINISHED) {
                                okFinishedForBroker = true;
                                break;
                            }
                            Thread.sleep(1000);
                        }
                        assertTrue(okFinishedForBroker);
                        
                        Map<String,Integer> counters = broker.getWorkers().getWorkerManager(workerId)
                            .getResourceUsageCounters().getCountersView();
                        for (String resource: Arrays.asList(RESOURCES)) {
                            System.out.println("Counter resource="+resource+"; counter="+counters.get(resource));
                            assertEquals(0, counters.get(resource).intValue());
                        }
                    }
                    assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
                    assertTrue(sendNotificationsErrorDone.get());

                }
            }
            
            Map<String,Integer> counters = broker.getGlobalResourceUsageCounters().getCountersView();
            for (String resource: Arrays.asList(RESOURCES)) {
                System.out.println("Counter resource="+resource+"; counter="+counters.get(resource));
                assertEquals(0, counters.get(resource).intValue());
            }
        }

    }

}

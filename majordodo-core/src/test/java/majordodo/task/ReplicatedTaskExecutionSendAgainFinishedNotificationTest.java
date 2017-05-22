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
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.network.BrokerHostData;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.replication.ZKBrokerLocator;
import majordodo.replication.ZKTestEnv;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import majordodo.task.Task;
import majordodo.task.TaskProperties;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.task.TasksHeap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class ReplicatedTaskExecutionSendAgainFinishedNotificationTest {

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.INFO;
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
            }
        });
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        ch.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                if (record.getLevel().intValue() >= level.intValue()) {
                    return "" + new java.sql.Timestamp(record.getMillis()) + " " + record.getLevel() + " " + record.getLoggerName() + ": " + formatMessage(record) + "\n";
                } else {
                    return null;
                }
            }
        });
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            return new TaskProperties(group1, RESOURCES);
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String[] RESOURCES = new String[]{"db1", "db2"};
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void resendNotificationToPromotedLeaderTest() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();
            long taskId;
            String workerId = "abc";
            String taskParams = "param";

            String host = "localhost";
            int port = 7000;
            String host2 = "localhost";
            int port2 = 7001;

            BrokerConfiguration brokerConfig = new BrokerConfiguration();
            // startAsWritable a broker and do some work
            try (Broker broker1 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(),
                folderSnapshots.newFolder().toPath(),
                BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false),
                new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
                broker1.startAsWritable();
                try (NettyChannelAcceptor server1 = new NettyChannelAcceptor(broker1.getAcceptor(), host, port)) {
                    server1.start();

                    try (Broker broker2 = new Broker(brokerConfig,
                        new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(),
                            folderSnapshots.newFolder().toPath(),
                            BrokerHostData.formatHostdata(new BrokerHostData(host2, port2, "", false, null)), false),
                        new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
                        broker2.start();

                        try (NettyChannelAcceptor server2 = new NettyChannelAcceptor(broker2.getAcceptor(), host2, port2)) {
                            server2.start();

                            try (ZKBrokerLocator locator = new ZKBrokerLocator(zkServer.getAddress(),
                                zkServer.getTimeout(), zkServer.getPath())) {

                                CountDownLatch connectedLatch = new CountDownLatch(1);
                                CountDownLatch disconnectedLatch = new CountDownLatch(1);
                                CountDownLatch allTaskExecuted = new CountDownLatch(1);
                                CountDownLatch startExecutingTask = new CountDownLatch(1);
                                CountDownLatch secondBrokerIsLeader = new CountDownLatch(1);
                                WorkerStatusListener listener = new WorkerStatusListener() {

                                    @Override
                                    public void connectionEvent(String event, WorkerCore core) {
                                        System.out.println("connectionEvent " + event);
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
                                            startExecutingTask.countDown();
                                            assertTrue(secondBrokerIsLeader.await(1, TimeUnit.MINUTES));
                                            allTaskExecuted.countDown();
                                            return "theresult";
                                        }

                                    }
                                    );

                                    taskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();

                                    assertTrue(startExecutingTask.await(30, TimeUnit.SECONDS));
                                    broker1.close();
                                    server1.close();
                                    // worker must disconnect from broker1
                                    assertTrue(disconnectedLatch.await(1, TimeUnit.MINUTES));

                                    while (!broker2.isWritable()) {
                                        System.out.println("waiting for broker2 to become leader");
                                        Thread.sleep(1000);
                                    }

                                    broker2.getWorkers().getWorkerManager(workerId).getResourceUsageCounters().updateResourceCounters();
                                    Map<String, Integer> countersWorker = broker2.getWorkers().getWorkerManager(workerId)
                                        .getResourceUsageCounters().getCountersView();
                                    for (String resource : Arrays.asList(RESOURCES)) {
                                        System.out.println("Counter resource=" + resource + "; counter=" + countersWorker.get(resource));
                                        assertNotNull(countersWorker.get(resource));
                                        assertEquals(1, countersWorker.get(resource).intValue());
                                    }

                                    broker2.getGlobalResourceUsageCounters().updateResourceCounters();
                                    Map<String, Integer> countersGlobal = broker2.getGlobalResourceUsageCounters().getCountersView();
                                    for (String resource : Arrays.asList(RESOURCES)) {
                                        System.out.println("Counter resource=" + resource + "; counter=" + countersGlobal.get(resource));
                                        assertNotNull(countersGlobal.get(resource));
                                        assertEquals(1, countersGlobal.get(resource).intValue());
                                    }

                                    secondBrokerIsLeader.countDown();
                                    System.out.println("broker2 is now writable");

                                    assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

                                    boolean okFinishedForBroker = false;
                                    for (int i = 0; i < 100; i++) {
                                        TaskStatusView task = broker2.getClient().getTask(taskId);
                                        if (task.getStatus() == Task.STATUS_FINISHED) {
                                            okFinishedForBroker = true;
                                            break;
                                        }
                                        Thread.sleep(1000);
                                    }
                                    assertTrue(okFinishedForBroker);

                                    countersWorker = broker2.getWorkers().getWorkerManager(workerId)
                                        .getResourceUsageCounters().getCountersView();
                                    for (String resource : Arrays.asList(RESOURCES)) {
                                        System.out.println("Counter resource=" + resource + "; counter=" + countersWorker.get(resource));
                                        assertEquals(0, countersWorker.get(resource).intValue());
                                    }

                                    countersGlobal = broker2.getGlobalResourceUsageCounters().getCountersView();
                                    for (String resource : Arrays.asList(RESOURCES)) {
                                        System.out.println("Counter resource=" + resource + "; counter=" + countersGlobal.get(resource));
                                        assertEquals(0, countersGlobal.get(resource).intValue());
                                    }
                                }

                            }

                        }
                    }
                }

            }
        }
    }
}

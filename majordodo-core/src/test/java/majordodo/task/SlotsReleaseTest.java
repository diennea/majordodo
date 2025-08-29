
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

import majordodo.clientfacade.SubmitTaskResult;
import majordodo.clientfacade.TaskStatusView;
import majordodo.executors.TaskExecutor;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import majordodo.clientfacade.AddTaskRequest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class SlotsReleaseTest {

    protected Path workDir;

    @After
    public void deleteWorkdir() throws Exception {
        if (workDir != null) {
            Files.walkFileTree(workDir, new FileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

            });
        }

    }

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            return new TaskProperties(group1, null);
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Test
    public void slotReleaseOnErrorTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        final String SLOTID = "myslot";

        // startAsWritable a broker and request a task, with slot
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID, 0, null, null));
            taskId = res.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(res.getOutcome() == null);
            // slot is busy
            assertEquals(0, broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID, 0, null, null)).getTaskId());

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
                                throw new Exception("error !");
                            }

                        }
                        );

                        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

                        boolean okFinishedForBroker = false;
                        for (int i = 0; i < 100; i++) {
                            TaskStatusView task = broker.getClient().getTask(taskId);
                            if (task.getStatus() == Task.STATUS_ERROR) {
                                okFinishedForBroker = true;
                                break;
                            }
                            Thread.sleep(1000);
                        }
                        assertTrue(okFinishedForBroker);
                    }
                    assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

                    // now the slot is free
                    taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, SLOTID, 0, null, null)).getTaskId();
                    assertTrue(taskId > 0);
                }

            }
        }

    }

    @Test
    public void slotReleaseOnWorkerDeathTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        final String SLOTID = "myslot";

        // startAsWritable a broker and request a task, with slot
        BrokerConfiguration bc = new BrokerConfiguration();
        bc.setMaxWorkerIdleTime(1000);
        try (Broker broker = new Broker(bc, new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID, 0, null, null));
            taskId = res.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(res.getOutcome() == null);
            // slot is busy
            assertEquals(0, broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID, 0, null, null)).getTaskId());

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
                                core.die();
                                throw new Exception("error");
                            }
                        }
                        );
                        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));
                    }
                    assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
                }
                boolean okFinishedForBroker = false;
                for (int i = 0; i < 100; i++) {
                    TaskStatusView task = broker.getClient().getTask(taskId);
                    if (task.getStatus() == Task.STATUS_ERROR) {
                        System.out.println("result: " + task.getResult());
                        okFinishedForBroker = true;
                        assertEquals("worker abc died", task.getResult());
                        break;
                    }
                    Thread.sleep(1000);
                }
                assertTrue(okFinishedForBroker);
                // now the slot is free
                taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, SLOTID, 0, null, null)).getTaskId();
                assertTrue(taskId > 0);

            }
        }

    }

    @Test
    public void slotReleaseOnDeadlineTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        final String SLOTID = "myslot";

        // startAsWritable a broker and request a task, with slot
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, System.currentTimeMillis(), SLOTID, 0, null, null));
            taskId = res.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(res.getOutcome() == null);
            // slot is busy
            assertEquals(0, broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, SLOTID, 0, null, null)).getTaskId());
            Thread.sleep(1000);
            broker.purgeTasks();
            // slot is free
            long taskId2 = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, SLOTID, 0, null, null)).getTaskId();
            assertTrue(taskId2 > 0);
        }

    }

    @Test
    public void slotReleaseOnAbandonedTransaction() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        final String SLOTID = "myslot";

        // startAsWritable a broker and request a task, with slot
        BrokerConfiguration brokerConfiguration = new BrokerConfiguration();
        brokerConfiguration.setTransactionsTtl(1000);
        try (Broker broker = new Broker(brokerConfiguration, new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            long tx = broker.getClient().beginTransaction();
            assertNotNull(broker.getClient().getTransaction(tx));
            SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(tx, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, System.currentTimeMillis(), SLOTID, 0, null, null));
            taskId = res.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(res.getOutcome() == null);
            // slot is busy
            assertEquals(0, broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, SLOTID, 0, null, null)).getTaskId());
            Thread.sleep(5000);
            // on checkpoint the transaction will be rolled back
            assertNotNull(broker.getClient().getTransaction(tx));
            broker.checkpoint();
            // slot is free
            long taskId2 = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, SLOTID, 0, null, null)).getTaskId();
            assertTrue(taskId2 > 0);
            assertNull(broker.getClient().getTransaction(tx));

        }

    }

    @Test
    public void slotReleaseOnCommitLogErrorTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;

        String taskParams = "param";
        final String SLOTID = "myslot";
        final String SLOTID2 = "myslot2";
        final String SLOTID3 = "myslot3";
        final String SLOTID4 = "myslot4";

        AtomicBoolean errorOnWriteOnLog = new AtomicBoolean(true);
        // startAsWritable a broker and request a task, with slot
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog() {
            @Override
            public LogSequenceNumber logStatusEdit(StatusEdit action) throws LogNotAvailableException {
                if (errorOnWriteOnLog.get()) {
                    throw new LogNotAvailableException("error !");
                }
                return super.logStatusEdit(action);
            }

        }, new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();

            errorOnWriteOnLog.set(true);
            {
                try {
                    broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID, 0, null, null));
                    fail();
                } catch (LogNotAvailableException ok) {
                }
                assertTrue(broker.getSlotsStatusView().getBusySlots().isEmpty());
            }

            errorOnWriteOnLog.set(false);

            {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID, 0, null, null));
                taskId = res.getTaskId();
                assertTrue(taskId > 0);
                assertTrue(res.getOutcome() == null);
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID));
            }

            {
                // now with transaction
                long txId = broker.getClient().beginTransaction();
                errorOnWriteOnLog.set(true);
                try {
                    SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(txId, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID2, 0, null, null));
                    fail();
                } catch (LogNotAvailableException ok) {
                }
                assertNull(broker.getSlotsStatusView().getBusySlots().get(SLOTID2));
                errorOnWriteOnLog.set(false);
                broker.getClient().rollbackTransaction(txId);
            }

            {
                errorOnWriteOnLog.set(false);
                // now with error on commit transaction
                long txId = broker.getClient().beginTransaction();
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(txId, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID3, 0, null, null));
                taskId = res.getTaskId();
                assertTrue(taskId > 0);
                assertTrue(res.getOutcome() == null);
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID3));
                errorOnWriteOnLog.set(true);
                try {
                    broker.getClient().commitTransaction(txId);
                    fail();
                } catch (LogNotAvailableException ok) {
                }
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID3));

                try {
                    // rollback fails, slot is still busy
                    broker.getClient().rollbackTransaction(txId);
                    fail();
                } catch (LogNotAvailableException ok) {
                }
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID3));

                errorOnWriteOnLog.set(false);

                // client tries to rollback, so the slow is free
                broker.getClient().rollbackTransaction(txId);
                assertNull(broker.getSlotsStatusView().getBusySlots().get(SLOTID3));

            }

            {
                errorOnWriteOnLog.set(false);
                // now with error on commit transaction
                long txId = broker.getClient().beginTransaction();
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(txId, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, SLOTID4, 0, null, null));
                taskId = res.getTaskId();
                assertTrue(taskId > 0);
                assertTrue(res.getOutcome() == null);
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID4));
                errorOnWriteOnLog.set(true);
                try {
                    broker.getClient().commitTransaction(txId);
                    fail();
                } catch (LogNotAvailableException ok) {
                }
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID4));
                errorOnWriteOnLog.set(false);

                // now the commit is performed with success and the slot is busy
                broker.getClient().commitTransaction(txId);
                assertEquals((Long) taskId, broker.getSlotsStatusView().getBusySlots().get(SLOTID4));

            }

        }

    }

}

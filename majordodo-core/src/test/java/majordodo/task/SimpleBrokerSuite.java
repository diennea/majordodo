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
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.SubmitTaskResult;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public abstract class SimpleBrokerSuite extends BasicBrokerEnv {

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Test
    public void workerConnectionTest() throws Exception {

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
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

            core.setExecutorFactory((String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    return "";
                }

            });

            String taskParams = "param";
            long taskId = getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null)).getTaskId();

            assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void manyTasks_max1() throws Exception {

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 10; i++) {
            String taskParams = "p1=value1,p2=value2";
            long taskId = getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null)).getTaskId();
            todo.add(taskId);
        }

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch allTaskExecuted = new CountDownLatch(10);
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
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
            assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void manyTasks_max10() throws Exception {
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        java.util.logging.Logger.getLogger("").setLevel(Level.ALL);
        java.util.logging.Logger.getLogger("").addHandler(ch);

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 10; i++) {
            String taskParams = "p1=value1,p2=value2";
            long taskId = getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null)).getTaskId();
            todo.add(taskId);
        }

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch allTaskExecuted = new CountDownLatch(todo.size());
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
        tags.put(TASKTYPE_MYTYPE, 10);
        WorkerCoreConfiguration config = new WorkerCoreConfiguration();
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

            assertTrue(allTaskExecuted.await(60, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void manyTasks_max10_batch() throws Exception {
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        java.util.logging.Logger.getLogger("").setLevel(Level.ALL);
        java.util.logging.Logger.getLogger("").addHandler(ch);

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        List<AddTaskRequest> requests = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String taskParams = "p1=value1,p2=value2";
            requests.add(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null));
        }
        List<SubmitTaskResult> results = getClient().submitTasks(requests);
        for (SubmitTaskResult result : results) {
            long taskId = result.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(result.getOutcome() == null);
            todo.add(taskId);
        }

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch allTaskExecuted = new CountDownLatch(todo.size());
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
        tags.put(TASKTYPE_MYTYPE, 10);
        WorkerCoreConfiguration config = new WorkerCoreConfiguration();
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

            assertTrue(allTaskExecuted.await(60, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void manyTasks_max10_batch_transaction() throws Exception {
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        java.util.logging.Logger.getLogger("").setLevel(Level.ALL);
        java.util.logging.Logger.getLogger("").addHandler(ch);

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        List<AddTaskRequest> requests = new ArrayList<>();
        long transaction = getClient().beginTransaction();
        for (int i = 0; i < 10; i++) {
            String taskParams = "p1=value1,p2=value2";
            requests.add(new AddTaskRequest(transaction, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null));
        }
        List<SubmitTaskResult> results = getClient().submitTasks(requests);
        for (SubmitTaskResult result : results) {
            long taskId = result.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(result.getOutcome() == null);
            todo.add(taskId);
        }
        getClient().commitTransaction(transaction);

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch allTaskExecuted = new CountDownLatch(todo.size());
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
        tags.put(TASKTYPE_MYTYPE, 10);
        WorkerCoreConfiguration config = new WorkerCoreConfiguration();
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

            assertTrue(allTaskExecuted.await(60, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void manyassignTaskAfterStart() throws Exception {

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 10; i++) {
            String taskParams = "p1=value1,p2=value2";
            long taskId = getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null)).getTaskId();
            todo.add(taskId);
        }

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch allTaskExecuted = new CountDownLatch(todo.size());
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
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
            assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void multipleWorkers() throws Exception {

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 20; i++) {
            String taskParams = "p1=value1,p2=value2";
            long taskId = getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null)).getTaskId();
            todo.add(taskId);
        }

        CountDownLatch allTaskExecuted = new CountDownLatch(todo.size());
        // launch 10 workers
        List<WorkerCore> cores = new ArrayList<>();
        final int numWorkers = 10;
        CountDownLatch disconnectedLatch = new CountDownLatch(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            String workerId = "tester" + i;
            String workerProcessId = "tester" + i + "_" + System.nanoTime();
            CountDownLatch connectedLatch = new CountDownLatch(1);

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
            config.setWorkerId("workerid_" + i);
            config.setMaxThreadsByTaskType(tags);
            config.setGroups(Arrays.asList(group));
            WorkerCore core = new WorkerCore(config, "here" + i, getBrokerLocator(), listener);
            cores.add(core);

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
        }

        assertTrue(allTaskExecuted.await(300, TimeUnit.SECONDS));

        for (WorkerCore core : cores) {
            core.stop();
        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void transactionTest() throws Exception {

        // submit 10 tasks in transaction
        Set<Long> todo = new ConcurrentSkipListSet<>();
        long transaction = getClient().beginTransaction();
        for (int i = 0; i < 10; i++) {
            String taskParams = "p1=value1,p2=value2";
            long taskId = getClient().submitTask(new AddTaskRequest(transaction, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null)).getTaskId();
            todo.add(taskId);
        }
        getClient().commitTransaction(transaction);

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch allTaskExecuted = new CountDownLatch(10);
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
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {

                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
                    return "";
                }

            });
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
            assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));
        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }
}

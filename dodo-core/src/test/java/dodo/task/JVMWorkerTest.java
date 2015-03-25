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
package dodo.task;

import dodo.clustering.MemoryCommitLog;
import dodo.executors.TaskExecutor;
import dodo.network.jvm.JVMBrokerLocator;
import dodo.worker.WorkerCore;
import dodo.worker.WorkerStatusListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple tests
 *
 * @author enrico.olivelli
 */
public class JVMWorkerTest {

    @Test
    public void workerConnectionTest() throws Exception {
        Broker broker = new Broker(new MemoryCommitLog());
        broker.start();
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
        tags.put("tag1", 1);
        WorkerCore core = new WorkerCore(10, "abc", "here", "localhost", tags, new JVMBrokerLocator(broker), listener);
        core.start();
        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

        core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

            @Override
            public void executeTask(Map<String, Object> parameters, Map<String, Object> results) throws Exception {
                results.put("res1", "myvalue");
                allTaskExecuted.countDown();
            }

        });

        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("param1", "value1");
        taskParams.put("param2", "value2");
        long taskId = broker.getClient().submitTask("mytasktype", "queue1", "tag1", taskParams);        

        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        core.stop();
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void manyTasks_max1() throws Exception {
        Broker broker = new Broker(new MemoryCommitLog());
        broker.start();

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> taskParams = new HashMap<>();
            taskParams.put("param1", "value1");
            taskParams.put("param2", "value2");
            long taskId = broker.getClient().submitTask("mytasktype", "queue1", "tag1", taskParams);            
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
        tags.put("tag1", 1);
        WorkerCore core = new WorkerCore(10, "abc", "here", "localhost", tags, new JVMBrokerLocator(broker), listener);

        core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

            @Override
            public void executeTask(Map<String, Object> parameters, Map<String, Object> results) throws Exception {
                results.put("res1", "myvalue");
                allTaskExecuted.countDown();
                long taskid = (Long) parameters.get("taskid");
                todo.remove(taskid);
                System.out.println("executeTask:"+parameters);
            }

        });
        core.start();
        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        core.stop();
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
        Broker broker = new Broker(new MemoryCommitLog());
        broker.start();

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> taskParams = new HashMap<>();
            taskParams.put("param1", "value1");
            taskParams.put("param2", "value2");
            long taskId = broker.getClient().submitTask("mytasktype", "queue1", "tag1", taskParams);            
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
        tags.put("tag1", 10);
        WorkerCore core = new WorkerCore(10, "abc", "here", "localhost", tags, new JVMBrokerLocator(broker), listener);

        core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

            @Override
            public void executeTask(Map<String, Object> parameters, Map<String, Object> results) throws Exception {
                results.put("res1", "myvalue");
                allTaskExecuted.countDown();
                long taskid = (Long) parameters.get("taskid");
                todo.remove(taskid);
            }

        });
        core.start();
        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        core.stop();
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.ALL;
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
        SimpleFormatter f = new SimpleFormatter() {

            @Override
            public synchronized String format(LogRecord record) {
                if (record.getThrown() != null) {
                    return super.format(record);
                } else {
                    return record.getThreadID() + " - " + record.getLoggerName() + " - " + java.text.MessageFormat.format(record.getMessage(), record.getParameters()) + "\r\n";
                }
            }

        };

        ch.setFormatter(f);
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }

    @Test
    public void manyassignTaskAfterStart() throws Exception {

        Broker broker = new Broker(new MemoryCommitLog());
        broker.start();

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> taskParams = new HashMap<>();
            taskParams.put("param1", "value1");
            taskParams.put("param2", "value2");
            long taskId = broker.getClient().submitTask("mytasktype", "queue1", "tag1", taskParams);            
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
        tags.put("tag1", 1);
        WorkerCore core = new WorkerCore(10, "abc", "here", "localhost", tags, new JVMBrokerLocator(broker), listener);

        core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

            @Override
            public void executeTask(Map<String, Object> parameters, Map<String, Object> results) throws Exception {
                results.put("res1", "myvalue");
                allTaskExecuted.countDown();
                long taskid = (Long) parameters.get("taskid");
                todo.remove(taskid);
            }

        });
        core.start();
        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        core.stop();
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

        assertTrue(todo.isEmpty());
    }

    @Test
    public void multipleWorkers() throws Exception {

        Broker broker = new Broker(new MemoryCommitLog());
        broker.start();

        // submit 10 tasks
        Set<Long> todo = new ConcurrentSkipListSet<>();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> taskParams = new HashMap<>();
            taskParams.put("param1", "value1");
            taskParams.put("param2", "value2");
            long taskId = broker.getClient().submitTask("mytasktype", "queue1", "tag1", taskParams);            
            todo.add(taskId);
        }

        CountDownLatch allTaskExecuted = new CountDownLatch(todo.size());
        // launch 10 workers
        List<WorkerCore> cores = new ArrayList<>();
        final int numWorkers = 2;
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
            tags.put("tag1", 1);
            WorkerCore core = new WorkerCore(10, workerProcessId, workerId, "localhost", tags, new JVMBrokerLocator(broker), listener);
            cores.add(core);

            core.setExecutorFactory((String typeType, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public void executeTask(Map<String, Object> parameters, Map<String, Object> results) throws Exception {
                    results.put("res1", "myvalue");
                    allTaskExecuted.countDown();
                    long taskid = (Long) parameters.get("taskid");
                    todo.remove(taskid);
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

}

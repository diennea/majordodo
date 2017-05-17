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
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;
import majordodo.clientfacade.AddTaskRequest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class TaskExecutionRecoveryOnWorkerRestartUsingResourcesTest {

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

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.SEVERE;
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
        SimpleFormatter f = new SimpleFormatter();
        ch.setFormatter(f);
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
    private static final String[] RESOURCES = new String[]{"resource1", "resource2"};
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Test
    public void taskRecoveryTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());
        
        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        AtomicBoolean resourcesPresentAtFirstRun = new AtomicBoolean();
        AtomicBoolean resourcesPresentAtSecondRun = new AtomicBoolean();

        // startAsWritable a broker and do some work
        BrokerConfiguration brokerConfig = new BrokerConfiguration();
        brokerConfig.setMaxWorkerIdleTime(5000);
        try (Broker broker = new Broker(brokerConfig, new FileCommitLog(workDir, workDir, 1024 * 1024), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();

            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();

                // startAsWritable a worker, it will die
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
                    CountDownLatch taskStartedLatch = new CountDownLatch(1);
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
                                System.out.println("executeTask: " + parameters);
                                String resources = Arrays.asList(RESOURCES).stream().collect(Collectors.joining(","));
                                resourcesPresentAtFirstRun.set(resources.equals(parameters.get("resources")));
                                taskStartedLatch.countDown();
                                Integer attempt = (Integer) parameters.get("attempt");
                                if (attempt == null || attempt == 1) {
                                    core.die();
                                    return null;
                                }
                                return "theresult";
                            }

                        }
                        );

                        assertTrue(taskStartedLatch.await(30, TimeUnit.SECONDS));
                    }
                }

                boolean ok = false;
                for (int i = 0; i < 100; i++) {
                    Task task = broker.getBrokerStatus().getTask(taskId);
//                    System.out.println("task:" + task);
                    if (task.getStatus() == Task.STATUS_WAITING) {
                        ok = true;
                        break;
                    }
                    Thread.sleep(1000);
                }
                assertTrue(ok);

                // boot the worker again
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
                    CountDownLatch taskStartedLatch = new CountDownLatch(1);
                    Map<String, Integer> tags = new HashMap<>();
                    tags.put(TASKTYPE_MYTYPE, 1);

                    WorkerCoreConfiguration config = new WorkerCoreConfiguration();
                    config.setMaxPendingFinishedTaskNotifications(1);
                    config.setWorkerId(workerId);
                    config.setMaxThreadsByTaskType(tags);
                    config.setGroups(Arrays.asList(group));
                    try (WorkerCore core = new WorkerCore(config, "process2", locator, null);) {
                        core.start();
                        core.setExecutorFactory(
                                (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {
                            @Override
                            public String executeTask(Map<String, Object> parameters) throws Exception {
                                resourcesPresentAtSecondRun.set("resource1,resource2".equals(parameters.get("resources")));
                                System.out.println("executeTask2: " + parameters + " ,taskStartedLatch:" + taskStartedLatch.getCount());
                                taskStartedLatch.countDown();

                                Integer attempt = (Integer) parameters.get("attempt");
                                if (attempt == null || attempt == 1) {
                                    throw new RuntimeException("impossible!");
                                }
                                return "theresult";
                            }

                        }
                        );
                        assertTrue(taskStartedLatch.await(10, TimeUnit.SECONDS));
                        ok = false;
                        for (int i = 0; i < 100; i++) {
                            Task task = broker.getBrokerStatus().getTask(taskId);
                            System.out.println("task2:" + task);
                            if (task.getStatus() == Task.STATUS_FINISHED) {
                                ok = true;
                                assertEquals("theresult", task.getResult());
                                break;
                            }
                            Thread.sleep(1000);
                        }
                        assertTrue(ok);
                        
                        Map<String,Integer> counters = broker.getWorkers().getWorkerManager(workerId)
                            .getResourceUsageCounters().getCountersView();
                    
                        for (String resource: Arrays.asList(RESOURCES)) {
                            System.out.println("Counter resource="+resource+"; counter="+counters.get(resource));
                            assertEquals(0, counters.get(resource).intValue());
                        }
                    }
                }

            }
            assertTrue(resourcesPresentAtFirstRun.get());
            assertTrue(resourcesPresentAtSecondRun.get());
            
            Map<String,Integer> counters = broker.getGlobalResourceUsageCounters().getCountersView();
            for (String resource: Arrays.asList(RESOURCES)) {
                System.out.println("Counter resource="+resource+"; counter="+counters.get(resource));
                assertEquals(0, counters.get(resource).intValue());
            }
        }

    }

}

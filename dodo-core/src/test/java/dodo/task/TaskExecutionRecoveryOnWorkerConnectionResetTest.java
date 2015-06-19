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

import dodo.client.TaskStatusView;
import dodo.clustering.FileCommitLog;
import dodo.clustering.GroupMapperFunction;
import dodo.clustering.Task;
import dodo.clustering.TasksHeap;
import dodo.executors.TaskExecutor;
import dodo.network.netty.NettyBrokerLocator;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.worker.WorkerCore;
import dodo.worker.WorkerCoreConfiguration;
import dodo.worker.WorkerStatusListener;
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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class TaskExecutionRecoveryOnWorkerConnectionResetTest {

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

    protected GroupMapperFunction createGroupMapperFunction() {
        return new GroupMapperFunction() {

            @Override
            public int getGroup(long taskid, int tasktype, String userid) {
                return groupsMap.getOrDefault(userid, 0);

            }
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final int TASKTYPE_MYTYPE = 987;
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
        System.out.println("SETUPWORKDIR:" + workDir);
        long taskId;
        String workerId = "abc";
        String taskParams = "param";

        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
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

        // start a broker and do some work
        BrokerConfiguration brokerConfig = new BrokerConfiguration();
        brokerConfig.setMaxWorkerIdleTime(5000);
        try (Broker broker = new Broker(brokerConfig, new FileCommitLog(workDir, workDir), new TasksHeap(1000, createGroupMapperFunction()));) {
            broker.start();
            taskId = broker.getClient().submitTask(TASKTYPE_MYTYPE, userId, taskParams,0,0);

            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();

                // start a worker, connection will be dropped during the execution of the task
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort())) {
                    CountDownLatch taskStartedLatch = new CountDownLatch(1);
                    Map<Integer, Integer> tags = new HashMap<>();
                    tags.put(TASKTYPE_MYTYPE, 1);

                    WorkerCoreConfiguration config = new WorkerCoreConfiguration();
                    config.setWorkerId(workerId);
                    config.setMaximumThreadByTaskType(tags);
                    config.setGroups(Arrays.asList(group));
                    config.setTasksRequestTimeout(1000);
                    try (WorkerCore core = new WorkerCore(config, "process1", locator, listener);) {
                        core.start();
                        core.setExecutorFactory(
                                (int tasktype, Map<String, Object> parameters) -> new TaskExecutor() {
                                    @Override
                                    public String executeTask(Map<String, Object> parameters) throws Exception {
                                        taskStartedLatch.countDown();
                                        System.out.println("executeTask: " + parameters);
                                        core.disconnect();
                                        disconnectedLatch.await(10000, TimeUnit.MILLISECONDS);
                                        return "theresult";
                                    }

                                }
                        );

                        assertTrue(taskStartedLatch.await(30, TimeUnit.SECONDS));

                        boolean ok = false;
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
                    }

                }

            }

        }
    }
}

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
public class SimpleBrokerRestartWithCheckpointTest {

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
        SimpleFormatter f = new SimpleFormatter();
        ch.setFormatter(f);
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }

    protected GroupMapperFunction createGroupMapperFunction() {
        return new GroupMapperFunction() {

            @Override
            public int getGroup(long taskid, String tasktype, String userid) {
                return groupsMap.getOrDefault(userid, 0);

            }
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
    public void snapshotTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());
        System.out.println("SETUPWORKDIR:" + workDir);
        long taskId;
        String workerId = "abc";
        String taskParams = "param";

        // startAsWritable a broker and do some work
        try (Broker broker = new Broker(new BrokerConfiguration(), new FileCommitLog(workDir, workDir), new TasksHeap(1000, createGroupMapperFunction()));) {
            broker.startAsWritable();
            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort())) {

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
                    config.setWorkerId(workerId);
                    config.setMaximumThreadByTaskType(tags);
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

                        taskId = broker.getClient().submitTask(TASKTYPE_MYTYPE, userId, taskParams,0,0,null).getTaskId();
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

                    // do a checkpoint
                    broker.checkpoint();
                    assertEquals(1, broker.getBrokerStatus().getCheckpointsCount());
                }
            }
        }

        // startAsWritable another broker
        try (Broker broker = new Broker(new BrokerConfiguration(), new FileCommitLog(workDir, workDir), new TasksHeap(1000, createGroupMapperFunction()));) {
            broker.startAsWritable();
            // ask for task status and worker status
            TaskStatusView task = broker.getClient().getTask(taskId);
            assertNotNull(task);
            assertEquals(taskParams, task.getData());
            assertEquals("theresult", task.getResult());
            assertEquals(Task.STATUS_FINISHED, task.getStatus());
        }
    }

}

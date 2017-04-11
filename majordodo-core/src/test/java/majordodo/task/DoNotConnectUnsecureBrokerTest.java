
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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import majordodo.clientfacade.AddTaskRequest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class DoNotConnectUnsecureBrokerTest {

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
    public void workerWillNeverConnectTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        final String SLOTID = "myslot";

        // startAsWritable a broker and request a task, with slot
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();

            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.setSsl(true);
                server.start();

                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
                    locator.setSslUnsecure(false);

                    CountDownLatch connectedLatch = new CountDownLatch(1);
                    CountDownLatch connectionErrorLatch = new CountDownLatch(1);
                    CountDownLatch disconnectedLatch = new CountDownLatch(1);
                    WorkerStatusListener listener = new WorkerStatusListener() {

                        @Override
                        public void connectionEvent(String event, WorkerCore core) {
                            if (event.equals(WorkerStatusListener.EVENT_CONNECTED)) {
                                connectedLatch.countDown();
                            }
                            if (event.equals(WorkerStatusListener.EVENT_CONNECTION_ERROR)) {
                                connectionErrorLatch.countDown();
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
                        assertTrue(connectionErrorLatch.await(10, TimeUnit.SECONDS));
                        locator.setSslUnsecure(true);
                        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
                    }
                    assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));

                }

            }
        }

    }

}

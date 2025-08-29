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
package majordodo.codepools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import majordodo.client.CodePoolUtils;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.CreateCodePoolRequest;
import majordodo.clientfacade.CreateCodePoolResult;
import majordodo.clientfacade.SubmitTaskResult;
import majordodo.clientfacade.TaskStatusView;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import majordodo.task.MemoryCommitLog;
import majordodo.task.Task;
import majordodo.task.TaskProperties;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.task.TasksHeap;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for codepools
 *
 * @author enrico.olivelli
 */
public class TaskExecutionWithCodePoolSerializedObjectTest {

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
    public void useSerializedTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        long taskId;
        String workerId = "abc";
        String taskParams = "newinstance:majordodo.testclients.SimpleExecutor";
        final String CODEPOOL = "codepool";
        Path expectedJar = mavenTargetDir
                .getParent()
                .getParent()
                .resolve("majordodo-test-clients")
                .resolve("target")
                .resolve("majordodo-test-clients-" + Broker.VERSION() + ".jar");
        if (!Files.isRegularFile(expectedJar)) {
            expectedJar = mavenTargetDir
                    .getParent()
                    .resolve("majordodo-test-clients")
                    .resolve("target")
                    .resolve("majordodo-test-clients-" + Broker.VERSION() + ".jar");
        }
        if (!Files.isRegularFile(expectedJar)) {
            fail("cannot find majordodo-test-clients-" + Broker.VERSION() + ".jar artifact");
        }
        byte[] mockCodePoolData = CodePoolUtils.createZipWithOneEntry("codepooltest.jar", Files.readAllBytes(expectedJar));

        // startAsWritable a broker and request a task, with slot
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            CreateCodePoolResult res1 = broker.getClient().createCodePool(new CreateCodePoolRequest(CODEPOOL, System.currentTimeMillis(), 0, mockCodePoolData));
            assertTrue(res1.ok);

            SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, CODEPOOL, Task.MODE_EXECUTE_OBJECT));
            taskId = res.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(res.getOutcome() == null);

            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {

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
                    Map<String, Integer> tags = new HashMap<>();
                    tags.put(TASKTYPE_MYTYPE, 1);

                    WorkerCoreConfiguration config = new WorkerCoreConfiguration();
                    config.setMaxPendingFinishedTaskNotifications(1);
                    config.setWorkerId(workerId);
                    config.setMaxThreadsByTaskType(tags);
                    config.setGroups(Arrays.asList(group));
                    config.setCodePoolsDirectory("target");
                    config.setEnableCodePools(true);
                    try (WorkerCore core = new WorkerCore(config, workerId, locator, listener);) {
                        core.start();
                        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

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

                }

            }

            TaskStatusView task = broker.getClient().getTask(taskId);
            assertEquals("SimpleExecutor run!", task.getResult());
        }

    }

}

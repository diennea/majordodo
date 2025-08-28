
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import majordodo.clientfacade.AddTaskRequest;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MaxTasksPerUserTest {

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
    public void userPeekTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

        String workerId = "abc";
        String taskParams = "param";
        int numTasks = 100;
        AtomicLong concurrentTasks = new AtomicLong();
        AtomicLong peekConcurrentTasks = new AtomicLong();

        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            List<Long> taskIds = new ArrayList<>();
            for (int i = 0; i < numTasks; i++) {
                SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null));
                long taskId = res.getTaskId();
                assertTrue(taskId > 0);
                taskIds.add(taskId);
                assertTrue(res.getOutcome() == null);
            }

            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.start();
                try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {

                    CountDownLatch connectedLatch = new CountDownLatch(1);
                    CountDownLatch disconnectedLatch = new CountDownLatch(1);
                    CountDownLatch allTaskExecuted = new CountDownLatch(numTasks);
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
                    int maxThreadsPerTaskType = 100;
                    int limitPercent = 10;
                    Map<String, Integer> tags = new HashMap<>();
                    tags.put(TASKTYPE_MYTYPE, maxThreadsPerTaskType);

                    WorkerCoreConfiguration config = new WorkerCoreConfiguration();
                    config.setWorkerId(workerId);
                    config.setMaxThreadsByTaskType(tags);
                    config.setMaxThreads(20000);
                    config.setGroups(Arrays.asList(group));
                    config.setMaxThreadPerUserPerTaskTypePercent(limitPercent);

                    int expectedPeek = (maxThreadsPerTaskType * limitPercent) / 100;

                    try (WorkerCore core = new WorkerCore(config, workerId, locator, listener);) {
                        core.start();
                        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));
                        core.setExecutorFactory(
                            (String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

                            @Override
                            public String executeTask(Map<String, Object> parameters) throws Exception {

                                long actual = concurrentTasks.incrementAndGet();
                                System.out.println("executeTask: " + parameters + " actual:" + actual);
                                try {
                                    Thread.sleep(100);
                                } finally {
                                    concurrentTasks.decrementAndGet();
                                    peekConcurrentTasks.accumulateAndGet(actual, (a, b) -> {
                                        // keep the maximum value
                                        // never decrease
                                        return a > b ? a : b;
                                    });
                                }
                                allTaskExecuted.countDown();
                                return "OK";
                            }

                        }
                        );

                        assertTrue(allTaskExecuted.await(1, TimeUnit.MINUTES));

                    }
                    assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
                    System.out.println("PEEK is " + peekConcurrentTasks + " over " + expectedPeek);
                    assertTrue(peekConcurrentTasks.get() <= expectedPeek);

                }

            }
        }

    }

}

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
package majordodo.replication;

import java.io.File;
import majordodo.task.StatusChangesLog;
import majordodo.executors.TaskExecutor;
import majordodo.network.BrokerLocator;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.BasicBrokerEnv;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.network.BrokerHostData;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class SimpleSecureZKBrokerLocatorTest extends BasicBrokerEnv {

    @BeforeClass
    public static void setUpJaas() {
        System.setProperty("java.security.auth.login.config", new File("src/test/resources/test_jaas.conf").getAbsolutePath());
    }

    @AfterClass
    public static void clearUpJaas() {
        System.clearProperty("java.security.auth.login.config");
    }

    NettyChannelAcceptor server;
    ZKTestEnv zkEnv;
    String host = "localhost";
    int port = 7000;

    @Override
    protected BrokerLocator createBrokerLocator() throws Exception {
        return new ZKBrokerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath());
    }

    @Override
    protected StatusChangesLog createStatusChangesLog() throws Exception {
        return new ReplicatedCommitLog(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), workDir, BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), true);
    }

    @Override
    protected void beforeStartBroker() throws Exception {
        zkEnv = new ZKTestEnv(workDir);
        zkEnv.startBookie();
    }

    @Override
    protected void afterStartBroker() throws Exception {
        server = new NettyChannelAcceptor(broker.getAcceptor(), host, port);
        server.start();
    }

    @After
    public void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
        if (zkEnv != null) {
            zkEnv.close();
        }
    }
    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

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
        config.setMaxPendingFinishedTaskNotifications(1);
        config.setWorkerId("workerid");
        config.setMaxThreadsByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        declareGroupForUser(userId, group);
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

            core.setExecutorFactory((String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {
                    System.out.println("executeTask " + parameters);
                    allTaskExecuted.countDown();
                    return "";
                }

            });

            String taskParams = "param";
            long taskId = getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
            assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
    }

}

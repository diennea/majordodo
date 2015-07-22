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
package dodo.replication;

import dodo.clustering.StatusChangesLog;
import dodo.executors.TaskExecutor;
import dodo.network.BrokerLocator;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.task.BasicBrokerEnv;
import dodo.task.Broker;
import dodo.worker.WorkerCore;
import dodo.worker.WorkerCoreConfiguration;
import dodo.worker.WorkerStatusListener;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class SimpleZKBrokerLocatorTest extends BasicBrokerEnv {

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
        return new ReplicatedCommitLog(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), workDir, Broker.formatHostdata(host, port));
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
        config.setWorkerId("workerid");
        config.setMaximumThreadByTaskType(tags);
        config.setGroups(Arrays.asList(group));
        groupsMap.put(userId,group);
        try (WorkerCore core = new WorkerCore(config, "here", getBrokerLocator(), listener);) {
            core.start();
            assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

            core.setExecutorFactory((String tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {
                    System.out.println("executeTask "+parameters);
                    allTaskExecuted.countDown();
                    return "";
                }

            });

            String taskParams = "param";
            System.out.println("QUI");
            long taskId = getClient().submitTask(TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null).getTaskId();
            System.out.println("QUA:"+taskId);

            assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        }
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
    }

}

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
import dodo.clustering.BrokerStatusSnapshot;
import dodo.clustering.FileCommitLog;
import dodo.clustering.LogSequenceNumber;
import dodo.clustering.MemoryCommitLog;
import dodo.clustering.StatusChangesLog;
import dodo.clustering.StatusEdit;
import dodo.clustering.Task;
import dodo.executors.TaskExecutor;
import dodo.network.BrokerLocator;
import dodo.network.netty.NettyBrokerLocator;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.worker.WorkerCore;
import dodo.worker.WorkerStatusListener;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class SimpleSnapshotTest extends BasicBrokerEnv {

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderLogs = new TemporaryFolder();

    @Override
    protected StatusChangesLog createStatusChangesLog() {
        return new FileCommitLog(folderSnapshots.getRoot().toPath(), folderLogs.getRoot().toPath());
    }

    NettyChannelAcceptor server;

    @Override
    protected BrokerLocator createBrokerLocator() {
        return new NettyBrokerLocator(server.getHost(), server.getPort());
    }

    @Before
    public void startServer() throws Exception {
        server = new NettyChannelAcceptor(broker.getAcceptor());
        server.start();
    }

    @After
    public void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    private static final int TASKTYPE_MYTYPE = 987;
    private static final String tenantName = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(tenantName, group);
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
        Map<Integer, Integer> tags = new HashMap<>();
        tags.put(TASKTYPE_MYTYPE, 1);
        WorkerCore core = new WorkerCore(10, "abc", "here", "localhost", tags, getBrokerLocator(), listener, Arrays.asList(group));
        core.start();
        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

        core.setExecutorFactory((int tasktype, Map<String, Object> parameters) -> new TaskExecutor() {

            @Override
            public String executeTask(Map<String, Object> parameters) throws Exception {

                allTaskExecuted.countDown();
                return "";
            }

        });

        String taskParams = "param";
        long taskId = getClient().submitTask(TASKTYPE_MYTYPE, tenantName, taskParams);

        assertTrue(allTaskExecuted.await(30, TimeUnit.SECONDS));

        core.stop();
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
        
        broker.checkpoint();
    }

}

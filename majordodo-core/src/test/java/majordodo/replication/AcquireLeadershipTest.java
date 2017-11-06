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

import majordodo.task.TasksHeap;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.TaskProperties;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.utils.TestUtils;
import static org.junit.Assert.assertNotNull;
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
public class AcquireLeadershipTest {

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

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderSnapshots2 = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderSnapshots3 = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void simpleBrokerReplicationTest() throws Exception {

        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();

            long taskId;
            String taskParams = "param";

            String host = "localhost";
            int port = 7000;
            String host2 = "localhost";
            int port2 = 7001;
            String host3 = "localhost";
            int port3 = 7002;

            BrokerConfiguration brokerConfig = new BrokerConfiguration();
            brokerConfig.setMaxWorkerIdleTime(5000);

            Broker broker1 = null;
            Broker broker2 = null;
            Broker broker3 = null;
            try {
                broker1 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));
                broker1.startAsWritable();
                try (NettyChannelAcceptor server1 = new NettyChannelAcceptor(broker1.getAcceptor(), host, port)) {
                    server1.start();

                    try {
                        broker2 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots2.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host2, port2, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));
                        broker2.start();
                        Broker _broker2 = broker2;
                        try (NettyChannelAcceptor server2 = new NettyChannelAcceptor(broker2.getAcceptor(), host2, port2)) {
                            server2.start();

                            taskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();

                            // need to write at least another entry to the ledger, if not the second broker could not see the add_task entry
                            broker1.noop();

                            assertNotNull(broker1.getClient().getTask(taskId));

                            // wait for the follower to actually have followed the stream of data
                            TestUtils.waitForCondition(() -> {
                                return _broker2.getClient().getTask(taskId) != null;
                            }, null, 100);
                        }

                        // broker1 dies, now broker2 becomes the leader
                        broker1.close();
                        broker1 = null;

                        TestUtils.waitForCondition(() -> {
                            return _broker2.isWritable();
                        }, null, 100);

                        // start a third broker, wait to get the new task
                        try {
                            broker3 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots3.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host3, port3, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));
                            broker3.start();
                            Broker _broker3 = broker3;
                            try (NettyChannelAcceptor server3 = new NettyChannelAcceptor(broker3.getAcceptor(), host3, port3)) {
                                server3.start();

                                // need to write at least another entry to the ledger, if not the second broker could not see the add_task entry
                                broker2.noop();

                                // wait for the follower to actually have followed the stream of data
                                TestUtils.waitForCondition(() -> {
                                    return _broker3.getClient().getTask(taskId) != null;
                                }, null, 100);
                            }
                        } finally {
                            if (broker3 != null) {
                                broker3.close();
                                broker3 = null;
                            }
                        }

                    } finally {
                        if (broker2 != null) {
                            broker2.close();
                            broker2 = null;
                        }
                    }
                }
            } finally {
                if (broker3 != null) {
                    broker3.close();
                }
                if (broker2 != null) {
                    broker2.close();
                }
                if (broker1 != null) {
                    broker1.close();
                }
            }
        }

    }

    @Test
    public void dataChangedOnZooKeeper() throws Exception {

        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();

            String taskParams = "param";
            String host = "localhost";
            int port = 7000;

            BrokerConfiguration brokerConfig = new BrokerConfiguration();
            brokerConfig.setMaxWorkerIdleTime(5000);

            ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false);

            try (Broker broker1 = new Broker(brokerConfig, log, new TasksHeap(1000, createTaskPropertiesMapperFunction()))) {
                broker1.startAsWritable();
                CountDownLatch latch = new CountDownLatch(1);
                broker1.setBrokerDiedCallback(new Runnable() {
                    @Override
                    public void run() {
                        latch.countDown();
                    }
                });
                System.out.println("SETTING DATA AT " + zkServer.getPath() + "/leader");
                log.getClusterManager().getZooKeeper().setData(
                    zkServer.getPath() + "/leader", "changed".getBytes("ASCII"), -1);
//                log.getClusterManager().getZooKeeper().delete(zkServer.getPath() + "/leader", -1);
                System.out.println("SET DATA AT " + zkServer.getPath() + "/leader");

                broker1.getClient()
                    .submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();

                try {
                    broker1.getClient()
                        .submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                } catch (Exception maybe) {
                }

                assertTrue(latch.await(1, TimeUnit.MINUTES));

            }
        }

    }

    @Test
    public void dataDeleteFromZooKeeper() throws Exception {

        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();

            String taskParams = "param";
            String host = "localhost";
            int port = 7000;

            BrokerConfiguration brokerConfig = new BrokerConfiguration();
            brokerConfig.setMaxWorkerIdleTime(5000);

            ReplicatedCommitLog log = new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false);

            try (Broker broker1 = new Broker(brokerConfig, log, new TasksHeap(1000, createTaskPropertiesMapperFunction()))) {
                broker1.startAsWritable();

                // delete the 'leader' node, as there is only one broker it will check to be the only leader again
                // and all will continue to work as expected
                log.getClusterManager().getZooKeeper().delete(zkServer.getPath() + "/leader", -1);

                broker1.getClient()
                    .submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                broker1.getClient()
                    .submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
            }
        }
    }
}

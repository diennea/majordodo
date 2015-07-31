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

import majordodo.replication.ReplicatedCommitLog;
import majordodo.client.TaskStatusView;
import majordodo.task.GroupMapperFunction;
import majordodo.task.TasksHeap;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import majordodo.network.netty.NettyChannelAcceptor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class SimpleBrokerStatusReplicationTest {

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

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
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

            BrokerConfiguration brokerConfig = new BrokerConfiguration();
            brokerConfig.setMaxWorkerIdleTime(5000);

            try (Broker broker1 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), Broker.formatHostdata(host, port)), new TasksHeap(1000, createGroupMapperFunction()));) {
                broker1.startAsWritable();
                try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker1.getAcceptor(),host, port)) {
                    server.start();

                    try (Broker broker2 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), Broker.formatHostdata(host2, port2)), new TasksHeap(1000, createGroupMapperFunction()));) {
                        broker2.start();

                        taskId = broker1.getClient().submitTask(TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null).getTaskId();

                        // need to write at least another entry to the ledger, if not the second broker could not see the add_task entry
                        broker1.noop();

                        assertNotNull(broker1.getClient().getTask(taskId));

                        boolean ok = false;
                        for (int i = 0; i < 10; i++) {
                            TaskStatusView task = broker2.getClient().getTask(taskId);
                            System.out.println("task:" + task);
                            Thread.sleep(1000);
                            if (task != null) {
                                ok = true;
                                break;
                            }
                        }
                        assertTrue(ok);

                    }
                }
            }
        }

    }
}

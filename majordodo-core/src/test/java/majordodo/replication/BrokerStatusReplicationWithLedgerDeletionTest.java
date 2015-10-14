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

import majordodo.clientfacade.TaskStatusView;
import majordodo.task.GroupMapperFunction;
import majordodo.task.TasksHeap;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.NettyChannelAcceptor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
public class BrokerStatusReplicationWithLedgerDeletionTest {

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

            try (ReplicatedCommitLog log1 = new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)));
                    Broker broker1 = new Broker(brokerConfig, log1, new TasksHeap(1000, createGroupMapperFunction()));) {
                broker1.startAsWritable();
                try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker1.getAcceptor(), host, port)) {
                    server.start();

                    taskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, null, 0)).getTaskId();

                    log1.setLedgersRetentionPeriod(1);
                    log1.setMaxLogicalLogFileSize(10);

                    System.out.println("ledgers:" + log1.getActualLedgersList());
                    broker1.checkpoint();
                    broker1.noop();
                    broker1.noop();
                    broker1.noop();
                    System.out.println("ledgers:" + log1.getActualLedgersList());
                    broker1.checkpoint();
                    broker1.noop();
                    broker1.noop();
                    broker1.noop();
                    System.out.println("ledgers:" + log1.getActualLedgersList());
                    broker1.checkpoint();
                    broker1.noop();
                    broker1.noop();
                    broker1.noop();
                    broker1.checkpoint();
                    System.out.println("ledgers:" + log1.getActualLedgersList());
                    assertEquals(1, log1.getActualLedgersList().getActiveLedgers().size());
                    assertFalse(log1.getActualLedgersList().getActiveLedgers().contains(log1.getActualLedgersList().getFirstLedger()));

                    try (ReplicatedCommitLog log2 = new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host2, port2, "", false, null)));
                            Broker broker2 = new Broker(brokerConfig, log2, new TasksHeap(1000, createGroupMapperFunction()));) {
                        broker2.start();

                        // need to write at least another entry to the ledger, if not the second broker could not see the add_task entry
                        broker1.noop();

                        assertNotNull(broker1.getClient().getTask(taskId));

                        boolean ok = false;
                        for (int i = 0; i < 10; i++) {
                            TaskStatusView task = broker2.getClient().getTask(taskId);
//                            System.out.println("task:" + task);
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

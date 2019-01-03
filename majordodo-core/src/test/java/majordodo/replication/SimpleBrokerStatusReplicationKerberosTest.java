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
import java.io.FileWriter;
import majordodo.clientfacade.TaskStatusView;
import majordodo.task.TasksHeap;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.TaskProperties;
import majordodo.task.TaskPropertiesMapperFunction;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
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
public class SimpleBrokerStatusReplicationKerberosTest {

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
    public TemporaryFolder folderZk = new TemporaryFolder();

    private MiniKdc kdc;
    private Properties conf;

    @Rule
    public TemporaryFolder kdcDir = new TemporaryFolder();

    @Rule
    public TemporaryFolder kerberosWorkDir = new TemporaryFolder();

    @Before
    public void startMiniKdc() throws Exception {

        createMiniKdcConf();
        kdc = new MiniKdc(conf, kdcDir.getRoot());
        kdc.start();

        String localhostName = "localhost";
        String principalServerNoRealm = "majordodo/" + localhostName;
        String principalServer = "majordodo/" + localhostName + "@" + kdc.getRealm();
        String principalClientNoRealm = "majordodoclient/" + localhostName;
        String principalClient = principalClientNoRealm + "@" + kdc.getRealm();

        System.out.println("adding principal: " + principalServerNoRealm);
        System.out.println("adding principal: " + principalClientNoRealm);

        File keytabClient = new File(kerberosWorkDir.getRoot(), "majordodoclient.keytab");
        kdc.createPrincipal(keytabClient, principalClientNoRealm);

        File keytabServer = new File(kerberosWorkDir.getRoot(), "majordodoserver.keytab");
        kdc.createPrincipal(keytabServer, principalServerNoRealm);

        File jaas_file = new File(kerberosWorkDir.getRoot(), "jaas.conf");
        try (FileWriter writer = new FileWriter(jaas_file)) {
            writer.write("\n"
                + "MajordodoServer {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabServer.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n"
                + "  principal=\"" + principalServer + "\";\n"
                + "};\n"
                + "\n"
                + "\n"
                + "\n"
                + "MajordodoClient {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabClient.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n"
                + "  principal=\"" + principalClient + "\";\n"
                + "};\n"
            );

        }

        File krb5file = new File(kerberosWorkDir.getRoot(), "krb5.conf");
        try (FileWriter writer = new FileWriter(krb5file)) {
            writer.write("[libdefaults]\n"
                + " default_realm = " + kdc.getRealm() + "\n"
                // disable UDP as Kerby will listen only on TCP by default
                + " udp_preference_limit=1\n"
                + "\n"
                + "[realms]\n"
                + " " + kdc.getRealm() + "  = {\n"
                + "  kdc = " + kdc.getHost() + ":" + kdc.getPort() + "\n"
                + " }"
            );

        }

        System.setProperty("java.security.auth.login.config", jaas_file.getAbsolutePath());
        System.setProperty("java.security.krb5.conf", krb5file.getAbsolutePath());
        javax.security.auth.login.Configuration.getConfiguration().refresh();

    }

    /**
     *
     * /**
     * Create a Kdc configuration
     */
    public void createMiniKdcConf() {
        conf = MiniKdc.createConf();
    }

    @After
    public void stopMiniKdc() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("java.security.krb5.conf");
        if (kdc != null) {
            kdc.stop();
        }
    }

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

            try (Broker broker1 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
                broker1.startAsWritable();
                try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker1.getAcceptor(), host, port)) {
                    server.start();

                    try (Broker broker2 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host2, port2, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
                        broker2.start();

                        taskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();

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

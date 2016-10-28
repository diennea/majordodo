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
import java.util.Properties;
import majordodo.network.BrokerHostData;
import majordodo.network.BrokerLocator;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.SimpleBrokerSuite;
import majordodo.task.StatusChangesLog;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * simple tests using real network connector
 *
 * @author enrico.olivelli
 */
public class KerberosReplicatedEnvTest extends SimpleBrokerSuite {

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

        String localhostName = "localhost.localdomain";
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
                + "\n"
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
    
    NettyChannelAcceptor server;
    ZKTestEnv zkEnv;
    String host = "localhost.localdomain";
    int port = 7000;

    @Override
    protected BrokerLocator createBrokerLocator() throws Exception {
        return new ZKBrokerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath());
    }

    @Override
    protected StatusChangesLog createStatusChangesLog() throws Exception {
        return new ReplicatedCommitLog(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), workDir,BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false);
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

}

/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package majordodo.replication;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

public class ZKTestEnv implements AutoCloseable {

    static {
        System.setProperty("zookeeper.admin.enableServer", "true");
        System.setProperty("zookeeper.admin.serverPort", "0");
    }

    TestingServer zkServer;
    EmbeddedServer embeddedServer;
    Path path;

    public ZKTestEnv(Path path) throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        props.put("kerberos.removeHostFromPrincipal", "true");
        props.put("kerberos.removeRealmFromPrincipal", "true");
        InstanceSpec spec = new InstanceSpec(path.toFile(), -1, -1, -1, true, 1, 2000, 200, props);
        zkServer = new TestingServer(spec, false);
        zkServer.start();
        this.path = path;
    }

    public void startBookie() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(0);
        conf.setUseHostNameAsBookieID(true);
        conf.setAllowEphemeralPorts(true);
        conf.setBookieId(UUID.randomUUID().toString());

        Path targetDir = path.resolve("bookie_data_"+conf.getBookieId());
        conf.setMetadataServiceUri("zk://" + zkServer.getConnectString() + "/ledgers");
        conf.setLedgerDirNames(new String[]{targetDir.toAbsolutePath().toString()});
        conf.setJournalDirName(targetDir.toAbsolutePath().toString());
        conf.setFlushInterval(1000);
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setGcWaitTime(10);
        conf.setAutoRecoveryDaemonEnabled(false);

        // in unit tests we do not need real network for bookies
        conf.setEnableLocalTransport(true);
//        conf.setDisableServerSocketBind(true);

        conf.setAllowLoopback(true);

        BookKeeperAdmin.format(conf, false, true);
        long _start = System.currentTimeMillis();

        BookieConfiguration bkConf = new BookieConfiguration(conf);
        embeddedServer = EmbeddedServer.builder(bkConf).build();

        embeddedServer.getLifecycleComponentStack().start();
        if (!waitForBookieServiceState(Lifecycle.State.STARTED)) {
            LOG.warning("bookie start timed out");
        }

        long _stop = System.currentTimeMillis();
        LOG.info("Booting Apache Bookkeeper finished. Time " + (_stop - _start) + " ms");
    }
    private static final Logger LOG = Logger.getLogger(ZKTestEnv.class.getName());

    public void stopBookie() throws InterruptedException {
        embeddedServer.getLifecycleComponentStack().close();
        if (!waitForBookieServiceState(Lifecycle.State.CLOSED)) {
            LOG.warning("bookie stop timed out");
        }
    }

    public String getAddress() {
        return zkServer.getConnectString();
    }

    public int getTimeout() {
        return 40000;
    }

    public String getPath() {
        return "/dodotest";
    }

    @Override
    public void close() throws Exception {
        try {
            if (embeddedServer != null) {
                embeddedServer.getLifecycleComponentStack().close();
                if (!waitForBookieServiceState(Lifecycle.State.CLOSED)) {
                    LOG.warning("bookie stop timed out");
                }
            }
        } catch (Throwable t) {
        }
        try {
            if (zkServer != null) {
                zkServer.close();
            }
        } catch (Throwable t) {
        }
    }

    private boolean waitForBookieServiceState(Lifecycle.State expectedState) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Lifecycle.State currentState = embeddedServer.getBookieService().lifecycleState();
            if (currentState == expectedState) {
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }

}

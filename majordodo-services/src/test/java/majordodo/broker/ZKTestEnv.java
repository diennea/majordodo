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
package majordodo.broker;

import java.nio.file.Path;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.curator.test.TestingServer;

public class ZKTestEnv implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ZKTestEnv.class.getName());

    TestingServer zkServer;
    EmbeddedServer embeddedServer;
    Path path;

    public ZKTestEnv(Path path) throws Exception {
        zkServer = new TestingServer(1282, path.toFile(), true);
        this.path = path;
    }

    public void startBookie() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(5621);
        conf.setUseHostNameAsBookieID(true);

        Path targetDir = path.resolve("bookie_data");
        conf.setZkServers("localhost:1282");
        conf.setLedgerDirNames(new String[]{targetDir.toAbsolutePath().toString()});
        conf.setJournalDirName(targetDir.toAbsolutePath().toString());
        conf.setFlushInterval(1000);
        conf.setAutoRecoveryDaemonEnabled(false);

        conf.setAllowLoopback(true);
        
        BookKeeperAdmin.format(conf, false, true);
        BookieConfiguration bkConf = new BookieConfiguration(conf);
        embeddedServer = EmbeddedServer.builder(bkConf).build();

        embeddedServer.getLifecycleComponentStack().start();
        if (!waitForBookieServiceState(Lifecycle.State.STARTED)) {
            LOG.warning("bookie start timed out");
        }
    }

    public String getAddress() {
        return "localhost:1282";
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

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

import majordodo.network.netty.GenericNettyBrokerLocator;
import majordodo.task.Broker;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Locates master broker using ZK
 *
 * @author enrico.olivelli
 */
public class ZKBrokerLocator extends GenericNettyBrokerLocator {

    private static final Logger LOGGER = Logger.getLogger(ZKBrokerLocator.class.getName());

    private AsyncCallback.DataCallback leaderData = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object param, byte[] data, Stat arg4) {
            if (Code.get(rc) == Code.OK) {
                try {
                    String brokerData = new String(data, StandardCharsets.UTF_8);
                    LOGGER.log(Level.SEVERE, "processResult:" + Code.get(rc) + " " + brokerData);
                    leaderBroker = Broker.parseHostdata(data);
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "error reading leader broker data", t);
                }
            } else {
                LOGGER.log(Level.SEVERE, "processResult:" + Code.get(rc) + " path=" + path);
                lookForLeader();
            }
        }
    };

    private void lookForLeader() {
        if (zk != null) {
            zk.getData(basePath + "/leader", workerWatcher, leaderData, null);
        }
    }

    private final Watcher workerWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent event) {
            LOGGER.info("event " + event.getPath() + " " + event.getState() + " " + event.getType());
        }

    };

    private ZooKeeper zk;
    private final String basePath;
    private InetSocketAddress leaderBroker;

    public ZKBrokerLocator(String zkAddress, int zkSessiontimeout, String basePath) throws Exception {
        zk = new ZooKeeper(zkAddress, zkSessiontimeout, workerWatcher);
        this.basePath = basePath;
        LOGGER.info("zkAddress:" + zkAddress + ", zkSessionTimeout:" + zkSessiontimeout + " basePath:" + basePath);
        lookForLeader();
    }

    @Override
    protected InetSocketAddress getServer() {
        return leaderBroker;
    }

    @Override
    public void close() {
        try {
            zk.close();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            zk = null;
        }
    }

}

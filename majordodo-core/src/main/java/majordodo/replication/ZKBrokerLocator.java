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

import java.util.function.Supplier;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.GenericNettyBrokerLocator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locates master broker using ZK
 *
 * @author enrico.olivelli
 */
public class ZKBrokerLocator extends GenericNettyBrokerLocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKBrokerLocator.class);

    private BrokerHostData lookForLeader() {
        ZooKeeper currentClient = zk.get();
        LOGGER.info("lookingForLeader broker zkclient={}", currentClient);
        if (currentClient != null) {
            try {
                Stat stat = new Stat();
                byte[] result = currentClient.getData(basePath + "/leader", workerWatcher, stat);
                BrokerHostData hostdata = BrokerHostData.parseHostdata(result);
                LOGGER.info("zknode {}/leader contains {} (stat {})", basePath, hostdata, stat);
                return hostdata;
            } catch (Throwable t) {
                LOGGER.error("error reading leader broker data", t);
                return null;
            }
        } else {
            return null;
        }
    }

    private final Watcher workerWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent event) {
            // only for debug purposes
            LOGGER.info("event {} {} {}", event.getPath(), event.getState(), event.getType());
        }

    };

    @Override
    public void brokerDisconnected() {
    }

    private Supplier<ZooKeeper> zk;
    private ZooKeeper ownedZk;
    private final String basePath;

    public ZKBrokerLocator(String zkAddress, int zkSessiontimeout, String basePath) throws Exception {
        ownedZk = new ZooKeeper(zkAddress, zkSessiontimeout, workerWatcher);
        zk = () -> ownedZk;
        this.basePath = basePath;
        LOGGER.info("zkAddress:{}, zkSessionTimeout:{} basePath:{}", zkAddress, zkSessiontimeout, basePath);
        lookForLeader();
    }

    public ZKBrokerLocator(Supplier<ZooKeeper> zk, String basePath) throws Exception {
        this.zk = zk;
        this.basePath = basePath;
        LOGGER.info("basePath:{} using system-provided Zookeeper Client", basePath);
        lookForLeader();
    }

    @Override
    protected BrokerHostData getServer() {
        return lookForLeader();
    }

    @Override
    public void close() {
        try {
            if (ownedZk != null) {
                ownedZk.close();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            ownedZk = null;
        }
    }

}

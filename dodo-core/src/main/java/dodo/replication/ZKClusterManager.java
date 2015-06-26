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
package dodo.replication;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Cluster Management
 *
 * @author enrico.olivelli
 */
public class ZKClusterManager {

    private static final Logger LOGGER = Logger.getLogger(ZKClusterManager.class.getName());

    private final ZooKeeper zk;
    private final LeaderShipChangeListener listener;

    private class SystemWatcher implements Watcher {

        @Override
        public void process(WatchedEvent we) {
            LOGGER.log(Level.SEVERE, "ZK event: " + we);
        }
    }
    private final String basePath;
    private final byte[] localhostdata;
    private final String leaderpath;

    public ZKClusterManager(String zkAddress, int zkTimeout, String basePath, LeaderShipChangeListener listener, byte[] localhostdata) throws Exception {
        this.zk = new ZooKeeper(zkAddress, zkTimeout, new SystemWatcher());
        this.basePath = basePath;
        this.listener = listener;
        this.localhostdata = localhostdata;
        this.leaderpath = basePath + "/leader";
    }

    public void start() throws Exception {
        if (this.zk.exists(basePath, false) == null) {
            LOGGER.log(Level.INFO, "creating base path " + basePath);
            this.zk.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private static enum MasterStates {

        ELECTED,
        NOTELECTED,
        RUNNING
    }
    private MasterStates state = MasterStates.NOTELECTED;

    private void checkMaster() {

    }

    private Watcher masterExistsWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent we) {
            if (we.getType() == EventType.NodeDeleted) {
                runForMaster();
            }
        }
    };
    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {

        @Override
        public void processResult(int rc, String string, Object o, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if (stat == null) {
                        state = MasterStates.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    checkMaster();
            }
        }
    };

    private void masterExists() {
        zk.exists(leaderpath, masterExistsWatcher, masterExistsCallback, null);
    }

    private void takeLeaderShip() {
        listener.leadershipAcquired();
    }

    private AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {

        @Override
        public void processResult(int code, String path, Object o, String name) {
            switch (Code.get(code)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    state = MasterStates.ELECTED;
                    takeLeaderShip();
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                case SESSIONEXPIRED:
                    onSessionExpired();
                    break;
                default:
                    LOGGER.log(Level.SEVERE, "bad ZK state " + KeeperException.create(Code.get(code), path));

            }
        }

    };

    private void onSessionExpired() {
        listener.leadershipLost();
    }

    public void runForMaster() {
        zk.create(leaderpath, localhostdata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    public void stop() {
        listener.leadershipLost();
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException ignore) {
            }
        }
    }

}

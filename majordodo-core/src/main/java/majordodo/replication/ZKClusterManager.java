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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import majordodo.task.LogNotAvailableException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster Management
 *
 * @author enrico.olivelli
 */
public class ZKClusterManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKClusterManager.class);

    private final ZooKeeper zk;
    private final LeaderShipChangeListener listener;

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    boolean isLeader() {
        return state == MasterStates.ELECTED;
    }

    CountDownLatch firstConnectionLatch = new CountDownLatch(1);

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED")
    void waitForConnection() throws InterruptedException {
        firstConnectionLatch.await(this.connectionTimeout, TimeUnit.MILLISECONDS);
    }

    void ensureLeaderRole() throws LogNotAvailableException {
        if (zk == null) {
            throw new LogNotAvailableException("no valid zookeeper handle");
        }
        try {
            byte[] data = zk.getData(leaderpath, masterExistsWatcher, null);
            LOGGER.info("data on ZK at {}: {}", leaderpath, new String(data, StandardCharsets.UTF_8));
            if (!Arrays.equals(data, localhostdata)) {
                LOGGER.error("expected data on ZK at {}: {} different from actual {}", leaderpath, new String(localhostdata, StandardCharsets.UTF_8), new String(data, StandardCharsets.UTF_8));
                throw new LogNotAvailableException("it seems that a new broker become leader");
            }
        } catch (KeeperException | InterruptedException err) {
            LOGGER.error("zookeeper client error", err);
            throw new LogNotAvailableException(err);
        }
    }

    private class SystemWatcher implements Watcher {

        @Override
        public void process(WatchedEvent we) {
            LOGGER.error("ZK event: " + we);
            switch (we.getState()) {
                case Expired:
                    onSessionExpired();
                    break;
                case SyncConnected:
                    firstConnectionLatch.countDown();
                    break;
                default:
                    break;
            }
        }
    }

    private final String basePath;
    private final byte[] localhostdata;
    private final String leaderpath;
    private final String discoverypath;
    private final String ledgersPath;
    private final int connectionTimeout;
    private final List<ACL> acls;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2")
    public ZKClusterManager(String zkAddress, int zkTimeout, String basePath, LeaderShipChangeListener listener,
                            byte[] localhostdata, boolean writeacls) throws Exception {
        this.zk = new ZooKeeper(zkAddress, zkTimeout, new SystemWatcher());
        this.basePath = basePath;
        this.listener = listener;
        this.acls = writeacls ? ZooDefs.Ids.CREATOR_ALL_ACL : ZooDefs.Ids.OPEN_ACL_UNSAFE;
        this.localhostdata = localhostdata;
        this.leaderpath = basePath + "/leader";
        this.discoverypath = basePath + "/discoverypath";
        this.ledgersPath = basePath + "/ledgers";
        this.connectionTimeout = zkTimeout; // TODO: specific configuration ?
    }

    /**
     * Let (embedded) brokers read actual list of ledgers used. in order to
     * perform extrernal clean ups
     *
     * @param zk
     * @param ledgersPath
     * @return
     * @throws LogNotAvailableException
     */
    public static LedgersInfo readActualLedgersListFromZookeeper(ZooKeeper zk, String ledgersPath) throws LogNotAvailableException {
        while (zk.getState() != ZooKeeper.States.CLOSED) {
            try {
                Stat stat = new Stat();
                byte[] actualLedgers = zk.getData(ledgersPath, false, stat);
                return LedgersInfo.deserialize(actualLedgers, stat.getVersion());
            } catch (KeeperException.NoNodeException firstboot) {
                LOGGER.error("node " + ledgersPath + " not found");
                return LedgersInfo.deserialize(null, -1); // -1 is a special ZK version
            } catch (KeeperException.ConnectionLossException error) {
                LOGGER.error("error while loading actual ledgers list at " + ledgersPath, error);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException err) {
                    // maybe stopping
                    throw new LogNotAvailableException(err);
                }
            } catch (Exception error) {
                LOGGER.error("error while loading actual ledgers list at " + ledgersPath, error);
                throw new LogNotAvailableException(error);
            }
        }
        throw new LogNotAvailableException(new Exception("zk client closed"));
    }

    public LedgersInfo getActualLedgersList() throws LogNotAvailableException {
        return readActualLedgersListFromZookeeper(zk, ledgersPath);
    }

    public void saveActualLedgersList(LedgersInfo info) throws LogNotAvailableException {
        byte[] actualLedgers = info.serialize();
        try {
            while (true) {
                try {
                    try {
                        Stat newStat = zk.setData(ledgersPath, actualLedgers, info.getZkVersion());
                        info.setZkVersion(newStat.getVersion());
                        LOGGER.error("save new ledgers list " + info);
                        return;
                    } catch (KeeperException.NoNodeException firstboot) {
                        zk.create(ledgersPath, actualLedgers, acls, CreateMode.PERSISTENT);
                    } catch (KeeperException.BadVersionException fenced) {
                        throw new LogNotAvailableException(new Exception("ledgers actual list was fenced, expecting version " + info.getZkVersion() + " " + fenced, fenced).fillInStackTrace());
                    }
                } catch (KeeperException.ConnectionLossException anyError) {
                    LOGGER.error("temporary error", anyError);
                    Thread.sleep(10000);
                } catch (Exception anyError) {
                    throw new LogNotAvailableException(anyError);
                }
            }
        } catch (InterruptedException stop) {
            LOGGER.error("fatal error", stop);
            throw new LogNotAvailableException(stop);
        }
    }

    public void start() throws Exception {
        try {
            if (this.zk.exists(basePath, false) == null) {
                LOGGER.error("creating base path " + basePath + ", acls:" + acls);
                try {
                    this.zk.create(basePath, new byte[0], acls, CreateMode.PERSISTENT);
                } catch (KeeperException anyError) {
                    throw new Exception("Could not init Zookeeper space at path " + basePath + ": " + anyError, anyError);
                }
            }
            if (this.zk.exists(discoverypath, false) == null) {
                LOGGER.error("creating discoverypath path " + discoverypath);
                try {
                    this.zk.create(discoverypath, new byte[0], acls, CreateMode.PERSISTENT);
                } catch (KeeperException anyError) {
                    throw new Exception("Could not init Zookeeper space at path " + discoverypath, anyError);
                }
            }
            String newPath = zk.create(discoverypath + "/brokers", localhostdata, acls, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.error("my own discoverypath path is " + newPath);

        } catch (KeeperException error) {
            throw new Exception("Could not init Zookeeper space at path " + basePath + ": " + error, error);
        }
    }

    public static enum MasterStates {

        ELECTED,
        NOTELECTED,
        RUNNING
    }

    private volatile MasterStates state = MasterStates.NOTELECTED;

    public MasterStates getState() {
        return state;
    }

    final AsyncCallback.DataCallback masterCheckBallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object o, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case NONODE:
                    requestLeadership();
                    break;
                case OK: {
                    LOGGER.info("data on ZK at {}: {}", leaderpath, new String(data, StandardCharsets.UTF_8));
                    if (state == MasterStates.ELECTED
                            && !Arrays.equals(data, localhostdata)) {
                        String localData = new String(localhostdata, StandardCharsets.UTF_8);
                        String currentData = new String(data, StandardCharsets.UTF_8);
                        LOGGER.error("expected data on ZK at {}: {} different from current {}", leaderpath, localData, currentData);
                        leadershipLost("expected data on ZK at " + leaderpath + ": " + localData + " different from current " + currentData);
                    }
                    zk.getData(leaderpath, masterExistsWatcher, masterCheckBallback, null);
                    break;
                }
                default:
                    LOGGER.info("masterCheckBallback - Unhandle code " + rc + " at " + path);
                    break;
            }
        }
    };

    private void checkMaster() {
        zk.getData(leaderpath, masterExistsWatcher, masterCheckBallback, null);
    }

    private final Watcher masterExistsWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent we) {
            LOGGER.error("process event {}, at {}, state {}", we.getType(), we.getPath(), we.getState());
            if (we.getType() == EventType.NodeDeleted) {
                requestLeadership();
            } else if (we.getType() == EventType.NodeDataChanged) {
                checkMaster();
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
                        requestLeadership();
                    }
                    break;
                default:
                    checkMaster();
            }
        }
    };

    private void masterExists() {
        LOGGER.error("setting watch at " + leaderpath);
        zk.exists(leaderpath, masterExistsWatcher, masterExistsCallback, null);
    }

    private void takeLeaderShip() {
        masterExists();
        listener.leadershipAcquired();
    }

    public byte[] getActualMaster() throws Exception {
        try {
            return zk.getData(leaderpath, false, new Stat());
        } catch (KeeperException.NoNodeException noMaster) {
            return null;
        }
    }

    private final AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {

        @Override
        public void processResult(int code, String path, Object o, String name) {
            LOGGER.info("masterCreateCallback path:" + path + ", code:" + Code.get(code));
            switch (Code.get(code)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    LOGGER.error("create success at " + path + ", code:" + Code.get(code) + ", I'm the new LEADER");
                    state = MasterStates.ELECTED;
                    takeLeaderShip();
                    break;
                case NODEEXISTS:
                    LOGGER.error("create failed at " + path + ", code:" + Code.get(code) + ", a LEADER already exists");
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                default:
                    LOGGER.error("bad ZK state " + KeeperException.create(Code.get(code), path));
            }
        }

    };

    private void leadershipLost(String reason) {
        listener.leadershipLost(reason);
    }

    private void onSessionExpired() {
        leadershipLost("ZK session expired");
    }

    public void requestLeadership() {
        zk.create(leaderpath, localhostdata, acls, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    @Override
    public void close() {
        listener.leadershipLost("service is stopping");
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException ignore) {
            }
        }
    }

}

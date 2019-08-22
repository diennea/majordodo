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
package majordodo.client.discovery;

import static org.apache.zookeeper.ServerAdminClient.stat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import majordodo.client.BrokerAddress;
import majordodo.client.BrokerDiscoveryService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Discevery service over zookeeper
 *
 * @author enrico.olivelli
 */
public class ZookeeperDiscoveryService implements BrokerDiscoveryService {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperDiscoveryService.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Supplier<ZooKeeper> client;
    private String zkPath = "/majordodo";
    private volatile BrokerAddress leaderBrokerCache;

    public ZookeeperDiscoveryService(Supplier<ZooKeeper> client) {
        this.client = client;
    }

    public ZookeeperDiscoveryService(ZooKeeper client) {
        this.client = () -> client;
    }

    public String getZkPath() {
        return zkPath;
    }

    public ZookeeperDiscoveryService setZkPath(String zkPath) {
        this.zkPath = zkPath;
        return this;
    }

    @Override
    public BrokerAddress getLeaderBroker() {
        BrokerAddress _address = leaderBrokerCache;
        if (_address != null) {
            return _address;
        }

        String leaderPath = zkPath + "/leader";
        ZooKeeper currentClient = client.get();
        if (currentClient == null) {
            LOGGER.log(Level.SEVERE, "zookeeper client is not available");
            return null;
        }
        LOGGER.log(Level.INFO, "lookingForLeader broker zkclient={0}", currentClient);
        try {
            Stat stat = new Stat();
            byte[] data = currentClient.getData(leaderPath, false, stat);
            _address = parseBrokerAddress(data, stat);
            leaderBrokerCache = _address;
            return _address;
        } catch (KeeperException.NoNodeException nobroker) {
            return null;
        } catch (KeeperException | InterruptedException | IOException err) {
            LOGGER.log(Level.SEVERE, "zookeeper client error", err);
            return null;
        }
    }

    @Override
    public void brokerFailed(BrokerAddress address) {
        LOGGER.log(Level.SEVERE, "brokerFailed {0}, discarding cached value {1}", new Object[]{address, leaderBrokerCache});
        leaderBrokerCache = null;
    }

    private BrokerAddress parseBrokerAddress(byte[] data, Stat stat) throws IOException {
        Map<String, String> res = MAPPER.readValue(new ByteArrayInputStream(data), Map.class);
        LOGGER.log(Level.INFO, "zookeeper client result {0} stat {1}", new Object[] {res, stat});
        BrokerAddress address = new BrokerAddress();
        address.setInfo(res);
        String url = res.get("client.api.url");
        if (url == null) {
            return null;
        }
        URI uri = URI.create(url);
        address.setAddress(uri.getHost());
        address.setPath(uri.getPath());
        address.setPort(uri.getPort());
        address.setProtocol(uri.getScheme());
        return address;
    }

    @Override
    public List<BrokerAddress> discoverBrokers() {
        String discoveryPath = zkPath + "/discovery";
        ZooKeeper zk = client.get();
        if (zk == null) {
            LOGGER.log(Level.SEVERE, "zookeeper client is not available");
            return null;
        }
        try {
            List<BrokerAddress> aa = new ArrayList<>();
            List<String> all = zk.getChildren(discoveryPath, false);
            for (String s : all) {
//                LOGGER.log(Level.SEVERE, "getting " + s);
                try {
                    Stat stat = new Stat();
                    byte[] data = zk.getData(s, false, stat);
                    BrokerAddress address = parseBrokerAddress(data, stat);
                    if (address != null) {
                        aa.add(address);
                    }
                } catch (KeeperException.NoNodeException nobroker) {
                    return null;
                }
            }
            return aa;
        } catch (KeeperException | InterruptedException | IOException err) {
            LOGGER.log(Level.SEVERE, "zookeeper client error", err);
            return null;
        }
    }

}

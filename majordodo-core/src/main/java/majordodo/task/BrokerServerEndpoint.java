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
package majordodo.task;

import majordodo.network.Channel;
import majordodo.network.ServerSideConnectionAcceptor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Connections manager broker-side
 *
 * @author enrico.olivelli
 */
public class BrokerServerEndpoint implements ServerSideConnectionAcceptor<BrokerSideConnection> {

    private static final Logger LOGGER = Logger.getLogger(BrokerServerEndpoint.class.getName());
    private final Map<String, BrokerSideConnection> workersConnections = new ConcurrentHashMap<>();
    private final Map<Long, BrokerSideConnection> connections = new ConcurrentHashMap<>();

    private final Broker broker;

    public BrokerServerEndpoint(Broker broker) {
        this.broker = broker;
    }

    @Override
    public BrokerSideConnection createConnection(Channel channel) {
        BrokerSideConnection connection = new BrokerSideConnection();
        connection.setBroker(broker);
        connection.setRequireAuthentication(broker.getConfiguration().isRequireAuthentication());
        connection.setChannel(channel);
        channel.setMessagesReceiver(connection);
        connections.put(connection.getConnectionId(), connection);
        return connection;
    }

    public Map<String, BrokerSideConnection> getWorkersConnections() {
        return workersConnections;
    }

    public Map<Long, BrokerSideConnection> getConnections() {
        return connections;
    }

    BrokerSideConnection getActualConnectionFromWorker(String workerId) {
        return workersConnections.get(workerId);
    }

    void connectionAccepted(BrokerSideConnection con) {
        LOGGER.log(Level.SEVERE, "connectionAccepted {0}", con);
        workersConnections.put(con.getClientId(), con);
    }

    void connectionClosed(BrokerSideConnection con) {
        LOGGER.log(Level.SEVERE, "connectionClosed {0}", con);
        connections.remove(con.getConnectionId());
        if (con.getClientId() != null) {
            workersConnections.remove(con.getClientId()); // to be remove only if the connection is the current connection
        }
    }

}

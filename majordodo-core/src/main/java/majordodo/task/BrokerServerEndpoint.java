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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import majordodo.network.Channel;
import majordodo.network.ServerSideConnectionAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connections manager broker-side
 *
 * @author enrico.olivelli
 */
public class BrokerServerEndpoint implements ServerSideConnectionAcceptor<BrokerSideConnection> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServerEndpoint.class);
    private final Map<String, BrokerSideConnection> workersConnections = new ConcurrentHashMap<>();
    private final Map<Long, BrokerSideConnection> connections = new ConcurrentHashMap<>();

    private final Broker broker;
    private ConnectionFactory connectionFactory = (channel, broker) -> {
        BrokerSideConnection connection = new BrokerSideConnection();
        connection.setBroker(broker);
        connection.setRequireAuthentication(broker.getConfiguration().isRequireAuthentication());
        connection.setChannel(channel);
        return connection;
    };

    public interface ConnectionFactory {
        BrokerSideConnection createConnection(Channel channel, Broker broker);
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public BrokerServerEndpoint(Broker broker) {
        this.broker = broker;
    }

    @Override
    public BrokerSideConnection createConnection(Channel channel) {
        BrokerSideConnection connection = connectionFactory.createConnection(channel, broker);
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
        LOGGER.info("connectionAccepted {}", con);
        workersConnections.put(con.getClientId(), con);
    }

    void connectionClosed(BrokerSideConnection con) {
        LOGGER.info("connectionClosed {}", con);
        connections.remove(con.getConnectionId());
        if (con.getClientId() != null) {
            workersConnections.remove(con.getClientId()); // to be remove only if the connection is the current connection
        }
    }

}

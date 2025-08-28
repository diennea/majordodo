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
package majordodo.network.netty;

import majordodo.network.BrokerLocator;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.ConnectionRequestInfo;
import majordodo.network.Message;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import majordodo.network.BrokerHostData;
import static majordodo.security.sasl.ClientAuthenticationUtils.performAuthentication;

/**
 * Network connection, based on Netty
 *
 * @author enrico.olivelli
 */
public abstract class GenericNettyBrokerLocator implements BrokerLocator {

    protected abstract BrokerHostData getServer();

    private boolean sslUnsecure;

    public boolean isSslUnsecure() {
        return sslUnsecure;
    }

    public void setSslUnsecure(boolean sslUnsecure) {
        this.sslUnsecure = sslUnsecure;
    }

    @Override
    public Channel connect(ChannelEventListener messageReceiver, ConnectionRequestInfo workerInfo) throws InterruptedException, BrokerNotAvailableException, BrokerRejectedConnectionException {
        boolean ok = false;
        NettyConnector connector = new NettyConnector(messageReceiver);
        try {
            BrokerHostData broker = getServer();
            LOGGER.error("connect to broker {}", broker);
            if (broker == null) {
                throw new BrokerNotAvailableException(new Exception("no broker available"));
            }
            InetSocketAddress addre = broker.getSocketAddress();
            connector.setPort(addre.getPort());
            String host = addre.getHostName();
            if (host == null) {
                host = addre.getAddress().getHostAddress();
            }
            connector.setHost(host);
            connector.setSsl(broker.isSsl());
            connector.setSslUnsecure(sslUnsecure);
            NettyChannel channel;
            try {
                channel = connector.connect();
            } catch (final Exception e) {
                throw new BrokerNotAvailableException(e);
            }
            try {
                performAuthentication(channel, channel.getRemoteHost(), workerInfo.getSharedSecret());
            } catch (Exception err) {
                throw new BrokerRejectedConnectionException("auth failed:" + err, err);
            }

            Message acceptMessage = Message.CONNECTION_REQUEST(workerInfo.getWorkerId(),
                workerInfo.getProcessId(), workerInfo.getLocation(), workerInfo.getSharedSecret(),
                workerInfo.getRunningTaskIds(), workerInfo.getMaxThreads(), workerInfo.getMaxThreadsByTaskType(),
                workerInfo.getGroups(), workerInfo.getExcludedGroups(), workerInfo.getResourceLimits(),
                workerInfo.getClientType(), workerInfo.getMaxThreadPerUserPerTaskTypePercent());
            try {
                Message connectionResponse = channel.sendMessageWithReply(acceptMessage, 10000);
                if (connectionResponse.type == Message.TYPE_ACK) {
                    ok = true;
                    return channel;
                } else {
                    throw new BrokerRejectedConnectionException("Broker rejected connection, response message:" + connectionResponse);
                }
            } catch (TimeoutException err) {
                throw new BrokerNotAvailableException(err);
            }
        } finally {
            if (!ok && connector != null) {
                connector.close();
            }
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericNettyBrokerLocator.class);

}

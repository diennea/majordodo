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
import java.util.logging.Logger;
import majordodo.network.BrokerHostData;
import majordodo.security.sasl.SaslNettyClient;
import majordodo.security.sasl.SaslUtils;

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
            if (broker == null) {
                throw new BrokerNotAvailableException(new Exception("no broker available"));
            }
            InetSocketAddress addre = broker.getSocketAddress();
            connector.setPort(addre.getPort());
            connector.setHost(addre.getAddress().getHostAddress());
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

            Message acceptMessage = Message.WORKER_CONNECTION_REQUEST(workerInfo.getWorkerId(), workerInfo.getProcessId(), workerInfo.getLocation(), workerInfo.getSharedSecret(), workerInfo.getRunningTaskIds(), workerInfo.getMaxThreads(), workerInfo.getMaxThreadsByTaskType(), workerInfo.getGroups(), workerInfo.getExcludedGroups(), workerInfo.getResourceLimits());
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

    private void performAuthentication(Channel _channel, String serverHostname, String sharedSecret) throws Exception {

        SaslNettyClient saslNettyClient = new SaslNettyClient(
            "worker", sharedSecret,
            serverHostname
        );

        byte[] firstToken = new byte[0];
        if (saslNettyClient.hasInitialResponse()) {
            firstToken = saslNettyClient.evaluateChallenge(new byte[0]);
        }
        Message saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_REQUEST(SaslUtils.AUTH_DIGEST_MD5, firstToken), 10000);

        for (int i = 0; i < 100; i++) {
            byte[] responseToSendToServer;
            switch (saslResponse.type) {
                case Message.TYPE_SASL_TOKEN_SERVER_RESPONSE:
                    byte[] token = (byte[]) saslResponse.parameters.get("token");
                    responseToSendToServer = saslNettyClient.evaluateChallenge(token);
                    saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_TOKEN(responseToSendToServer), 10000);
                    if (saslNettyClient.isComplete()) {                        
                        LOGGER.severe("SASL auth completed with success");
                        return;
                    }
                    break;
                case Message.TYPE_ERROR:
                    throw new Exception("Server returned ERROR during SASL negotiation, Maybe authentication failure (" + saslResponse.parameters + ")");
                default:
                    throw new Exception("Unexpected server response during SASL negotiation (" + saslResponse + ")");
            }
        }
        throw new Exception("SASL negotiation took too many steps");
    }
    private static final Logger LOGGER = Logger.getLogger(GenericNettyBrokerLocator.class.getName());

}

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
package majordodo.network.jvm;

import majordodo.network.BrokerLocator;
import majordodo.network.Channel;
import majordodo.network.ConnectionRequestInfo;
import majordodo.network.ChannelEventListener;
import majordodo.network.Message;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerRejectedConnectionException;
import java.util.concurrent.TimeoutException;

/**
 * Connects to the broker inside the same JVM (for tests)
 *
 * @author enrico.olivelli
 */
public class JVMBrokerLocator implements BrokerLocator {

    private final JVMBrokerSupportInterface broker;
    private final String brokerId;

    public JVMBrokerLocator(String brokerId) {
        this.brokerId = brokerId;
        if (brokerId != null) {
            this.broker = JVMBrokersRegistry.lookupBroker(brokerId);
        } else {
            this.broker = JVMBrokersRegistry.getDefaultBroker();
        }
    }

    @Override
    public Channel connect(ChannelEventListener worker, ConnectionRequestInfo workerInfo) throws InterruptedException, BrokerRejectedConnectionException, BrokerNotAvailableException {
        if (broker == null || !broker.isRunning()) {
            throw new BrokerNotAvailableException(new Exception("embedded broker " + brokerId + " is not running"));
        }
        JVMChannel workerSide = new JVMChannel();
        workerSide.setMessagesReceiver(worker);
        JVMChannel brokerSide = new JVMChannel();
        broker.getAcceptor().createConnection(brokerSide);
        brokerSide.setOtherSide(workerSide);
        workerSide.setOtherSide(brokerSide);
        Message acceptMessage = Message.WORKER_CONNECTION_REQUEST(workerInfo.getWorkerId(), workerInfo.getProcessId(), workerInfo.getLocation(), workerInfo.getSharedSecret(), workerInfo.getRunningTaskIds(),workerInfo.getMaxThreads(),workerInfo.getMaxThreadsByTaskType(),workerInfo.getGroups(),workerInfo.getExcludedGroups());
        try {
            Message connectionResponse = workerSide.sendMessageWithReply(acceptMessage, 10000);
            if (connectionResponse.type == Message.TYPE_ACK) {
                return workerSide;
            } else {
                throw new BrokerRejectedConnectionException("Broker rejected connection, response message:" + connectionResponse);
            }
        } catch (TimeoutException err) {
            throw new BrokerNotAvailableException(err);
        }

    }

    @Override
    public void brokerDisconnected() {
    }

}

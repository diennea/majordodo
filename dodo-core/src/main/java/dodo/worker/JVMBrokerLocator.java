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
package dodo.worker;

import dodo.network.Channel;
import dodo.network.Message;
import dodo.network.jvm.JVMChannel;
import dodo.task.Broker;
import java.util.concurrent.TimeoutException;

/**
 * Connects to the broker inside the same JVM (for tests)
 *
 * @author enrico.olivelli
 */
public class JVMBrokerLocator implements BrokerLocator {

    private Broker broker;

    public JVMBrokerLocator(Broker broker) {
        this.broker = broker;
    }

    @Override
    public Channel connect(WorkerCore worker) throws InterruptedException, BrokerRejectedConnectionException, BrokerNotAvailableException {
        if (!broker.isRunning()) {
            throw new BrokerNotAvailableException(new Exception("embedded broker is not running"));
        }
        JVMChannel workerSide = new JVMChannel(worker);
        BrokerSideConnection connection = new BrokerSideConnection();
        JVMChannel brokerSide = new JVMChannel(connection);
        brokerSide.setOtherSide(workerSide);
        workerSide.setOtherSide(brokerSide);
        connection.setChannel(brokerSide);
        connection.setBroker(broker);

        broker.getAcceptor().registerConnection(connection);

        Message acceptMessage = Message.WORKER_CONNECTION_REQUEST(worker.getWorkerId(), worker.getProcessId(), worker.getMaximumThreadPerTag(), worker.getLocation(),worker.getRunningTasks().keySet());
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

}

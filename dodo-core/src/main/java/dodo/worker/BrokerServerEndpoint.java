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

import dodo.clustering.Action;
import dodo.network.Message;
import dodo.scheduler.WorkerManager;
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connections manager broker-side
 *
 * @author enrico.olivelli
 */
public class BrokerServerEndpoint {

    private final Map<String, BrokerSideConnection> workersConnections = new ConcurrentHashMap<String, BrokerSideConnection>();

    private Broker broker;

    public BrokerServerEndpoint(Broker broker) {
        this.broker = broker;
    }

    public void acceptConnection(BrokerSideConnection connection, final Message connectionRequestMessage) {
        try {
            String workerId = connection.getWorkerId();
            BrokerSideConnection actual = workersConnections.get(workerId);
            if (actual != null) {
                connection.answerConnectionNotAcceptedAndClose(connectionRequestMessage,new Exception("already connected from "+workerId));
                return;
            }
            Action action = Action.NODE_REGISTERED(workerId, connection.getLocation(), connection.getMaximumNumberOfTasks());
            broker.executeAction(action, (a, result) -> {
                if (result.error != null) {
                    connection.answerConnectionNotAcceptedAndClose(connectionRequestMessage,result.error);
                    return;
                }
                workersConnections.put(workerId, connection);
                WorkerManager manager = broker.getWorkerManager(workerId);
                connection.activate(manager);
            });

        } catch (InterruptedException ex) {
            connection.answerConnectionNotAcceptedAndClose(connectionRequestMessage,ex);
        } catch (InvalidActionException ex) {
            connection.answerConnectionNotAcceptedAndClose(connectionRequestMessage,ex);
        }
    }

}

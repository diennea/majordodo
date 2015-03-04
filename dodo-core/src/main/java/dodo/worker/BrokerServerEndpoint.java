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
import dodo.clustering.LogNotAvailableException;
import dodo.scheduler.WorkerManager;
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public void acceptConnection(BrokerSideConnection connection) throws ConnectionDeniedException {
        try {
            String workerId = connection.getWorkerId();
            BrokerSideConnection actual = workersConnections.get(workerId);
            if (actual != null) {
                throw new ConnectionDeniedException("worker " + workerId + " is already connected, processid = " + actual.getWorkerProcessId() + ", location = " + actual.getLocation());
            }
            WorkerManager manager = broker.executeAction(Action.NODE_REGISTERED(workerId, connection.getLocation(), connection.getMaximumNumberOfTasks())).workerManager;
            connection.activate(manager);
            manager.nodeConnected();
            workersConnections.put(workerId, connection);
        } catch (LogNotAvailableException ex) {
            throw new ConnectionDeniedException("system not available");
        } catch (InterruptedException ex) {
            throw new ConnectionDeniedException("system not available");
        } catch (InvalidActionException ex) {
            throw new ConnectionDeniedException("broker refused connection:" + ex);
        }
    }

}

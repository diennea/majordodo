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

import majordodo.task.Broker;
import majordodo.task.BrokerSideConnection;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runtime status manager for a Node
 *
 * @author enrico.olivelli
 */
public class WorkerManager {

    private static final Logger LOGGER = Logger.getLogger(WorkerManager.class.getName());

    private final String workerId;
    private final Broker broker;
    private BrokerSideConnection connection;
    private final int maxWorkerIdleTime;
    private long lastActivity = System.currentTimeMillis();

    public WorkerManager(String workerId, Broker broker) {
        this.workerId = workerId;
        this.broker = broker;
        this.maxWorkerIdleTime = broker.getConfiguration().getMaxWorkerIdleTime();
    }

    public Broker getBroker() {
        return broker;
    }

    public void wakeUp() {
        WorkerStatus status = broker.getBrokerStatus().getWorkerStatus(workerId);
        if (status == null) {
            // ???
            return;
        }
        if (status.getStatus() == WorkerStatus.STATUS_DEAD) {
            return;
        }
        long now = System.currentTimeMillis();
        if (connection == null) {
            LOGGER.log(Level.FINE, "wakeup {0} -> no connection", workerId);
            try {
                if (status.getStatus() == WorkerStatus.STATUS_CONNECTED) {
                    LOGGER.log(Level.FINE, "wakeup {0} -> no connection -> setting STATUS_DISCONNECTED", workerId);
                    broker.declareWorkerDisconnected(workerId, now);
                } else {
                    long delta = now - lastActivity;
                    if (delta > maxWorkerIdleTime) {
                        LOGGER.log(Level.SEVERE, "wakeup {0} -> declaring dead (connection did not reestabilish in time)", workerId);
                        broker.declareWorkerDead(workerId, now);

                        for (long taskId : tasksRunningOnRemoteWorker) {
                            LOGGER.log(Level.SEVERE, "wakeup {0} -> requesting recovery for task {1}", new Object[]{workerId, taskId});
                            broker.taskNeedsRecoveryDueToWorkerDeath(taskId, workerId);
                        }
                    }
                }
            } catch (LogNotAvailableException err) {
                LOGGER.log(Level.SEVERE, "wakeup " + workerId + " -> worker lifecycle error", err);
            }
        } else {
            if (lastActivity < connection.getLastReceivedMessageTs()) {
                lastActivity = connection.getLastReceivedMessageTs();
            }
            LOGGER.log(Level.FINE, "wakeup {0}, lastActivity {1} ", new Object[]{workerId, new java.util.Date(lastActivity)});
            if (connection != null) {
                int max = 100;
                while (max-- > 0) {
                    Long taskToBeSubmitted = taskToBeSubmittedToRemoteWorker.poll();
                    if (taskToBeSubmitted != null) {
                        LOGGER.log(Level.INFO, "wakeup {0} -> assign task {1}", new Object[]{workerId, taskToBeSubmitted});
                        Task task = broker.getBrokerStatus().getTask(taskToBeSubmitted);
                        if (task == null) {
                            // task disappeared ?
                            LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task disappeared?", new Object[]{workerId, taskToBeSubmitted});
                        } else {
                            if (task.getStatus() == Task.STATUS_RUNNING && task.getWorkerId().equals(workerId)) {
                                connection.sendTaskAssigned(task, (Void result, Throwable error) -> {
                                    if (error != null) {
                                        // the write failed
                                        taskToBeSubmittedToRemoteWorker.add(taskToBeSubmitted);
                                    } else {
                                        tasksRunningOnRemoteWorker.add(taskToBeSubmitted);
                                    }
                                });
                            } else {
                                LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task {2} not in running status for this worker", new Object[]{workerId, taskToBeSubmitted, task});
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    private final Set<Long> tasksRunningOnRemoteWorker = new ConcurrentSkipListSet<>();
    private final BlockingQueue<Long> taskToBeSubmittedToRemoteWorker = new LinkedBlockingDeque<>();

    public void activateConnection(BrokerSideConnection connection) {
        LOGGER.log(Level.INFO, "activateConnection {0}", connection);
        lastActivity = System.currentTimeMillis();
        this.connection = connection;
    }

    public void taskShouldBeRunning(long taskId) {
        tasksRunningOnRemoteWorker.add(taskId);
    }

    public void taskAssigned(long taskId) {
        taskToBeSubmittedToRemoteWorker.add(taskId);
    }

    public void deactivateConnection(BrokerSideConnection aThis) {
        LOGGER.log(Level.INFO, "deactivateConnection {0}", connection);
        if (this.connection == aThis) {
            this.connection = null;
        }
    }

}

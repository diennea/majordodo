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
import java.util.concurrent.locks.ReentrantLock;
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
        if (workerId == null) {
            throw new NullPointerException();
        }
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
            LOGGER.log(Level.SEVERE, "wakeup {0} -> no status?", workerId);
            return;
        }
        if (status.getStatus() == WorkerStatus.STATUS_DEAD) {
            return;
        }
        long now = System.currentTimeMillis();
        connectionLock.lock();
        try {
            if (connection != null && !connection.validate()) {
                connection.close();
                deactivateConnection(connection);
                connection = null;
            }
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

                            LOGGER.log(Level.SEVERE, "wakeup {0} -> requesting recovery for tasks {1}", new Object[]{workerId, tasksRunningOnRemoteWorker});
                            broker.tasksNeedsRecoveryDueToWorkerDeath(tasksRunningOnRemoteWorker, workerId);
                            tasksRunningOnRemoteWorker.clear();
                        }
                    }
                } catch (LogNotAvailableException err) {
                    LOGGER.log(Level.SEVERE, "wakeup " + workerId + " -> worker lifecycle error", err);
                }
            } else {
                if (lastActivity < connection.getLastReceivedMessageTs()) {
                    lastActivity = connection.getLastReceivedMessageTs();
                }
                LOGGER.log(Level.FINEST, "wakeup {0}, lastActivity {1}  taskToBeSubmittedToRemoteWorker {2} tasksRunningOnRemoteWorker {3}", new Object[]{workerId, new java.util.Date(lastActivity), taskToBeSubmittedToRemoteWorker, tasksRunningOnRemoteWorker});
                if (connection != null) {
                    int max = 100;
                    while (max-- > 0) {
                        Long taskToBeSubmitted = taskToBeSubmittedToRemoteWorker.poll();
                        if (taskToBeSubmitted != null) {
                            LOGGER.log(Level.FINEST, "wakeup {0} -> assign task {1}", new Object[]{workerId, taskToBeSubmitted});
                            Task task = broker.getBrokerStatus().getTask(taskToBeSubmitted);
                            if (task == null) {
                                // task disappeared ?
                                LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task disappeared?", new Object[]{workerId, taskToBeSubmitted});
                            } else {
                                if (tasksRunningOnRemoteWorker.contains(taskToBeSubmitted)) {
                                    LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task {2} is already running on worker", new Object[]{workerId, taskToBeSubmitted, task});
                                    return;
                                }
                                if (task.getStatus() == Task.STATUS_RUNNING && task.getWorkerId().equals(workerId)) {
                                    connection.sendTaskAssigned(task, (Void result, Throwable error) -> {
                                        if (error != null) {
                                            // the write failed
                                            LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task {2} network failure, rescheduling for retry:{3}", new Object[]{workerId, taskToBeSubmitted, task, error});
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
        } finally {
            connectionLock.unlock();
        }
    }

    private final Set<Long> tasksRunningOnRemoteWorker = new ConcurrentSkipListSet<>();
    private final BlockingQueue<Long> taskToBeSubmittedToRemoteWorker = new LinkedBlockingDeque<>();

    public void activateConnection(BrokerSideConnection connection) {
        LOGGER.log(Level.INFO, "activateConnection {0}", connection);        
        connectionLock.lock();
        try {
            lastActivity = System.currentTimeMillis();
            this.connection = connection;
        } finally {
            connectionLock.unlock();
        }
    }

    public void taskShouldBeRunning(long taskId) {
        LOGGER.severe(workerId + " taskShouldBeRunning " + taskId);
        tasksRunningOnRemoteWorker.add(taskId);
        taskToBeSubmittedToRemoteWorker.remove(taskId);
    }

    public void taskAssigned(long taskId) {
        taskToBeSubmittedToRemoteWorker.add(taskId);
        if (tasksRunningOnRemoteWorker.contains(taskId)) {
            LOGGER.log(Level.SEVERE, "taskAssigned {0}, the task is already running on remote worker?", new Object[]{taskId});
        }
    }

    private ReentrantLock connectionLock = new ReentrantLock();

    public void deactivateConnection(BrokerSideConnection aThis) {
        connectionLock.lock();
        try {
            LOGGER.log(Level.INFO, "deactivateConnection {0}", connection);
            if (this.connection == aThis) {
                this.connection = null;
            }
        } finally {
            connectionLock.unlock();
        }
    }

    void taskFinished(long taskId) {
        tasksRunningOnRemoteWorker.remove(taskId);
    }

}

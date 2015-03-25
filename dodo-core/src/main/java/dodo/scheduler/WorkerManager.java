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
package dodo.scheduler;

import dodo.callbacks.SimpleCallback;
import dodo.clustering.Task;
import dodo.task.Broker;
import dodo.worker.BrokerSideConnection;
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
    private final Scheduler scheduler;
    private final Broker broker;
    private BrokerSideConnection connection;
    private long lastWakeup = System.currentTimeMillis();

    public WorkerManager(String workerId, Scheduler scheduler, Broker broker) {
        this.workerId = workerId;
        this.scheduler = scheduler;
        this.broker = broker;
    }

    public Broker getBroker() {
        return broker;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    private static final long MAX_IDLE_TIME = 1000 * 60; // TODO: configuration

    public void wakeUp() {
        long now = System.currentTimeMillis();
        long _lastWakeUpDelta = now - lastWakeup;
        lastWakeup = now;
        if (connection == null) {
            LOGGER.log(Level.FINE, "wakeup {0} -> no connection", workerId);
            WorkerStatus status = broker.getBrokerStatus().getWorkerStatus(workerId);
            if (status == null) {
                // ???
                return;
            }
            if (status.getStatus() == WorkerStatus.STATUS_CONNECTED) {
                status.setStatus(WorkerStatus.STATUS_DISCONNECTED);
            }
            if (_lastWakeUpDelta > MAX_IDLE_TIME) {
                status.setStatus(WorkerStatus.STATUS_DEAD);
                connection.workerDied();
                scheduler.workerDied(workerId);
                connection = null;
                LOGGER.log(Level.SEVERE, "wakeup {0} -> declaring dead (connection did not reestabilish in time)", workerId);
            }
            return;
        }
        LOGGER.log(Level.FINE, "wakeup {0} ", workerId);
        long delta = System.currentTimeMillis() - connection.getLastReceivedMessageTs();
        if (delta > MAX_IDLE_TIME) {
            LOGGER.log(Level.FINE, "worker {0} is no more alive, receovery needed", workerId);
            WorkerStatus status = broker.getBrokerStatus().getWorkerStatus(workerId);
            status.setStatus(WorkerStatus.STATUS_DEAD);
            connection.workerDied();
            scheduler.workerDied(workerId);
            LOGGER.log(Level.SEVERE, "wakeup {0} -> declaring dead (no message received)", workerId);
            connection = null;
        }

        if (connection != null) {
            Long taskToBeSubmitted = taskToBeSubmittedToRemoteWorker.poll();            
            if (taskToBeSubmitted != null) {
                LOGGER.log(Level.INFO, "wakeup {0} -> assign task {1}", new Object[]{workerId, taskToBeSubmitted});
                Task task = broker.getBrokerStatus().getTask(taskToBeSubmitted);
                if (task == null) {
                    // task disappeared ?
                    LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task disappeared?", new Object[]{workerId, taskToBeSubmitted});
                } else {
                    if (task.getStatus() == Task.STATUS_RUNNING && task.getWorkerId().equals(workerId)) {
                        connection.sendTaskAssigned(task, new SimpleCallback<Void>() {

                            @Override
                            public void onResult(Void result, Throwable error) {
                                if (error != null) {
                                    // the write failed
                                    taskToBeSubmittedToRemoteWorker.add(taskToBeSubmitted);
                                } else {
                                    tasksRunningOnRemoteWorker.add(taskToBeSubmitted);
                                }
                            }
                        });
                    } else {
                        LOGGER.log(Level.SEVERE, "wakeup {0} -> assign task {1}, task {2} not in running status for this worker", new Object[]{workerId, taskToBeSubmitted, task});
                    }
                }
            }
        }
    }

    private final Set<Long> tasksRunningOnRemoteWorker = new ConcurrentSkipListSet<>();
    private final BlockingQueue<Long> taskToBeSubmittedToRemoteWorker = new LinkedBlockingDeque<>();

    public void activateConnection(BrokerSideConnection connection) {
        LOGGER.log(Level.INFO, "activateConnection {0}", connection);
        this.connection = connection;
    }

    public void taskAssigned(long taskId) {
        taskToBeSubmittedToRemoteWorker.add(taskId);
    }

}

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

import dodo.clustering.Action;
import dodo.clustering.LogNotAvailableException;
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import dodo.worker.BrokerSideConnection;
import dodo.task.Task;
import dodo.task.TaskQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runtime status manager for a Node
 *
 * @author enrico.olivelli
 */
public class WorkerManager {

    private final WorkerStatus workerStatus;
    private final Scheduler scheduler;
    private final Broker broker;
    private BrokerSideConnection connection;

    public WorkerManager(WorkerStatus node, Scheduler scheduler, Broker broker) {
        this.workerStatus = node;
        this.scheduler = scheduler;
        this.broker = broker;
    }

    public Broker getBroker() {
        return broker;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public WorkerStatus getWorker() {
        return workerStatus;
    }

    public void activateConnection(BrokerSideConnection connection) {
        this.connection = connection;

        workerStatus.getMaximumNumberOfTasks().forEach((tag, max) -> {
            int remaining;
            AtomicInteger actualCount = workerStatus.getActualNumberOfTasks().get(tag);
            if (actualCount != null) {
                remaining = max - actualCount.get();
            } else {
                remaining = max;
            }
            for (int i = 0; i < remaining; i++) {
                scheduler.taskFinished(workerStatus.getWorkerId(), tag);
            }
        });
    }

    public void taskAssigned(Task task) {
        System.out.println("taskAssigned " + task.getTaskId());
        connection.sendTaskAssigned(task);
    }

    public void detectedOldNodeProcess() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}

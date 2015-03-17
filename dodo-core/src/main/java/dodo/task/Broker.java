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
package dodo.task;

import dodo.clustering.Task;
import dodo.clustering.TaskQueue;
import dodo.client.ClientFacade;
import dodo.clustering.StatusEdit;
import dodo.clustering.ActionResult;
import dodo.clustering.BrokerStatus;
import dodo.clustering.LogNotAvailableException;
import dodo.clustering.StatusChangesLog;
import dodo.scheduler.DefaultScheduler;
import dodo.scheduler.Workers;
import dodo.scheduler.Scheduler;
import dodo.worker.BrokerServerEndpoint;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Global statu s of the organizer
 *
 * @author enrico.olivelli
 */
public class Broker {

    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());

    private final StatusChangesLog log;
    private final Workers workers;
    private final BrokerStatus brokerStatus;
    private final Scheduler scheduler;
    private final BrokerServerEndpoint acceptor;
    private final ClientFacade client;
    private volatile boolean started;

    public ClientFacade getClient() {
        return client;
    }

    public Workers getWorkers() {
        return workers;
    }

    public BrokerStatus getBrokerStatus() {
        return brokerStatus;
    }

    public Broker(StatusChangesLog log) {
        this.log = log;
        this.scheduler = new DefaultScheduler(this);
        this.workers = new Workers(scheduler, this);
        this.acceptor = new BrokerServerEndpoint(this);
        this.client = new ClientFacade(this);
        this.brokerStatus = new BrokerStatus(log);
    }

    public void start() {
        this.brokerStatus.reload();
        this.workers.start();
        started = true;
    }

    public void stop() {
        this.workers.stop();
        started = false;
    }

    public BrokerServerEndpoint getAcceptor() {
        return acceptor;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public boolean isRunning() {
        return started;
    }

    public static interface ActionCallback {

        public void actionExecuted(StatusEdit action, ActionResult result);
    }

    public long addTask(String queueName,
            String taskType,
            String queueTag,
            Map<String, Object> parameters) throws LogNotAvailableException {
        if (queueTag == null) {
            queueTag = TaskQueue.DEFAULT_TAG;
        }
        StatusEdit addTask = StatusEdit.ADD_TASK(queueName, taskType, parameters, queueTag);
        long taskId = this.brokerStatus.applyModification(addTask).newTaskId;
        scheduler.wakeUpOnNewTask(taskId, queueTag);
        return taskId;
    }

    public void assignTaskToWorker(String workerId, long taskId) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.ASSIGN_TASK_TO_WORKER(taskId, workerId);
        this.brokerStatus.applyModification(edit);
        this.workers.wakeUpOnTaskAssigned(workerId, taskId);
    }

    public void taskFinished(String workerId, long taskId, int finalstatus, Map<String, Object> results) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.TASK_FINISHED(taskId, workerId, finalstatus, results);
        this.brokerStatus.applyModification(edit);
        Task task = brokerStatus.getTask(taskId);
        if (task != null) {
            TaskQueue queue = brokerStatus.getTaskQueue(task.getQueueName());
            if (queue != null) {
                this.scheduler.wakeUpOnTaskFinished(workerId, queue.getTag());
            }
        }
    }

    public void workerConnected(String workerId, String nodeLocation, Map<String, Integer> maximumNumberOfTasksPerTag, Set<Long> actualRunningTasks, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_CONNETED(workerId, nodeLocation, maximumNumberOfTasksPerTag, actualRunningTasks, timestamp);
        this.brokerStatus.applyModification(edit);
    }

}

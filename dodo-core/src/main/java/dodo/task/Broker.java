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

import dodo.client.ClientFacade;
import dodo.clustering.StatusEdit;
import dodo.clustering.ActionResult;
import dodo.clustering.BrokerStatus;
import dodo.clustering.LogNotAvailableException;
import dodo.clustering.StatusChangesLog;
import dodo.clustering.Task;
import dodo.clustering.TasksHeap;
import dodo.scheduler.Workers;
import dodo.worker.BrokerServerEndpoint;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Global status of the broker
 *
 * @author enrico.olivelli
 */
public class Broker implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());

    private final Workers workers;
    public final TasksHeap tasksHeap;
    private final BrokerStatus brokerStatus;
    private final BrokerServerEndpoint acceptor;
    private final ClientFacade client;
    private volatile boolean started;
    private final BrokerConfiguration configuration;
    private final CheckpointScheduler checkpointScheduler;

    public BrokerConfiguration getConfiguration() {
        return configuration;
    }

    public ClientFacade getClient() {
        return client;
    }

    public Workers getWorkers() {
        return workers;
    }

    public BrokerStatus getBrokerStatus() {
        return brokerStatus;
    }

    public Broker(BrokerConfiguration configuration, StatusChangesLog log, TasksHeap tasksHeap) {
        this.configuration = configuration;
        this.workers = new Workers(this);
        this.acceptor = new BrokerServerEndpoint(this);
        this.client = new ClientFacade(this);
        this.brokerStatus = new BrokerStatus(log);
        this.tasksHeap = tasksHeap;
        this.checkpointScheduler = new CheckpointScheduler(configuration, this);
    }

    public void start() {
        this.brokerStatus.recover();
        for (Task task : this.brokerStatus.getTasksAtBoot()) {
            switch (task.getStatus()) {
                case Task.STATUS_WAITING:
                    tasksHeap.insertTask(task.getTaskId(), task.getType(), task.getUserId());
                    break;
            }
        }
        this.workers.start(brokerStatus);
        started = true;
        this.checkpointScheduler.start();
    }

    public void stop() {
        this.checkpointScheduler.stop();
        this.workers.stop();
        this.brokerStatus.close();
        started = false;
    }

    @Override
    public void close() {

    }

    public BrokerServerEndpoint getAcceptor() {
        return acceptor;
    }

    public boolean isRunning() {
        return started;
    }

    public List<Long> assignTasksToWorker(int max, Map<Integer, Integer> availableSpace, List<Integer> groups, String workerId) throws LogNotAvailableException {
        List<Long> tasks = tasksHeap.takeTasks(max, groups, availableSpace);
        for (long taskId : tasks) {
            Task task = this.brokerStatus.getTask(taskId);
            if (task != null) {
                StatusEdit edit = StatusEdit.ASSIGN_TASK_TO_WORKER(taskId, workerId, task.getAttempts() + 1);
                this.brokerStatus.applyModification(edit);
            }
        }
        return tasks;
    }

    void checkpoint() throws LogNotAvailableException {
        this.brokerStatus.checkpoint();
    }

    public static interface ActionCallback {

        public void actionExecuted(StatusEdit action, ActionResult result);
    }

    public long addTask(
            int taskType,
            String userId,
            String parameter,
            int maxattempts) throws LogNotAvailableException {
        long taskId = brokerStatus.nextTaskId();
        StatusEdit addTask = StatusEdit.ADD_TASK(taskId, taskType, parameter, userId, maxattempts);
        this.brokerStatus.applyModification(addTask);
        this.tasksHeap.insertTask(taskId, taskType, userId);
        return taskId;
    }

    public void taskNeedsRecoveryDueToWorkerDeath(long taskId, String workerId) throws LogNotAvailableException {
        taskFinished(workerId, taskId, Task.STATUS_ERROR, "worker " + workerId + " died");
    }

    public void taskFinished(String workerId, long taskId, int finalstatus, String result) throws LogNotAvailableException {
        Task task = this.brokerStatus.getTask(taskId);
        if (task == null) {
            LOGGER.log(Level.SEVERE, "taskFinished {0}, task does not exist", taskId);
            return;
        }
        if (finalstatus == Task.STATUS_FINISHED) {
            StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, finalstatus, result);
            this.brokerStatus.applyModification(edit);
            return;
        }
        int maxAttepts = task.getMaxattempts();
        int attempt = task.getAttempts();
        if (maxAttepts > 0 && attempt >= maxAttepts) {
            // too many attempts
            LOGGER.log(Level.SEVERE, "taskFinished {0}, too many attempts {1}/{2}", new Object[]{taskId, attempt, maxAttepts});
            StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_ERROR, result);
            this.brokerStatus.applyModification(edit);
            return;
        }

        // submit for new execution
        LOGGER.log(Level.INFO, "taskFinished {0}, attempts {1}/{2}, scheduling for retry", new Object[]{taskId, attempt, maxAttepts});
        StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_WAITING, result);
        this.brokerStatus.applyModification(edit);
        this.tasksHeap.insertTask(taskId, task.getType(), task.getUserId());

    }

    public void workerConnected(String workerId, String processId, String nodeLocation, Set<Long> actualRunningTasks, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_CONNECTED(workerId, processId, nodeLocation, actualRunningTasks, timestamp);
        this.brokerStatus.applyModification(edit);
    }

    public void declareWorkerDisconnected(String workerId, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_DISCONNECTED(workerId, timestamp);
        this.brokerStatus.applyModification(edit);
    }

    public void declareWorkerDead(String workerId, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_DIED(workerId, timestamp);
        this.brokerStatus.applyModification(edit);
    }

}

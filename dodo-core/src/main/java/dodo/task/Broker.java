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
import dodo.clustering.TasksHeap;
import dodo.scheduler.Workers;
import dodo.worker.BrokerServerEndpoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Global status of the broker
 *
 * @author enrico.olivelli
 */
public class Broker {
    
    private final StatusChangesLog log;
    private final Workers workers;
    public final TasksHeap tasksHeap;
    private final BrokerStatus brokerStatus;
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

    public Broker(StatusChangesLog log, TasksHeap tasksHeap) {
        this.log = log;
        this.workers = new Workers(this);
        this.acceptor = new BrokerServerEndpoint(this);
        this.client = new ClientFacade(this);
        this.brokerStatus = new BrokerStatus(log);
        this.tasksHeap = tasksHeap;
    }

    public void start() {
        this.brokerStatus.recover();
        this.workers.start(brokerStatus);
        started = true;
    }

    public void stop() {
        this.workers.stop();
        started = false;
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
            StatusEdit edit = StatusEdit.ASSIGN_TASK_TO_WORKER(taskId, workerId);
            this.brokerStatus.applyModification(edit);
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
            String tenantInfo,
            String parameter) throws LogNotAvailableException {
        long taskId = brokerStatus.nextTaskId();
        StatusEdit addTask = StatusEdit.ADD_TASK(taskId, taskType, parameter, tenantInfo);
        this.brokerStatus.applyModification(addTask);
        this.tasksHeap.insertTask(taskId, taskType, tenantInfo);
        return taskId;
    }

    public void taskFinished(String workerId, long taskId, int finalstatus, String result) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.TASK_FINISHED(taskId, workerId, finalstatus, result);
        this.brokerStatus.applyModification(edit);
    }

    public void workerConnected(String workerId, String processId, String nodeLocation, Set<Long> actualRunningTasks, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_CONNECTED(workerId, processId, nodeLocation, actualRunningTasks, timestamp);
        this.brokerStatus.applyModification(edit);
    }

}

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

import majordodo.task.WorkerStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Snapshot of the status of the broker
 *
 * @author enrico.olivelli
 */
public class BrokerStatusSnapshot {

    List<Task> tasks = new ArrayList<>();
    List<WorkerStatus> workers = new ArrayList<>();
    long maxTaskId;
    long maxTransactionId;
    LogSequenceNumber actualLogSequenceNumber;

    public BrokerStatusSnapshot(long maxTaskId, long maxTransactionId, LogSequenceNumber actualLogSequenceNumber) {
        this.maxTaskId = maxTaskId;
        this.maxTransactionId = maxTransactionId;
        this.actualLogSequenceNumber = actualLogSequenceNumber;
    }

    public long getMaxTransactionId() {
        return maxTransactionId;
    }

    public void setMaxTransactionId(long maxTransactionId) {
        this.maxTransactionId = maxTransactionId;
    }

    public LogSequenceNumber getActualLogSequenceNumber() {
        return actualLogSequenceNumber;
    }

    public void setActualLogSequenceNumber(LogSequenceNumber actualLogSequenceNumber) {
        this.actualLogSequenceNumber = actualLogSequenceNumber;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public List<WorkerStatus> getWorkers() {
        return workers;
    }

    public void setWorkers(List<WorkerStatus> workers) {
        this.workers = workers;
    }

    public long getMaxTaskId() {
        return maxTaskId;
    }

    public void setMaxTaskId(long maxTaskId) {
        this.maxTaskId = maxTaskId;
    }

    public static BrokerStatusSnapshot deserializeSnapshot(Map<String, Object> snapshotdata) {
        long ledgerId = Long.parseLong(snapshotdata.get("ledgerid") + "");
        long sequenceNumber = Long.parseLong(snapshotdata.get("sequenceNumber") + "");

        long maxTaskId = Long.parseLong(snapshotdata.get("maxTaskId") + "");
        long maxTransactionId = Long.parseLong(snapshotdata.get("maxTransactionId") + "");
        BrokerStatusSnapshot result = new BrokerStatusSnapshot(maxTaskId, maxTransactionId, new LogSequenceNumber(ledgerId, sequenceNumber));
        List<Map<String, Object>> tasksStatus = (List<Map<String, Object>>) snapshotdata.get("tasks");
        if (tasksStatus != null) {
            tasksStatus.forEach(taskData -> {
                Task task = new Task();
                task.setTaskId(Long.parseLong(taskData.get("id") + ""));
                task.setStatus(Integer.parseInt(taskData.get("status") + ""));
                task.setMaxattempts(Integer.parseInt(taskData.get("maxattempts") + ""));
                String slot = (String) taskData.get("slot");
                task.setSlot(slot);
                task.setAttempts(Integer.parseInt(taskData.get("attempts") + ""));
                task.setParameter((String) taskData.get("parameter"));
                task.setResult((String) taskData.get("result"));
                task.setUserId((String) taskData.get("userId"));
                task.setCreatedTimestamp(Long.parseLong(taskData.get("createdTimestamp") + ""));
                task.setExecutionDeadline(Long.parseLong(taskData.get("executionDeadline") + ""));
                task.setType((String) taskData.get("type"));
                task.setWorkerId((String) taskData.get("workerId"));
                result.getTasks().add(task);
            });
        }
        List<Map<String, Object>> workersStatus = (List<Map<String, Object>>) snapshotdata.get("workers");
        if (workersStatus != null) {
            workersStatus.forEach(w -> {
                WorkerStatus workerStatus = new WorkerStatus();
                workerStatus.setWorkerId((String) w.get("workerId"));
                workerStatus.setWorkerLocation((String) w.get("location"));
                workerStatus.setProcessId((String) w.get("processId"));
                workerStatus.setLastConnectionTs(Long.parseLong(w.get("lastConnectionTs") + ""));
                workerStatus.setStatus(Integer.parseInt(w.get("status") + ""));
                result.getWorkers().add(workerStatus);
            });
        }
        return result;
    }

    public static Map<String, Object> serializeSnaphsot(LogSequenceNumber actualLogSequenceNumber, BrokerStatusSnapshot snapshotData) {
        Map<String, Object> filedata = new HashMap<>();
        filedata.put("ledgerid", actualLogSequenceNumber.ledgerId);
        filedata.put("sequenceNumber", actualLogSequenceNumber.sequenceNumber);
        filedata.put("maxTaskId", snapshotData.maxTaskId);
        filedata.put("maxTransactionId", snapshotData.maxTransactionId);
        List<Map<String, Object>> tasksStatus = new ArrayList<>();
        filedata.put("tasks", tasksStatus);
        List<Map<String, Object>> workersStatus = new ArrayList<>();
        filedata.put("workers", workersStatus);
        snapshotData.getWorkers().forEach(worker -> {
            Map<String, Object> workerData = new HashMap<>();
            workerData.put("workerId", worker.getWorkerId());
            workerData.put("location", worker.getWorkerLocation());
            workerData.put("processId", worker.getProcessId());
            workerData.put("lastConnectionTs", worker.getLastConnectionTs());
            workerData.put("status", worker.getStatus());
            workersStatus.add(workerData);
        });
        snapshotData.getTasks().forEach(
                task -> {
                    Map<String, Object> taskData = new HashMap<>();
                    taskData.put("id", task.getTaskId());
                    taskData.put("status", task.getStatus());
                    taskData.put("maxattempts", task.getMaxattempts());
                    if (task.getSlot() != null) {
                        taskData.put("slot", task.getSlot());
                    }
                    taskData.put("attempts", task.getAttempts());
                    taskData.put("executionDeadline", task.getExecutionDeadline());

                    taskData.put("parameter", task.getParameter());
                    taskData.put("result", task.getResult());
                    taskData.put("userId", task.getUserId());
                    taskData.put("createdTimestamp", task.getCreatedTimestamp());
                    taskData.put("type", task.getType());
                    taskData.put("workerId", task.getWorkerId());
                    tasksStatus.add(taskData);
                }
        );
        return filedata;
    }

}

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
package dodo.clustering;

import dodo.scheduler.WorkerStatus;
import dodo.task.TaskStatusView;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Replicated status of the broker. Each broker, leader or follower, contains a
 * copy of this status. The status is replicated to follower by reading the
 * StatusChangesLog
 *
 * @author enrico.olivelli
 */
public class BrokerStatus {

    private static final Logger LOGGER = Logger.getLogger(BrokerStatus.class.getName());

    public final Map<String, TaskQueue> queues = new HashMap<>();
    private final Map<Long, Task> tasks = new HashMap<>();
    private final Map<String, WorkerStatus> nodes = new HashMap<>();
    private final AtomicLong newTaskId = new AtomicLong();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final StatusChangesLog log;

    public WorkerStatus getWorkerStatus(String workerId) {
        return nodes.get(workerId);
    }

    public BrokerStatus(StatusChangesLog log) {
        this.log = log;
    }

    public static final class ModificationResult {

        public final LogSequenceNumber sequenceNumber;
        public final long newTaskId;

        public ModificationResult(LogSequenceNumber sequenceNumber, long newTaskId) {
            this.sequenceNumber = sequenceNumber;
            this.newTaskId = newTaskId;
        }

    }

    public ModificationResult applyModification(StatusEdit edit) throws LogNotAvailableException {
        LogSequenceNumber num = log.logStatusEdit(edit); // ? out of the lock ?
        return applyEdit(num, edit);
    }

    /**
     * Apply the modification to the status, this operation cannot fail, a
     * failure MUST lead to the death of the JVM because it will not be
     * recoverable as the broker will go out of synch
     *
     * @param edit
     */
    private ModificationResult applyEdit(LogSequenceNumber num, StatusEdit edit) {
        LOGGER.log(Level.FINE, "applyEdit {0}", edit);

        lock.writeLock().lock();
        try {
            switch (edit.editType) {
                case StatusEdit.TYPE_ASSIGN_TASK_TO_WORKER: {
                    long taskId = edit.taskId;
                    String workerId = edit.workerId;
                    Task task = tasks.get(taskId);
                    TaskQueue queue = queues.get(task.getQueueName());
                    Task taskFromQueue = queue.removeNext(taskId);
                    if (taskFromQueue != task) { // the object MUST be the same (reference comparison)
                        throw new IllegalStateException();
                    }
                    task.setStatus(Task.STATUS_RUNNING);
                    task.setWorkerId(workerId);
                    return new ModificationResult(num, -1);
                }
                case StatusEdit.TYPE_TASK_FINISHED: {
                    long taskId = edit.taskId;
                    String workerId = edit.workerId;
                    Task task = tasks.get(taskId);
                    if (task == null) {
                        throw new IllegalStateException();
                    }
                    if (!task.getWorkerId().equals(workerId)) {
                        throw new IllegalStateException("bad workerid " + workerId + ", expected " + task.getWorkerId());
                    }
                    task.setStatus(Task.STATUS_FINISHED);
                    return new ModificationResult(num, -1);
                }
                case StatusEdit.ACTION_TYPE_ADD_TASK: {
                    long newId = newTaskId.incrementAndGet();
                    Task task = new Task();
                    task.setTaskId(newId);
                    task.setCreatedTimestamp(System.currentTimeMillis());
                    task.setParameters(edit.taskParameter);
                    task.setType(edit.taskType);
                    task.setQueueName(edit.queueName);
                    task.setStatus(Task.STATUS_WAITING);
                    TaskQueue queue = queues.get(edit.queueName);
                    if (queue == null) {
                        String queueTag = edit.queueTag;
                        if (queueTag == null) {
                            queueTag = TaskQueue.DEFAULT_TAG;
                        }
                        queue = new TaskQueue(queueTag, edit.queueName);
                        queues.put(edit.queueName, queue);
                    }
                    tasks.put(newId, task);
                    queue.addNewTask(task);
                    return new ModificationResult(num, newId);
                }
                case StatusEdit.ACTION_TYPE_WORKER_CONNECTED: {
                    WorkerStatus node = nodes.get(edit.workerId);
                    if (node == null) {
                        node = new WorkerStatus();
                        node.setWorkerId(edit.workerId);

                        nodes.put(edit.workerId, node);
                    }
                    node.setStatus(WorkerStatus.STATUS_CONNECTED);
                    node.setWorkerLocation(edit.workerLocation);
                    node.setMaximumNumberOfTasks(edit.maximumNumberOfTasksPerTag);
                    node.setLastConnectionTs(edit.timestamp);
                    return new ModificationResult(num, -1);
                }
                default:
                    throw new IllegalArgumentException();

            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void reload() {

        lock.writeLock().lock();
        try {
            // TODO
        } finally {
            lock.writeLock().unlock();
        }

    }

    public TaskQueue getTaskQueue(String qname) {
        lock.readLock().lock();
        try {
            return queues.get(qname);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<TaskQueue> getTaskQueuesByTag(String tag) {
        lock.readLock().lock();
        try {
            List<TaskQueue> candidates = queues.values().stream().filter(q -> q.getTag().equalsIgnoreCase(tag)).collect(Collectors.toList());
            return candidates;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Task getTask(long taskId) {
        lock.readLock().lock();
        try {
            return tasks.get(taskId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public TaskStatusView getTaskStatus(long taskId) throws InterruptedException {
        Task task;
        lock.readLock().lock();
        try {
            task = tasks.get(taskId);
        } finally {
            lock.readLock().unlock();
        }
        if (task == null) {
            return null;
        }
        TaskStatusView s = new TaskStatusView();
        s.setCreatedTimestamp(task.getCreatedTimestamp());
        s.setQueueName(task.getQueueName());
        s.setWorkerId(task.getWorkerId());
        s.setStatus(task.getStatus());
        s.setTaskId(task.getTaskId());
        return s;
    }

}

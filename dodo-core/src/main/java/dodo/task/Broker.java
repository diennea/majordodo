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
import dodo.clustering.Action;
import dodo.clustering.ActionResult;
import dodo.clustering.CommitLog;
import dodo.clustering.LogSequenceNumber;
import dodo.scheduler.DefaultScheduler;
import dodo.scheduler.WorkerStatus;
import dodo.scheduler.Workers;
import dodo.scheduler.Scheduler;
import dodo.scheduler.WorkerManager;
import dodo.worker.BrokerServerEndpoint;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Global statu s of the organizer
 *
 * @author enrico.olivelli
 */
public class Broker {

    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());

    public final Map<String, TaskQueue> queues = new HashMap<>();
    private final Map<Long, Task> tasks = new HashMap<>();
    private final Map<String, WorkerStatus> nodes = new HashMap<>();
    private final CommitLog log;
    private final Workers nodeManagers;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong newTaskId = new AtomicLong();

    private final Scheduler scheduler;
    private final BrokerServerEndpoint acceptor;
    private final ClientFacade client;

    public ClientFacade getClient() {
        return client;
    }

    public Broker(CommitLog log) {
        this.log = log;
        this.scheduler = new DefaultScheduler(this);
        this.nodeManagers = new Workers(scheduler, this);
        this.acceptor = new BrokerServerEndpoint(this);
        this.client = new ClientFacade(this);
    }

    public BrokerServerEndpoint getAcceptor() {
        return acceptor;
    }

    public <T> T readonlyAccess(Callable<T> action) throws Exception {
        lock.readLock().lock();
        try {
            return action.call();
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

    private void validateAction(Action action) throws InvalidActionException {
        switch (action.actionType) {
            case Action.TYPE_ASSIGN_TASK_TO_WORKER: {
                long taskId = action.taskId;
                Task task = tasks.get(taskId);
                if (task == null) {
                    throw new InvalidActionException("task " + taskId + " does not exist");
                }
                if (task.getStatus() != Task.STATUS_WAITING) {
                    throw new InvalidActionException("task " + taskId + " is not in an assignable status, " + task.getStatus());
                }
                return;
            }
            case Action.TYPE_TASK_FINISHED: {
                long taskId = action.taskId;
                String workerId = action.workerId;
                Task task = tasks.get(taskId);
                if (task == null) {
                    throw new InvalidActionException("task " + taskId + " does not exist");
                }
                if (task.getStatus() != Task.STATUS_RUNNING) {
                    throw new InvalidActionException("task " + taskId + " is not in RUNNING status, " + task.getStatus());
                }
                if (task.getWorkerId() == null || !task.getWorkerId().equals(workerId)) {
                    throw new InvalidActionException("task " + taskId + " is not actually assigned to worker " + workerId);
                }
                return;
            }
            case Action.ACTION_TYPE_ADD_TASK: {
                return;
            }
            case Action.ACTION_TYPE_WORKER_REGISTERED: {
                WorkerStatus node = nodes.get(action.workerId);
                if (node != null) {
                    throw new InvalidActionException("not " + action.workerId + " already registered");
                }
                return;
            }
            default:
                throw new InvalidActionException("Action type unknown " + action.actionType);

        }
    }

    public WorkerManager getWorkerManager(String workerId) {
        lock.readLock().lock();
        try {
            return this.nodeManagers.getNodeManager(nodes.get(workerId));
        } finally {
            lock.readLock().unlock();
        }
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public static interface ActionCallback {

        public void actionExecuted(Action action, ActionResult result);
    }

    public void executeAction(final Action action, final ActionCallback callback) throws InterruptedException, InvalidActionException {
        System.out.println("[BROKER] executeAction " + action);
        LOGGER.log(Level.FINE, "executeAction {0}", action);

        lock.readLock().lock();
        try {
            validateAction(action);
        } finally {
            lock.readLock().unlock();
        }

        log.logAction(action, new CommitLog.ActionLogCallback() {

            @Override
            public void actionCommitted(LogSequenceNumber number, Throwable error) {

                if (error != null) {
                    callback.actionExecuted(action, ActionResult.ERROR(error));
                    return;
                }
                lock.writeLock().lock();
                try {
                    ActionResult result = applyAction(action);
                    LOGGER.log(Level.FINE, "executeAction {0} -> result {1}", new Object[]{action, result});
                    callback.actionExecuted(action, result);
                } catch (Throwable t) {
                    callback.actionExecuted(action, ActionResult.ERROR(t));
                } finally {
                    lock.writeLock().unlock();
                }
            }
        });

    }

    private ActionResult applyAction(Action action) {
        LOGGER.log(Level.FINE, "applyAction {0}", action);
        switch (action.actionType) {
            case Action.TYPE_ASSIGN_TASK_TO_WORKER: {
                long taskId = action.taskId;
                String workerId = action.workerId;
                Task task = tasks.get(taskId);
                task.setStatus(Task.STATUS_RUNNING);
                task.setWorkerId(workerId);
                TaskQueue queue = queues.get(task.getQueueName());
                queue.removeNext(taskId);
                WorkerStatus node = nodes.get(workerId);
                nodeManagers.getNodeManager(node).taskAssigned(task);
                return ActionResult.TASKID(taskId);
            }
            case Action.TYPE_TASK_FINISHED: {
                long taskId = action.taskId;
                String workerId = action.workerId;
                Task task = tasks.get(taskId);
                task.setStatus(Task.STATUS_FINISHED);
                task.setWorkerId(workerId);
                TaskQueue queue = queues.get(task.getQueueName());
                scheduler.taskFinished(workerId, queue.getTag());
                return ActionResult.TASKID(taskId);
            }
            case Action.ACTION_TYPE_ADD_TASK: {
                long newId = newTaskId.incrementAndGet();
                Task task = new Task();
                task.setTaskId(newId);
                task.setCreatedTimestamp(System.currentTimeMillis());
                task.setParameters(action.taskParameter);
                task.setType(action.taskType);
                task.setQueueName(action.queueName);
                task.setStatus(Task.STATUS_WAITING);
                TaskQueue queue = queues.get(action.queueName);
                if (queue == null) {
                    String queueTag = action.queueTag;
                    if (queueTag == null) {
                        queueTag = TaskQueue.DEFAULT_TAG;
                    }
                    queue = new TaskQueue(queueTag);
                    queues.put(action.queueName, queue);
                }
                tasks.put(newId, task);
                queue.addNewTask(task);
                scheduler.taskSubmitted(action.queueTag);
                return ActionResult.TASKID(newId);
            }
            case Action.ACTION_TYPE_WORKER_REGISTERED: {
                WorkerStatus node = nodes.get(action.workerId);
                if (node == null) {
                    node = new WorkerStatus();
                    node.setWorkerId(action.workerId);
                    node.setStatus(WorkerStatus.STATUS_ALIVE);
                    node.setWorkerLocation(action.workerLocation);
                    node.setMaximumNumberOfTasks(action.maximumNumberOfTasksPerTag);
                    nodes.put(action.workerId, node);
                    return ActionResult.OK();
                } else {
                    throw new IllegalStateException();
                }
            }
            default:
                return ActionResult.ERROR(new IllegalStateException("invalid action type " + action.actionType).fillInStackTrace());

        }
    }

    public String getTagForTask(long taskid) {
        Task task;
        lock.readLock().lock();
        try {
            task = tasks.get(taskid);
            if (task == null) {
                return null;
            }
            TaskQueue q = queues.get(task.getQueueName());
            if (q == null) {
                return null;
            }
            return q.getTag();
        } finally {
            lock.readLock().unlock();
        }

    }

}

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

import dodo.clustering.Action;
import dodo.clustering.ActionResult;
import dodo.clustering.CommitLog;
import dodo.clustering.LogNotAvailableException;
import dodo.scheduler.DefaultScheduler;
import dodo.scheduler.Node;
import dodo.scheduler.Nodes;
import dodo.scheduler.Scheduler;
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
public class Organizer {

    private static final Logger LOGGER = Logger.getLogger(Organizer.class.getName());

    public final Map<String, TaskQueue> queues = new HashMap<>();
    private final Map<Long, Task> tasks = new HashMap<>();
    private final Map<String, Node> nodes = new HashMap<>();
    private final CommitLog log;
    private final Nodes nodeManagers;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong newTaskId = new AtomicLong();

    public Organizer(CommitLog log) {
        this.log = log;
        Scheduler sched = new DefaultScheduler(this);
        this.nodeManagers = new Nodes(sched);
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
        s.setStatus(task.getStatus());
        s.setTaskId(task.getTaskId());
        return s;
    }

    public ActionResult executeAction(Action action) throws LogNotAvailableException, InterruptedException {
        LOGGER.log(Level.FINE, "executeAction {0}", action);
        lock.writeLock().lock();
        try {
            log.beforeAction(action);
            switch (action.actionType) {
                case Action.ASSIGN_TASK_TO_NODE: {
                    long taskId = action.taskId;
                    String nodeId = action.nodeId;
                    Task task = tasks.get(taskId);
                    task.setStatus(Task.STATUS_RUNNING);
                    TaskQueue queue = queues.get(task.getQueueName());
                    queue.removeNext(taskId);
                    Node node = nodes.get(nodeId);
                    nodeManagers.getNodeManager(node).taskAssigned(task);
                    return new ActionResult(taskId);
                }
                case Action.ACTION_TYPE_ADD_TASK: {
                    long newId = newTaskId.incrementAndGet();
                    Task task = new Task();
                    task.setTaskId(newId);
                    task.setCreatedTimestamp(System.currentTimeMillis());
                    task.setParameter(action.taskParameter);
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
                    return new ActionResult(newId);
                }
                case Action.ACTION_TYPE_NODE_REGISTERED: {
                    Node node = nodes.get(action.nodeId);
                    if (node == null) {
                        node = new Node();
                        node.setNodeId(action.nodeId);
                        node.setStatus(Node.STATUS_ALIVE);
                        node.setNodeLocation(action.nodeLocation);
                        node.setTags(action.nodeTags);
                        node.setMaximumNumberOfTasks(action.maximumNumberOfTasksPerTag);
                        nodes.put(action.nodeId, node);
                        nodeManagers.getNodeManager(node).nodeRegistered();
                    } else {
                        throw new IllegalStateException();
                    }
                    return new ActionResult(0);
                }
                default:
                    // 
                    return new ActionResult(0);

            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}

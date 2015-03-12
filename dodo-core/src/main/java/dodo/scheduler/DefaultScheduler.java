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
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import dodo.task.Task;
import dodo.task.TaskQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default simple scheduler
 *
 * @author enrico.olivelli
 */
public class DefaultScheduler extends Scheduler {

    private static final Logger LOGGER = Logger.getLogger(DefaultScheduler.class.getName());

    Broker broker;

    ConcurrentHashMap<String, List<PendingWorker>> pendingWorkersByTag = new ConcurrentHashMap<>();

    private static final class PendingWorker {

        String workerId;
        String tag;

        public PendingWorker(String workerId, String tag) {
            this.workerId = workerId;
            this.tag = tag;
        }

    }

    public DefaultScheduler(Broker organizer) {
        this.broker = organizer;
    }

    @Override
    public void taskSubmitted(String tag) {

        List<PendingWorker> workers = pendingWorkersByTag.get(tag);
        LOGGER.log(Level.FINE, "taskSubmitted {0}, pending workers: {1}", new Object[]{tag, workers});
        if (workers != null && !workers.isEmpty()) {
            PendingWorker w = workers.remove(0);
            taskFinished(w.workerId, tag);
        }
    }

    @Override
    public void workerConnected(String workerId, Map<String, Integer> maximumNumberOfTasksPerTag, Set<Long> actualRunningTasks) {
        maximumNumberOfTasksPerTag.forEach((tag, max) -> {
            for (int i = 0; i < max; i++) {
                taskFinished(workerId, tag);
            }
        });
    }

    @Override
    public void taskFinished(String workerId, String tag) {
        Task task = getNewTask(tag);
        boolean done = false;
        if (task != null) {
            task = estractTaskFromQueueTask(task.getTaskId(), task.getQueueName());
            if (task != null) {
                Action action = Action.ASSIGN_TASK_TO_WORKER(task.getTaskId(), workerId);
                try {
                    broker.executeAction(action, (a, result) -> {
                        if (result.error != null) {
                            LOGGER.log(Level.SEVERE, "cannot assign new task", result.error);
                            return;
                        }
                        if (!done) {
                            List<PendingWorker> workers = pendingWorkersByTag.get(tag);
                            if (workers == null) {
                                workers = new ArrayList<>();
                                pendingWorkersByTag.put(tag, workers);
                            }
                            workers.add(new PendingWorker(workerId, tag));
                        }
                    });
                } catch (InterruptedException | InvalidActionException nothingToDo) {
                    LOGGER.log(Level.SEVERE, "fatal error", nothingToDo);
                }
            }

        }

    }

    private Task getNewTask(String tag) {
        LOGGER.log(Level.INFO, "getNewTask tag={0}", tag);
        try {
            return broker.readonlyAccess(() -> {
                for (TaskQueue q : broker.queues.values()) {
                    if (q.getTag().equals(tag)) {
                        Task peek = q.peekNext();
                        if (peek != null) {
                            return peek;
                        }
                    }
                }
                return null;
            });
        } catch (Exception err) {
            LOGGER.log(Level.SEVERE, "cannot get a new task", err);
            return null;
        }
    }

    private Task estractTaskFromQueueTask(long taskid, String queue) {
        LOGGER.log(Level.INFO, "estractTaskFromQueueTask taskid={0}", taskid);
        try {
            return broker.writeAccess(() -> {
                return broker.queues.get(queue).removeNext(taskid);
            });
        } catch (Exception err) {
            LOGGER.log(Level.SEVERE, "cannot get a new task", err);
            return null;
        }
    }

}

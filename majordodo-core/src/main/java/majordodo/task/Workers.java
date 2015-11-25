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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles NodeManagers
 *
 * @author enrico.olivelli
 */
public class Workers {

    private static final Logger LOGGER = Logger.getLogger(Workers.class.getName());

    private final Map<String, WorkerManager> nodeManagers = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Broker broker;
    private final Thread workersActivityThread;
    private volatile boolean stop;
    private final ExecutorService workersThreadpool;

    private final Object waitForEvent = new Object();

    public Workers(Broker broker) {
        this.broker = broker;
        this.workersActivityThread = new Thread(new Life(), "workers-life");
        this.workersThreadpool = Executors.newFixedThreadPool(broker.getConfiguration().getWorkersThreadpoolSize(), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "workers-life-thread");
            }
        });
    }

    public void start(BrokerStatus statusAtBoot, Map<String, Collection<Long>> deadWorkerTasks, List<String> connectedAtBoot) {
        Collection<WorkerStatus> workersAtBoot = statusAtBoot.getWorkersAtBoot();
        Collection<Task> tasksAtBoot = statusAtBoot.getTasksAtBoot();
        for (WorkerStatus workerStatus : workersAtBoot) {
            String workerId = workerStatus.getWorkerId();
            WorkerManager manager = getWorkerManager(workerId);
            if (workerStatus.getStatus() == WorkerStatus.STATUS_CONNECTED) {
                connectedAtBoot.add(workerId);
            }
            Set<Long> toRecoverForWorker = new HashSet<>();
            deadWorkerTasks.put(workerId, toRecoverForWorker);
            LOGGER.log(Level.SEVERE, "Booting workerManager for workerId:{0}, actual status: {1} {2}", new Object[]{workerStatus.getWorkerId(), workerStatus.getStatus(), WorkerStatus.statusToString(workerStatus.getStatus())});
            for (Task task : tasksAtBoot) {
                if (workerId.equals(task.getWorkerId())) {
                    if (task.getStatus() == Task.STATUS_RUNNING) {
                        if (workerStatus.getStatus() == WorkerStatus.STATUS_DEAD) {
                            LOGGER.log(Level.FINE, "workerId:{0} should be running task {1}, but worker is DEAD", new Object[]{workerStatus.getWorkerId(), task.getTaskId()});
                            toRecoverForWorker.add(task.getTaskId());
                        } else {
                            LOGGER.log(Level.FINE, "Booting workerId:" + workerStatus.getWorkerId() + " should be running task " + task.getTaskId());
                            manager.taskShouldBeRunning(task.getTaskId());
                        }
                    } else {
                        LOGGER.log(Level.SEVERE, "workerId:" + workerStatus.getWorkerId() + " task " + task.getTaskId() + " is assigned to worker, but in status " + Task.statusToString(task.getStatus()));                        
                    }
                }
            }
        }
        workersActivityThread.start();
    }

    public void stop() {
        stop = true;
        wakeUp();
        try {
            workersActivityThread.join();
        } catch (InterruptedException exit) {
        }
        workersThreadpool.shutdown();
    }

    private class Life implements Runnable {

        @Override
        public void run() {
            try {
                while (!stop) {
                    synchronized (waitForEvent) {
                        waitForEvent.wait(500);
                    }
                    Collection<WorkerManager> managers;
                    lock.readLock().lock();
                    try {
                        managers = new ArrayList<>(nodeManagers.values());
                    } finally {
                        lock.readLock().unlock();
                    }
                    for (WorkerManager man : managers) {
                        if (!man.isThreadAssigned()) {
                            man.threadAssigned();
                            try {
                                workersThreadpool.submit(man.operation());
                            } catch (RejectedExecutionException rejected) {
                                LOGGER.log(Level.SEVERE, "workers manager rejected task", rejected);
                            }
                        }
                    }
                }
            } catch (Throwable exit) {
                // exiting loop                
                LOGGER.log(Level.SEVERE, "workers manager is dead", exit);
                broker.brokerFailed();
            }
        }
    }

    public void wakeUp() {
        synchronized (waitForEvent) {
            waitForEvent.notify();
        }
    }

    public WorkerManager getWorkerManager(String id) {
        WorkerManager man;
        lock.readLock().lock();
        try {
            man = nodeManagers.get(id);
        } finally {
            lock.readLock().unlock();
        }
        if (man == null) {
            lock.writeLock().lock();
            try {
                man = nodeManagers.get(id);
                if (man == null) {
                    man = new WorkerManager(id, broker);
                    nodeManagers.put(id, man);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return man;
    }

}

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

import dodo.task.Broker;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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

    private final Object waitForEvent = new Object();

    public Workers(Broker broker) {
        this.broker = broker;
        this.workersActivityThread = new Thread(new Life(), "workers-life");
    }

    public void start(BrokerStatus statusAtBoot) {
        Collection<WorkerStatus> workersAtBoot = statusAtBoot.getWorkersAtBoot();
        Collection<Task> tasksAtBoot = statusAtBoot.getTasksAtBoot();
        for (WorkerStatus status : workersAtBoot) {
            String workerId = status.getWorkerId();
            WorkerManager manager = getWorkerManager(workerId);
            LOGGER.log(Level.INFO, "Booting workerManager for workerId:" + status.getWorkerId());
            for (Task task : tasksAtBoot) {
                if (workerId.equals(task.getWorkerId())) {
                    LOGGER.log(Level.INFO, "Booting workerId:" + status.getWorkerId() + " should be running task " + task.getTaskId());
                    manager.taskShouldBeRunning(task.getTaskId());
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
            exit.printStackTrace();
        }
    }

    public void wakeUpOnTaskAssigned(String workerId, long taskId) {
        getWorkerManager(workerId).taskAssigned(taskId);
        wakeUp();
    }

    private class Life implements Runnable {

        @Override
        public void run() {
            try {
                while (!stop) {
                    synchronized (waitForEvent) {
                        waitForEvent.wait(500);
                    }
                    nodeManagers.values().stream().forEach((man) -> {
                        man.wakeUp();
                    });
                }
            } catch (Throwable exit) {
                // exiting loop
                exit.printStackTrace();
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

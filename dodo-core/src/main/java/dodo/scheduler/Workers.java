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

import dodo.task.Broker;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Handles NodeManagers
 *
 * @author enrico.olivelli
 */
public class Workers {

    private final Map<String, WorkerManager> nodeManagers = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Scheduler scheduler;
    private final Broker broker;

    public Workers(Scheduler scheduler, Broker broker) {
        this.scheduler = scheduler;
        this.broker = broker;
    }

    public WorkerManager getNodeManager(WorkerStatus node) {
        String id = node.getWorkerId();
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
                    man = new WorkerManager(node, scheduler,broker);
                    nodeManagers.put(id, man);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return man;
    }
}

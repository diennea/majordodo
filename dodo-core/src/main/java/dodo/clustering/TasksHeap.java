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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Heap of tasks to be executed. Tasks are not arranged in a queue but in an
 * heap.<br>
 * Important cases:<br>
 * <ul>
 * <li>A worker needs a task to be executed
 * <li>A clients submits a new task
 * <li>Compaction of the heap (removes empty slots)
 * <li>Readonly access for monitoring
 * </ul>
 *
 * @author enrico.olivelli
 */
public class TasksHeap {

    private TaskEntry[] actuallist;

    private int actualsize;
    private int fragmentation;
    private int maxFragmentation;

    private final int size;
    private final TenantMapperFunction tenantAssigner;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public int getActualsize() {
        return actualsize;
    }

    public int getFragmentation() {
        return fragmentation;
    }

    public int getSize() {
        return size;
    }

    public TasksHeap(int size, TenantMapperFunction tenantAssigner) {
        this.size = size;
        this.tenantAssigner = tenantAssigner;
        this.actuallist = new TaskEntry[size];
        this.maxFragmentation = size / 4;
    }

    public boolean insertTask(long taskid, int tasktype, String assignerData) {
        lock.writeLock().lock();
        try {
            if (actualsize == size) {
                return false;
            }
            this.actuallist[actualsize++] = new TaskEntry(taskid, tasktype, assignerData);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static final class TaskEntry {

        private final long taskid;
        private final int tasktype;
        private final String assignerData;

        TaskEntry(long taskid, int tasktype, String assignerData) {
            this.taskid = taskid;
            this.tasktype = tasktype;
            this.assignerData = assignerData;
        }

    }

    public void runCompaction() {
        TaskEntry[] newlist = new TaskEntry[size];
        int pos = 0;
        lock.writeLock().lock();
        try {
            for (int i = 0; i < actualsize; i++) {
                TaskEntry entry = actuallist[i];
                if (entry != null) {
                    newlist[pos++] = entry;
                }
            }
            actuallist = newlist;
            actualsize = pos;
            fragmentation = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long takeTask(int tenant, int tasktype) {
        while (true) {
            int pos = -1;
            lock.readLock().lock();
            try {
                for (int i = 0; i < actualsize; i++) {
                    TaskEntry entry = this.actuallist[i];
                    if (entry != null && entry.tasktype == tasktype) {
                        if (tenantAssigner.getActualTenant(entry.taskid, entry.assignerData) == tenant) {
                            pos = i;
                            break;
                        }
                    }
                }
                if (pos < 0) {
                    return -1;
                }
            } finally {
                lock.readLock().unlock();
            }

            lock.writeLock().lock();
            try {
                TaskEntry entry = this.actuallist[pos];
                if (entry != null) {
                    this.actuallist[pos] = null;
                    this.fragmentation++;
                    if (this.fragmentation > maxFragmentation) {
                        runCompaction();
                    }
                    return entry.taskid;
                }
                // try again
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

}

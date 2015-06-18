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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

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
    private int minValidPosition;

    private final int size;
    private final GroupMapperFunction groupMapper;
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

    public TasksHeap(int size, GroupMapperFunction tenantAssigner) {
        this.size = size;
        this.groupMapper = tenantAssigner;
        this.actuallist = new TaskEntry[size];
        for (int i = 0; i < size; i++) {
            this.actuallist[i] = new TaskEntry(0, 0, null);
        }
        this.maxFragmentation = size / 4;
    }

    public int getMaxFragmentation() {
        return maxFragmentation;
    }

    public void setMaxFragmentation(int maxFragmentation) {
        this.maxFragmentation = maxFragmentation;
    }

    public boolean insertTask(long taskid, int tasktype, String userid) {
        lock.writeLock().lock();
        try {
            if (actualsize == size) {
                return false;
            }
            TaskEntry entry = this.actuallist[actualsize++];
            entry.taskid = taskid;
            entry.tasktype = tasktype;
            entry.userid = userid;
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static final class TaskEntry {

        public long taskid;
        public int tasktype;
        public String userid;

        TaskEntry(long taskid, int tasktype, String userid) {
            this.taskid = taskid;
            this.tasktype = tasktype;
            this.userid = userid;
        }

        @Override
        public String toString() {
            return "TaskEntry{" + "taskid=" + taskid + ", tasktype=" + tasktype + ", userid=" + userid + '}';
        }

    }

    public void scan(Consumer<TaskEntry> consumer) {
        Stream.of(actuallist).forEach(consumer);
    }

    public void runCompaction() {
        lock.writeLock().lock();
        try {
            int[] nonemptypositions = new int[size];
            int insertpos = 0;
            int pos = 0;
            for (TaskEntry entry : actuallist) {
                if (entry.taskid > 0) {
                    nonemptypositions[insertpos++] = pos + 1; // NOTE_A: 0 means "empty", so we are going to add "+1" to every position
                }
                pos++;
            }
            int writepos = 0;
            for (int nonemptyindex = 0; nonemptyindex < size; nonemptyindex++) {
                int nextnotempty = nonemptypositions[nonemptyindex];
                if (nextnotempty == 0) {
                    break;
                }
                nextnotempty = nextnotempty - 1; // see NOTE_A
                actuallist[writepos].taskid = actuallist[nextnotempty].taskid;
                actuallist[writepos].tasktype = actuallist[nextnotempty].tasktype;
                actuallist[writepos].userid = actuallist[nextnotempty].userid;
                writepos++;
            }
            for (int j = writepos; j < size; j++) {
                actuallist[j].taskid = 0;
                actuallist[j].tasktype = 0;
                actuallist[j].userid = null;
            }

            minValidPosition = 0;
            actualsize = pos;
            fragmentation = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Long> takeTasks(int max, List<Integer> groups, Map<Integer, Integer> availableSpace) {        
        while (true) {            
            TasksChooser chooser = new TasksChooser(groups, availableSpace, max);
            lock.readLock().lock();
            try {
                for (int i = minValidPosition; i < actualsize; i++) {
                    TaskEntry entry = this.actuallist[i];
                    if (entry.taskid > 0) {
                        int group = groupMapper.getGroup(entry.taskid, entry.tasktype, entry.userid);
                        if (chooser.accept(i, entry, group)) {
                            break;
                        }
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            List<TasksChooser.Entry> choosen = chooser.getChoosenTasks();
            if (choosen.isEmpty()) {
                return Collections.emptyList();
            }

            List<Long> result = new ArrayList<>();
            lock.writeLock().lock();
            try {
                for (TasksChooser.Entry choosenentry : choosen) {
                    int pos = choosenentry.position;
                    TaskEntry entry = this.actuallist[pos];
                    if (entry.taskid == choosenentry.taskid) {
                        entry.taskid = 0;
                        entry.tasktype = 0;
                        entry.userid = null;
                        this.fragmentation++;
                        result.add(choosenentry.taskid);
                        if (pos == minValidPosition) {
                            minValidPosition++;
                        }
                    }
                }
                if (this.fragmentation > maxFragmentation) {
                    runCompaction();
                }
            } finally {
                lock.writeLock().unlock();
            }
            return result;
        }
    }

}

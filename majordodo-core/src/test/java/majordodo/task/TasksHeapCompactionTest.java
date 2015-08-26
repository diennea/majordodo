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

import majordodo.task.Task;
import majordodo.task.TasksHeap;
import majordodo.task.GroupMapperFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TasksHeapCompactionTest {

    private static final String TASKTYPE_MYTASK1 = "MYTASK1";
    private static final String TASKTYPE_MYTASK2 = "MYTASK2";
    private static final String USERID1 = "myuser1";
    private static final String USERID2 = "myuser2";
    private static final int GROUPID1 = 9713;
    private static final int GROUPID2 = 972;

    private GroupMapperFunction DEFAULT_FUNCTION = new GroupMapperFunction() {

        @Override
        public int getGroup(long taskid, String tasktype, String assignerData) {
            switch (assignerData) {
                case USERID1:
                    return GROUPID1;
                case USERID2:
                    return GROUPID2;
                default:
                    return -1;
            }
        }
    };

    @Test
    public void testCompation1() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        instance.setMaxFragmentation(1000000);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK1, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK2, USERID1);

        List<TasksHeap.TaskEntry> entries = new ArrayList<>();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });
        assertEquals(task1, entries.get(0).taskid);
        assertEquals(task2, entries.get(1).taskid);

        List<Long> taskids = instance.takeTasks(1, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace);
        assertEquals(1, taskids.size());
        assertEquals(task1, taskids.get(0).longValue());

        System.out.println("after take first");
        entries.clear();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });

        assertEquals(0, entries.get(0).taskid);
        assertEquals(task2, entries.get(1).taskid);

        System.out.println("compaction...");
        instance.runCompaction();
        entries.clear();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });

        assertEquals(task2, entries.get(0).taskid);
        assertEquals(0, entries.get(1).taskid);

    }

    @Test
    public void testCompation2() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        instance.setMaxFragmentation(1000000);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 100);
        AtomicLong newTaskId = new AtomicLong(0);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        long task3 = newTaskId.incrementAndGet();
        long task4 = newTaskId.incrementAndGet();
        long task5 = newTaskId.incrementAndGet();
        long task6 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK2, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK1, USERID1); // will be removed
        instance.insertTask(task3, TASKTYPE_MYTASK2, USERID1);
        instance.insertTask(task4, TASKTYPE_MYTASK1, USERID1); // will be removed
        instance.insertTask(task5, TASKTYPE_MYTASK1, USERID1); // will be removed
        instance.insertTask(task6, TASKTYPE_MYTASK2, USERID1);

        List<TasksHeap.TaskEntry> entries = new ArrayList<>();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });
        assertEquals(task1, entries.get(0).taskid);
        assertEquals(task2, entries.get(1).taskid);
        assertEquals(task3, entries.get(2).taskid);
        assertEquals(task4, entries.get(3).taskid);
        assertEquals(task5, entries.get(4).taskid);
        assertEquals(task6, entries.get(5).taskid);

        List<Long> taskids = instance.takeTasks(20, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace);
        assertEquals(3, taskids.size());
        assertEquals(task2, taskids.get(0).longValue());
        assertEquals(task4, taskids.get(1).longValue());
        assertEquals(task5, taskids.get(2).longValue());

        System.out.println("after take first");
        entries.clear();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });

        assertEquals(task1, entries.get(0).taskid);
        assertEquals(0, entries.get(1).taskid);
        assertEquals(task3, entries.get(2).taskid);
        assertEquals(0, entries.get(3).taskid);
        assertEquals(0, entries.get(4).taskid);
        assertEquals(task6, entries.get(5).taskid);

        System.out.println("compaction...");
        instance.runCompaction();
        entries.clear();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });

        assertEquals(task1, entries.get(0).taskid);
        assertEquals(task3, entries.get(1).taskid);
        assertEquals(task6, entries.get(2).taskid);
        assertEquals(0, entries.get(3).taskid);
        assertEquals(0, entries.get(4).taskid);
        assertEquals(0, entries.get(5).taskid);

    }

    @Test
    public void testCompation3() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        long task3 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK1, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK2, USERID1);

        List<TasksHeap.TaskEntry> entries = new ArrayList<>();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });
        assertEquals(task1, entries.get(0).taskid);
        assertEquals(task2, entries.get(1).taskid);

        List<Long> taskids = instance.takeTasks(1, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace);
        assertEquals(1, taskids.size());
        assertEquals(task1, taskids.get(0).longValue());

        System.out.println("after take first");
        entries.clear();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });

        assertEquals(0, entries.get(0).taskid);
        assertEquals(task2, entries.get(1).taskid);

        System.out.println("compaction...");
        instance.runCompaction();
        entries.clear();
        instance.scanFull(entry -> {
            System.out.println("entry:" + entry);
            entries.add(entry);
        });

        assertEquals(task2, entries.get(0).taskid);
        assertEquals(0, entries.get(1).taskid);

        instance.insertTask(task3, TASKTYPE_MYTASK2, USERID1);
        entries.clear();
        instance.scan(entries::add);
        assertEquals(2,entries.size());

    }

}

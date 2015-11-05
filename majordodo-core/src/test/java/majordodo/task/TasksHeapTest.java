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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TasksHeapTest {

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
    public void test1() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        List<Long> taskids = instance.takeTasks(1, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace);
        assertEquals(1, taskids.size());
        assertEquals(newTaskId.get(), taskids.get(0).longValue());
    }

    @Test
    public void test2() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        List<Long> taskids = instance.takeTasks(1, Arrays.asList(Task.GROUP_ANY), Collections.emptySet(), availableSpace);
        assertEquals(1, taskids.size());
        assertEquals(newTaskId.get(), taskids.get(0).longValue());
    }

    @Test
    public void test3() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(Task.TASKTYPE_ANY, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        List<Long> taskids = instance.takeTasks(1, Arrays.asList(GROUPID1), Collections.emptySet(), availableSpace);
        assertEquals(1, taskids.size());
        assertEquals(newTaskId.get(), taskids.get(0).longValue());
    }

    @Test
    public void test4() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        instance.insertTask(newTaskId.incrementAndGet(), TASKTYPE_MYTASK1, USERID1);
        List<Long> taskids = instance.takeTasks(1, Arrays.asList(GROUPID1), Collections.emptySet(), availableSpace);
        assertEquals(1, taskids.size());
        assertEquals(newTaskId.get(), taskids.get(0).longValue());
    }

    @Test
    public void test5() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 1);
        availableSpace.put(TASKTYPE_MYTASK2, 1);
        AtomicLong newTaskId = new AtomicLong(987);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK1, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK2, USERID1);

        {
            List<Long> taskids = instance.takeTasks(2, Arrays.asList(GROUPID1), Collections.emptySet(), availableSpace);
            assertEquals(2, taskids.size());
            assertEquals(task1, taskids.get(0).longValue());
            assertEquals(task2, taskids.get(1).longValue());
        }

    }

    @Test
    public void test6() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 3);
        availableSpace.put(TASKTYPE_MYTASK2, 1);
        AtomicLong newTaskId = new AtomicLong(0);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        long task3 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK1, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK2, USERID1);
        instance.insertTask(task3, TASKTYPE_MYTASK2, USERID1);

        {
            List<Long> taskids = instance.takeTasks(2, Arrays.asList(GROUPID1), Collections.emptySet(), availableSpace);
            assertEquals(2, taskids.size());
            assertEquals(task1, taskids.get(0).longValue());
            assertEquals(task2, taskids.get(1).longValue());
        }

        {
            List<Long> taskids = instance.takeTasks(2, Arrays.asList(GROUPID1), Collections.emptySet(), availableSpace);
            assertEquals(1, taskids.size());
            assertEquals(task3, taskids.get(0).longValue());
        }

    }

    @Test
    public void testExcelude() throws Exception {
        TasksHeap instance = new TasksHeap(10, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 3);
        availableSpace.put(TASKTYPE_MYTASK2, 1);
        AtomicLong newTaskId = new AtomicLong(0);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        long task3 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK1, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK2, USERID1);
        instance.insertTask(task3, TASKTYPE_MYTASK2, USERID2);

        {
            List<Long> taskids = instance.takeTasks(3, Arrays.asList(Task.GROUP_ANY), new HashSet<>(Arrays.asList(GROUPID1)), availableSpace);
            assertEquals(1, taskids.size());
            assertEquals(task3, taskids.get(0).longValue());
        }

    }

    @Test
    public void testAutoGrow() throws Exception {
        TasksHeap instance = new TasksHeap(1, DEFAULT_FUNCTION);
        Map< String, Integer> availableSpace = new HashMap<>();
        availableSpace.put(TASKTYPE_MYTASK1, 3);
        availableSpace.put(TASKTYPE_MYTASK2, 1);
        AtomicLong newTaskId = new AtomicLong(0);
        long task1 = newTaskId.incrementAndGet();
        long task2 = newTaskId.incrementAndGet();
        long task3 = newTaskId.incrementAndGet();
        instance.insertTask(task1, TASKTYPE_MYTASK1, USERID1);
        instance.insertTask(task2, TASKTYPE_MYTASK2, USERID1);
        instance.insertTask(task3, TASKTYPE_MYTASK2, USERID2);

        {
            List<Long> taskids = instance.takeTasks(3, Arrays.asList(Task.GROUP_ANY), new HashSet<>(Arrays.asList(GROUPID1)), availableSpace);
            assertEquals(1, taskids.size());
            assertEquals(task3, taskids.get(0).longValue());
        }

    }

}

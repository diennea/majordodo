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

import dodo.client.TaskStatusView;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class RecoverAtBootTest extends BasicBrokerEnv {

    static final String taskType1 = "type1";
    static final String taskType2 = "type2";
    static final String userid1 = "testuser1";
    static final String worker1 = "worker1";
    static final String worker1processid = "pid213";
    static final long taskId1 = 123;
    static final long taskId2 = 124;
    static final long taskId3 = 125;
    static final long taskId4 = 126;
    static final long sequenceNumber_snapshot = 826;
    static final long sequenceNumber1 = 826; // older than snapshot
    static final long sequenceNumber2 = 827;
    static final long sequenceNumber3 = 828;
    static final long sequenceNumber4 = 829;
    static final long sequenceNumber5 = 830;
    static final long sequenceNumber6 = 831;
    static final long sequenceNumber7 = 832;

    @Override
    protected StatusChangesLog createStatusChangesLog() {

        return new MemoryCommitLog(Arrays.asList(
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber1), StatusEdit.ADD_TASK(taskId1, taskType1, "param", userid1, 0, 0, null)),
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber2), StatusEdit.ADD_TASK(taskId2, taskType2, "param", userid1, 0, 0, null)),
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber3), StatusEdit.ADD_TASK(taskId3, taskType2, "param", userid1, 0, 0, null)),
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber4), StatusEdit.ADD_TASK(taskId4, taskType2, "param", userid1, 0, 0, null)),
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber5), StatusEdit.WORKER_CONNECTED(worker1, worker1processid, "localhost", new HashSet<>(), System.currentTimeMillis())),
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber6), StatusEdit.ASSIGN_TASK_TO_WORKER(taskId4, worker1, 1)),
                new MemoryCommitLog.MemoryLogLine(new LogSequenceNumber(0, sequenceNumber7), StatusEdit.TASK_STATUS_CHANGE(taskId4, worker1, Task.STATUS_FINISHED, "ok!"))
        ), new BrokerStatusSnapshot(taskId4, 0, new LogSequenceNumber(0, sequenceNumber_snapshot)));
    }

    @Test
    public void test() {
        broker.getClient().getAllTasks().forEach(task -> {
            System.out.println("task : " + task);
        });
        List<TaskStatusView> alltasks = broker.getClient().getAllTasks();
        assertEquals(3, alltasks.size());

        TaskStatusView task2 = alltasks.stream().filter(task -> task.getTaskId() == taskId2).findAny().get();
        assertEquals(Task.STATUS_WAITING, task2.getStatus());
        TaskStatusView task3 = alltasks.stream().filter(task -> task.getTaskId() == taskId3).findAny().get();
        assertEquals(Task.STATUS_WAITING, task3.getStatus());
        TaskStatusView task4 = alltasks.stream().filter(task -> task.getTaskId() == taskId4).findAny().get();
        assertEquals(Task.STATUS_FINISHED, task4.getStatus());
        assertEquals("ok!", task4.getResult());
        assertEquals(worker1, task4.getWorkerId());

        broker.getClient().getAllWorkers().forEach(worker -> {
            System.out.println("worker : " + worker);
        });
    }

}

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
import dodo.clustering.DummyCommitLog;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Simple tests
 *
 * @author enrico.olivelli
 */
public class SimpleSchedulerTest {

    @Test
    public void addTaskTest() throws Exception {
        Broker broker = new Broker(new DummyCommitLog());
        String queueName = "myqueue";
        Action addTask = Action.ADD_TASK(queueName, "mytask", "myparam", "tag1");
        long taskId = broker.executeAction(addTask).taskId;
        TaskStatusView task = broker.getTaskStatus(taskId);
        assertEquals(taskId, task.getTaskId());
        assertEquals(Task.STATUS_WAITING, task.getStatus());
        assertEquals(queueName, task.getQueueName());
        assertTrue(task.getCreatedTimestamp() > 0);

        String workerId = "mynode";
        String nodeLocation = "localhost";
        Map<String, Integer> maxTasksPerTag = new HashMap<>();
        maxTasksPerTag.put("tag1", 10);
        maxTasksPerTag.put("tag2", 10);
        Action addNode = Action.NODE_REGISTERED(workerId, nodeLocation, maxTasksPerTag);
        broker.executeAction(addNode).workerManager.nodeConnected();

        task = broker.getTaskStatus(taskId);
        assertEquals(taskId, task.getTaskId());
        assertEquals(Task.STATUS_RUNNING, task.getStatus());
        assertEquals(queueName, task.getQueueName());
        assertEquals(workerId, task.getWorkerId());
        assertTrue(task.getCreatedTimestamp() > 0);

        Action taskFinished = Action.TASK_FINISHED(taskId, workerId);
        broker.executeAction(taskFinished);

        task = broker.getTaskStatus(taskId);
        assertEquals(taskId, task.getTaskId());
        assertEquals(Task.STATUS_FINISHED, task.getStatus());
        assertEquals(queueName, task.getQueueName());
        assertEquals(workerId, task.getWorkerId());

    }

}

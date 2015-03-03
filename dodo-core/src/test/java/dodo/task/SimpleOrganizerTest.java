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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Simple tests
 *
 * @author enrico.olivelli
 */
public class SimpleOrganizerTest {

    @Test
    public void addTaskTest() throws Exception {
        Organizer organizer = new Organizer(new DummyCommitLog());
        String queueName = "myqueue";
        Action addTask = Action.ADD_TASK(queueName, "mytask", "myparam", "default");
        long taskId = organizer.executeAction(addTask).taskId;
        TaskStatusView task = organizer.getTaskStatus(taskId);
        assertEquals(taskId, task.getTaskId());
        assertEquals(Task.STATUS_WAITING, task.getStatus());
        assertEquals(queueName, task.getQueueName());
        assertTrue(task.getCreatedTimestamp() > 0);

    }
}

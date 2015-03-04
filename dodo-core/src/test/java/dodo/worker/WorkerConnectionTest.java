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
package dodo.worker;

import dodo.clustering.Action;
import dodo.clustering.DummyCommitLog;
import dodo.task.Broker;
import dodo.task.Task;
import dodo.task.TaskStatusView;
import dodo.worker.jvm.JVMBrokerSideConnection;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Simple tests for worker lifecycle
 *
 * @author enrico.olivelli
 */
public class WorkerConnectionTest {

    @Test
    public void testSimpleRegistration() throws Exception {
        Broker broker = new Broker(new DummyCommitLog());
        BrokerServerEndpoint endpoint = new BrokerServerEndpoint(broker);

        Action addTask = Action.ADD_TASK("myqueue", "mytask", "myparam", "tag1");
        long taskId = broker.executeAction(addTask).taskId;

        Map<String, Integer> tags = new HashMap<>();
        tags.put("tag1", 1);
        JVMBrokerSideConnection jmsConnection = new JVMBrokerSideConnection("test1", "process1", tags);

        endpoint.acceptConnection(jmsConnection);

        TaskStatusView task = broker.getTaskStatus(taskId);
        assertEquals(taskId, task.getTaskId());
        assertEquals(Task.STATUS_RUNNING, task.getStatus());
        assertEquals("test1", task.getWorkerId());

    }
}

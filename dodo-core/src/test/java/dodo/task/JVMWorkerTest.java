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

import dodo.client.ClientFacade;
import dodo.clustering.DummyCommitLog;
import dodo.executors.TaskExecutor;
import dodo.executors.TaskExecutorFactory;
import dodo.worker.JVMBrokerLocator;
import dodo.worker.WorkerCore;
import dodo.worker.WorkerStatusListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Simple tests
 *
 * @author enrico.olivelli
 */
public class JVMWorkerTest {

    @Test
    public void workerConnectionTest() throws Exception {
        Broker broker = new Broker(new DummyCommitLog());
        CountDownLatch connectedLatch = new CountDownLatch(1);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch taskExecuted = new CountDownLatch(1);
        WorkerStatusListener listener = new WorkerStatusListener() {

            @Override
            public void connectionEvent(String event, WorkerCore core) {
                if (event.equals(WorkerStatusListener.EVENT_CONNECTED)) {
                    connectedLatch.countDown();
                }
                if (event.equals(WorkerStatusListener.EVENT_DISCONNECTED)) {
                    disconnectedLatch.countDown();
                }
                System.out.println("connectionEvent:" + event);
            }

        };
        Map<String, Integer> tags = new HashMap<>();
        tags.put("tag1", 10);
        WorkerCore core = new WorkerCore(10, "abc", "here", "localhost", tags, new JVMBrokerLocator(broker), listener);
        core.start();
        assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

        core.registerTaskExecutorFactor("mytasktype", new TaskExecutorFactory() {

            @Override
            public TaskExecutor createTaskExecutor(Map<String, Object> parameters) {
                return new TaskExecutor() {

                    @Override
                    public void executeTask(Map<String, Object> parameters) throws Exception {
                        System.out.println("[WORKER] executeTask:" + parameters);
                        taskExecuted.countDown();
                    }

                };
            }
        });

        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("param1", "value1");
        taskParams.put("param2", "value2");
        long taskId = broker.getClient().submitTask("mytasktype", "queue1", "tag1", taskParams);
        System.out.println("taskId: " + taskId);

        assertTrue(taskExecuted.await(10, TimeUnit.SECONDS));

        core.stop();
        assertTrue(disconnectedLatch.await(10, TimeUnit.SECONDS));
    }
}

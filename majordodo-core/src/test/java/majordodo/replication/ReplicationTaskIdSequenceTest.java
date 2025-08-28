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
package majordodo.replication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.TaskStatusView;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import majordodo.task.TaskProperties;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.task.TasksHeap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class ReplicationTaskIdSequenceTest {

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            return new TaskProperties(group1, null);
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void simpleBrokerReplicationTest() throws Exception {

        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();

            long taskId;
            String taskParams = "param";

            String host = "localhost";
            int port = 7000;
            String host2 = "localhost";
            int port2 = 7001;

            BrokerConfiguration brokerConfig = new BrokerConfiguration();
            brokerConfig.setMaxWorkerIdleTime(5000);

            try (Broker broker1 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
                broker1.startAsWritable();
                try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker1.getAcceptor(), host, port)) {
                    server.start();

                    try (Broker broker2 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host2, port2, "", false, null)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
                        broker2.start();

                        taskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        long lastTaskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();

                        // need to write at least another entry to the ledger, if not the second broker could not see the add_task entry
                        broker1.noop();

                        assertNotNull(broker1.getClient().getTask(taskId));

                        boolean ok = false;
                        for (int i = 0; i < 10; i++) {
                            TaskStatusView task = broker2.getClient().getTask(lastTaskId);
                            //                            System.out.println("task:" + task);
                            Thread.sleep(1000);
                            if (task != null) {
                                ok = true;
                                break;
                            }
                        }
                        assertTrue(ok);

                        broker1.close();

                        for (int i = 0; i < 10; i++) {
                            if (broker2.isWritable()) {
                                break;
                            }
                            Thread.sleep(1000);
                        }
                        assertTrue(broker2.isWritable());

                        System.out.println("lastTaskId:" + lastTaskId);
                        long taskId2 = broker2.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId();
                        broker2.getClient().getAllTasks().forEach(t -> {
                            System.out.println("task:" + t.getTaskId());
                        });
                        System.out.println("taskId2:" + taskId2);
                        assertTrue(taskId2 > lastTaskId);
                    }
                }
            }
        }

    }
}

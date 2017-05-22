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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.network.BrokerHostData;
import majordodo.task.BrokerTestUtils;
import majordodo.task.LogNotAvailableException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author francesco.caliumi
 */
@BrokerTestUtils.StartReplicatedBrokers
@BrokerTestUtils.LogLevel(level="SEVERE")
public class BookkeeperFailuresTest extends BrokerTestUtils {
        
    private final LeaderShipChangeListener leaderShiplistener = new LeaderShipChangeListener() {
        @Override
        public void leadershipLost() {
            System.out.println("leadershipLost");
        }
        @Override
        public void leadershipAcquired() {
            System.out.println("leadershipAcquired");
        }
    };
    
    private BookKeeper createBookKeeper() throws Exception {
        byte[] localhostdata = BrokerHostData.formatHostdata(new BrokerHostData(broker1Host, broker1Port, "", false, null));
        ClientConfiguration config = new ClientConfiguration();
        config.setThrottleValue(0);
        
        ZKClusterManager zkClusterManager = new ZKClusterManager(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), leaderShiplistener, localhostdata, false);
        zkClusterManager.waitForConnection();
        BookKeeper bookKeeper = new BookKeeper(config, zkClusterManager.getZooKeeper());
        zkClusterManager.start();
        
        return bookKeeper;
    }
    
    @Before
    public void before() {
        broker1Config.setMaxWorkerIdleTime(1000);
        broker2Config.setMaxWorkerIdleTime(1000);
    }

    @Test
    public void fencingTest() throws Exception {
        String taskParams = "param";

        Map<String, Integer> tags = new HashMap<>();
        tags.put(TASKTYPE_MYTYPE, 1);

        assertTrue(broker1.isWritable());
        
        long taskId1 = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
        long taskId2 = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
        
        try {
            long taskId3 = broker2.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
            fail();
        } catch (Exception ex) {
            assertEquals("java.lang.Exception: broker_not_leader", ex.getMessage());
        }
        
        // Suspend flush in order to prevent the noop operation to alter the broker status before our test
        broker1.setSuspendLogFlush(true);
        
        ReplicatedCommitLog broker1scl = (ReplicatedCommitLog) broker1.getStatusChangesLog();
        
        try (BookKeeper bk = createBookKeeper();) {
            try (LedgerHandle fenceLedger = bk.openLedger(
                broker1scl.getCurrentLedgerId(), BookKeeper.DigestType.MAC, "dodo".getBytes(StandardCharsets.UTF_8));) {
            
                try {
                    long taskId4 = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
                    fail();
                } catch (LogNotAvailableException ex) {
                    assertEquals("org.apache.bookkeeper.client.BKException$BKLedgerFencedException", ex.getMessage());
                }
                
                assertFalse(broker1.isWritable());
                assertFalse(broker2.isWritable());
            }
        }
        
        waitForBrokerToBecomeLeader(broker2, 30*1000);
        
        long taskId5 = broker2.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
        assertTrue(taskId5 > 0);
    }
    
    @Test
    public void brokerUnavailableTest() throws Exception {
        String taskParams = "param";

        Map<String, Integer> tags = new HashMap<>();
        tags.put(TASKTYPE_MYTYPE, 1);

        assertTrue(broker1.isWritable());
        
        long taskId1 = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
        long taskId2 = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
        
        try {
            long taskId3 = broker2.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
            fail();
        } catch (Exception ex) {
            assertEquals("java.lang.Exception: broker_not_leader", ex.getMessage());
        }
        
        // Suspend flush in order to prevent the noop operation to alter the broker status before our test
        broker1.setSuspendLogFlush(true);
        
        zkServer.stopBookie();
        
        try {
            long taskId4 = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, null, null)).getTaskId();
            fail();
        } catch (LogNotAvailableException ex) {
            assertEquals("org.apache.bookkeeper.client.BKException$BKNotEnoughBookiesException", ex.getMessage());
        }
    }
}
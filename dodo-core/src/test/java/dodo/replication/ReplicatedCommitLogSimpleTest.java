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
package dodo.replication;

import dodo.task.*;
import com.sun.javafx.scene.control.skin.VirtualFlow;
import dodo.clustering.BrokerStatusSnapshot;
import dodo.clustering.FileCommitLog;
import dodo.clustering.LogSequenceNumber;
import dodo.clustering.StatusEdit;
import dodo.clustering.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Simple tests for FileCommit Log
 *
 * @author enrico.olivelli
 */
public class ReplicatedCommitLogSimpleTest {

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());                
                ZKClusterManager clusterManager = new ZKClusterManager(zkServer.getAddress(), 40000, "/dodo", new LeaderShipChangeListener(),
                        "ciao".getBytes())) {
            zkServer.startBookie();
            clusterManager.start();
            try (ReplicatedCommitLog log = new ReplicatedCommitLog(clusterManager, folderSnapshots.getRoot().toPath());) {
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                    fail();
                });
                log.startWriting();
                assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, 0);
                assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, 0);
                assertTrue(snapshot.getTasks().isEmpty());
                StatusEdit edit1 = StatusEdit.ADD_TASK(1, 123, "param1", "myuser", 0, 0, null);
                StatusEdit edit2 = StatusEdit.WORKER_CONNECTED("node1", "psasa", "localhost", new HashSet<>(), System.currentTimeMillis());
                StatusEdit edit3 = StatusEdit.ASSIGN_TASK_TO_WORKER(1, "worker1", 1);
                StatusEdit edit4 = StatusEdit.TASK_STATUS_CHANGE(1, "node1", Task.STATUS_FINISHED, "theresult");
                LogSequenceNumber logStatusEdit1 = log.logStatusEdit(edit1);
                LogSequenceNumber logStatusEdit2 = log.logStatusEdit(edit2);
                LogSequenceNumber logStatusEdit3 = log.logStatusEdit(edit3);
                LogSequenceNumber logStatusEdit4 = log.logStatusEdit(edit4);
            }

            try (ReplicatedCommitLog log = new ReplicatedCommitLog(clusterManager, folderSnapshots.getRoot().toPath());) {
                BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
                System.out.println("snapshot:" + snapshot);
                // no snapshot was taken...
                assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, 0);
                assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, 0);
                List<StatusEdit> edits = new ArrayList<>();
                AtomicLong last = new AtomicLong(-1);
                log.recovery(snapshot.getActualLogSequenceNumber(), (a, b) -> {
                    System.out.println("entry:" + a + ", " + b);
                   
                    assertTrue(a.sequenceNumber > last.get());
                    edits.add(b);
                    last.set(a.sequenceNumber);
                });
                log.startWriting();
                assertEquals(StatusEdit.TYPE_ADD_TASK, edits.get(0).editType);
                assertEquals(StatusEdit.TYPE_WORKER_CONNECTED, edits.get(1).editType);
                assertEquals(StatusEdit.TYPE_ASSIGN_TASK_TO_WORKER, edits.get(2).editType);
                assertEquals(StatusEdit.TYPE_TASK_STATUS_CHANGE, edits.get(3).editType);

            }

        }
    }

}

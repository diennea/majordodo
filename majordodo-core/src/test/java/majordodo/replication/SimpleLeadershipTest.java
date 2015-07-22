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

import majordodo.replication.ZKClusterManager;
import majordodo.replication.LeaderShipChangeListener;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * simple tests for cluster leadership
 *
 * @author enrico.olivelli
 */
public class SimpleLeadershipTest {

    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void test1() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath())) {
            CountDownLatch leadeshipTaken = new CountDownLatch(1);
            CountDownLatch leadeshipLost = new CountDownLatch(1);
            try (ZKClusterManager clusterManager = new ZKClusterManager(zkServer.getAddress(), 4000, "/dodo", new LeaderShipChangeListener() {

                @Override
                public void leadershipAcquired() {
                    System.out.println("leadershipAcquired!");
                    leadeshipTaken.countDown();
                }

                @Override
                public void leadershipLost() {
                    System.out.println("leadershipLost!");
                    leadeshipLost.countDown();
                }
            }, "test".getBytes("utf-8"))) {
                clusterManager.start();
                clusterManager.requestLeadership();
                assertTrue(leadeshipTaken.await(10, TimeUnit.SECONDS));
            }
            assertTrue(leadeshipLost.await(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void test2() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath())) {
            CountDownLatch leadeship1_Taken = new CountDownLatch(1);
            LeaderShipChangeListener leader1 = new LeaderShipChangeListener() {
                @Override
                public void leadershipAcquired() {
                    leadeship1_Taken.countDown();
                }
            };
            CountDownLatch leadeship2_Taken;
            ZKClusterManager clusterManager_2 = null;
            try (ZKClusterManager clusterManager_1 = new ZKClusterManager(zkServer.getAddress(), 4000, "/dodo", leader1, "test".getBytes("utf-8"))) {
                clusterManager_1.start();
                clusterManager_1.requestLeadership();
                assertTrue(leadeship1_Taken.await(10, TimeUnit.SECONDS));
                leadeship2_Taken = new CountDownLatch(1);
                LeaderShipChangeListener leader2 = new LeaderShipChangeListener() {
                    @Override
                    public void leadershipAcquired() {
                        leadeship2_Taken.countDown();
                    }
                };
                clusterManager_2 = new ZKClusterManager(zkServer.getAddress(), 4000, "/dodo", leader2, "test2".getBytes("utf-8"));
                clusterManager_2.start();
                clusterManager_2.requestLeadership();
            }

            assertTrue(leadeship2_Taken.await(10, TimeUnit.SECONDS));

            if (clusterManager_2 != null) {
                clusterManager_2.close();
            }

        }

    }

}

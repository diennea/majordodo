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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import majordodo.codepools.CodePool;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Tests for json serialization
 *
 * @author enrico.olivelli
 */
public class BrokerStatusSerializerTest {

    @Test
    public void test() throws Exception {
        BrokerStatusSnapshot snapBefore = new BrokerStatusSnapshot(17, 18, new LogSequenceNumber(101, 102));
        snapBefore.setTasks(new ArrayList<>());
        snapBefore.setTransactions(new ArrayList<>());
        snapBefore.setWorkers(new ArrayList<>());

        WorkerStatus dummy1 = new WorkerStatus();
        dummy1.setLastConnectionTs(12334);
        dummy1.setProcessId("3344");
        dummy1.setStatus(23);
        dummy1.setWorkerId("dfsdf");
        dummy1.setWorkerLocation("sdf");
        snapBefore.getWorkers().add(dummy1);

        WorkerStatus dummy2 = new WorkerStatus();
        dummy1.setLastConnectionTs(12334);
        dummy1.setProcessId("3344");
        dummy1.setStatus(23);
        dummy1.setWorkerId("dfsdf");
        dummy1.setWorkerLocation("sdf");
        snapBefore.getWorkers().add(dummy1);

        Task task1 = new Task();
        task1.setAttempts(123);
        task1.setMaxattempts(12345);
        task1.setCreatedTimestamp(1233);
        task1.setExecutionDeadline(45235);
        task1.setParameter("ccc");
        task1.setResult("fddf");
        task1.setSlot("fff");
        task1.setStatus(342);
        task1.setTaskId(343);
        task1.setType("444");
        task1.setUserId("fdfd");
        task1.setWorkerId("fgrfgdf");
        snapBefore.getTasks().add(task1);

        Task task2 = new Task();
        task2.setAttempts(123);
        task2.setMaxattempts(12345);
        task2.setCreatedTimestamp(1233);
        task2.setExecutionDeadline(45235);
        task2.setParameter("ccc");
        task2.setResult("fddf");
        task2.setSlot("fff");
        task2.setStatus(342);
        task2.setTaskId(343);
        task2.setType("444");
        task2.setUserId("fdfd");
        task2.setWorkerId("fgrfgdf");
        snapBefore.getTasks().add(task2);

        Task task3 = new Task();
        task3.setAttempts(123);
        task3.setMaxattempts(12345);
        task3.setCreatedTimestamp(1233);
        task3.setExecutionDeadline(45235);
        task3.setParameter("ccc");
        task3.setResult("fddf");
        task3.setSlot("fff");
        task3.setStatus(342);
        task3.setTaskId(343);
        task3.setType("444");
        task3.setUserId("fdfd");
        task3.setWorkerId("fgrfgdf");

        Transaction tx1 = new Transaction(4343, 2432);
        tx1.getPreparedTasks().add(task3);
        snapBefore.getTransactions().add(tx1);

        Transaction tx2 = new Transaction(4343, 2432);
        snapBefore.getTransactions().add(tx2);

        CodePool co1 = new CodePool("pool1", System.currentTimeMillis(), "test".getBytes(), 1000);
        snapBefore.getCodePools().add(co1);

        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BrokerStatusSnapshot.serializeSnapshot(snapBefore, oo);
        System.out.println("ser:" + oo.toString("utf-8"));
        InputStream in = new ByteArrayInputStream(oo.toByteArray());
        BrokerStatusSnapshot snap = BrokerStatusSnapshot.deserializeSnapshot(in);
        assertEquals(co1.getId(),snap.getCodePools().get(0).getId());
        assertEquals(co1.getTtl(),snap.getCodePools().get(0).getTtl());
        assertEquals(co1.getCreationTimestamp(),snap.getCodePools().get(0).getCreationTimestamp());
        Assert.assertArrayEquals(co1.getCodePoolData(),snap.getCodePools().get(0).getCodePoolData());
    }

    @Test
    public void testReal() throws Exception {

        InputStream data = BrokerStatusSerializerTest.class.getClassLoader().getResourceAsStream("examplesnap.json.gz");
        GZIPInputStream gzip = new GZIPInputStream(data);
        BrokerStatusSnapshot snap = BrokerStatusSnapshot.deserializeSnapshot(gzip);
        BrokerStatusSnapshot.serializeSnapshot(snap, new ByteArrayOutputStream());
    }
}

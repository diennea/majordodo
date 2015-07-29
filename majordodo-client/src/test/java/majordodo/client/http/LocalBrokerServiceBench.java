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
package majordodo.client.http;

import java.net.InetSocketAddress;
import majordodo.client.BrokerStatus;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import org.junit.Test;

/**
 * This is only a bencchmark to run on the localhost server
 *
 * @author enrico.olivelli
 */
public class LocalBrokerServiceBench {

    @Test
    public void test() throws Exception {
        ClientConfiguration config = ClientConfiguration
                .defaultConfiguration()
                .addBroker(new InetSocketAddress("localhost", 7364));
        try (Client client = new Client(config);) {

            try (ClientConnection con = client.openConnection()) {
                BrokerStatus status = con.getBrokerStatus();
                System.out.println("status:" + status);
                long _start = System.currentTimeMillis();
                int count = 100;
                for (int i = 0; i < count; i++) {
                    SubmitTaskRequest request = new SubmitTaskRequest();
                    request.setUserid("testuser");
                    request.setTasktype("script");
                    request.setData("System.out.println('test '+parameters+'!');");
                    request.setMaxattempts(1);
                    request.setSlot(null);
                    request.setTimeToLive(240000);
                    SubmitTaskResponse submitTask = con.submitTask(request);
                    System.out.println("submitTask:#" +i+" "+submitTask);
                }
                long _stop = System.currentTimeMillis();
                long delta = _stop - _start;
                double speed = count * 60000.0 / delta;
                System.out.println("time " + delta + " ms " + speed + " task/min " + (speed * 60) + " task/h");
                status = con.getBrokerStatus();
                System.out.println("status:" + status);
                TaskStatus t = con.getTaskStatus("1");
                System.out.println("t:"+t);

            }
        }
    }

}

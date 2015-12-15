package majordodo.testclients;

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

import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;

/**
 *
 * @author enrico.olivelli
 */
public class TaskSubmissionExample {

    public void submitCode() throws Exception {
        ClientConfiguration configuration = ClientConfiguration
                .defaultConfiguration()
                .setUsername("admin") // default admin user
                .setPassword("password") // default admin user
                .setBrokerDiscoveryService(new StaticBrokerDiscoveryService(BrokerAddress.http("127.0.0.1", 7364)));  // default localhost broker
        try (Client client = new Client(configuration);
                ClientConnection con = client.openConnection()) {
            {
                con.submitter()
                        .tasktype("mytype")
                        .maxAttempts(10)
                        .userId("user")
                        .timeToLive(1000)
                        .codePoolTimeToLive(60000);

                // submit a serialized object
                SubmitTaskResponse resp1 = con.submitter().submitTask(new ExampleExecutor());

                // request the instantiation of a class
                SubmitTaskResponse resp2 = con.submitter().submitTask(ExampleExecutor.class);

                while (true) {
                    TaskStatus task1 = con.getTaskStatus(resp1.getTaskId());
                    if (task1 == null) {
                        break;
                    }
                    System.out.println("result:" + task1.getResult());
                    System.out.println("status:" + task1.getStatus());
                    if (task1.getStatus().equals("finished") || task1.getStatus().equals("error")) {
                        break;
                    }
                }

            }

        }
    }
}

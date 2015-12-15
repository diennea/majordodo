package majordodo.testclients;

import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author enrico.olivelli
 */
public class TaskSubmissionExample {

    public void submitCode() throws Exception {
        ClientConfiguration configuration = ClientConfiguration
                .defaultConfiguration()
                .setUsername("admin") // default admin user
                .setPassword("password")  // default admin user
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

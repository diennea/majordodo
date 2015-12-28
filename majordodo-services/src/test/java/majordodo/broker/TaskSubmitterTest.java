/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.broker;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TaskSubmitterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        Properties pp = new Properties();
        pp.put("logs.dir", folder.newFolder().getAbsolutePath());
        pp.put("data.dir", folder.newFolder().getAbsolutePath());
        try (BrokerMain main = new BrokerMain(pp);) {
            main.start();
            ClientConfiguration configuration = ClientConfiguration
                    .defaultConfiguration()
                    .setUsername("admin")
                    .setPassword("password")
                    .setBrokerDiscoveryService(new StaticBrokerDiscoveryService(BrokerAddress.http("127.0.0.1", 7364)));
            try (Client client = new Client(configuration);
                    ClientConnection con = client.openConnection()) {
                {
                    con.submitter().tasktype("mytype");
                    SubmitTaskResponse resp1 = con.submitter().submitTask(new MyExecutor());
                    SubmitTaskResponse resp2 = con.submitter().submitTask(MyExecutor.class);

                    assertFalse(resp1.getTaskId().isEmpty());
                    assertFalse(resp2.getTaskId().isEmpty());

                    TaskStatus task1 = con.getTaskStatus(resp1.getTaskId());
                    assertEquals("mytype", task1.getTasktype());
                    TaskStatus task2 = con.getTaskStatus(resp2.getTaskId());
                    assertEquals("mytype", task2.getTasktype());

                    assertEquals("newinstance:majordodo.broker.MyExecutor", task2.getData());
                    assertTrue(task1.getData().startsWith("base64:"));

                }

            }

        }

    }

}

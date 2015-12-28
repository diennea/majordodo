/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.broker;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.CodePoolUtils;
import majordodo.client.CreateCodePoolRequest;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import majordodo.task.Broker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CodePoolsTest {

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
                    CreateCodePoolRequest createPoolRequest = new CreateCodePoolRequest();
                    createPoolRequest.setCodePoolID("mypool_" + System.currentTimeMillis());
                    createPoolRequest.setTtl(10000);
                    Path expectedJar = mavenTargetDir.getParent().getParent().resolve("majordodo-test-clients").resolve("target").resolve("majordodo-test-clients-" + Broker.VERSION() + ".jar");
                    byte[] mockCodePoolData = CodePoolUtils.createZipWithOneEntry("codepooltest.jar", Files.readAllBytes(expectedJar));
                    createPoolRequest.setCodePoolData(
                            CodePoolUtils.encodeCodePoolData(new ByteArrayInputStream(
                                    CodePoolUtils.createZipWithOneEntry("code.zip", mockCodePoolData))));
                    con.createCodePool(createPoolRequest);

                    SubmitTaskRequest req = new SubmitTaskRequest();
                    req.setTasktype("mytype");
                    req.setUserid("myuser");
                    req.setData("newinstance:majordodo.testclients.SimpleExecutor");
                    req.setMode(SubmitTaskRequest.MODE_OBJECT);
                    req.setCodePoolId(createPoolRequest.getCodePoolID());

                    SubmitTaskResponse resp = con.submitTask(req);
                    assertFalse(resp.getTaskId().isEmpty());
                    String taskId = resp.getTaskId();

                    TaskStatus task = con.getTaskStatus(taskId);
                    assertEquals("mytype", task.getTasktype());
                    assertEquals(taskId, task.getTaskId());
                    assertEquals("newinstance:majordodo.testclients.SimpleExecutor", task.getData());
                    assertEquals("myuser", task.getUserId());
                    assertEquals("waiting", task.getStatus());
                    assertEquals("object", task.getMode());
                    assertEquals(createPoolRequest.getCodePoolID(), task.getCodePoolId());                    
                    
                    
                    con.deleteCodePool(createPoolRequest.getCodePoolID());
                }

            }

        }

    }

}

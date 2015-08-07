/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.junit.Test;

public class BrokerHTTPClientTest {

    @Test
    public void test() throws Exception {
        try (BrokerMain main = new BrokerMain(new Properties());) {
            main.start();
            ClientConfiguration configuration = ClientConfiguration
                    .defaultConfiguration()
                    .setBrokerDiscoveryService(new StaticBrokerDiscoveryService(BrokerAddress.http("127.0.0.1", 7364)));
            try (Client client = new Client(configuration);
                    ClientConnection con = client.openConnection()) {
                {
                    SubmitTaskRequest req = new SubmitTaskRequest();
                    req.setTasktype("mytype");
                    req.setUserid("myuser");
                    req.setData("test1");

                    SubmitTaskResponse resp = con.submitTask(req);
                    assertFalse(resp.getTaskId().isEmpty());
                    String taskId = resp.getTaskId();

                    TaskStatus task = con.getTaskStatus(taskId);
                    assertEquals("mytype", task.getTasktype());
                    assertEquals(taskId, task.getTaskId());
                    assertEquals("test1", task.getData());
                    assertEquals("myuser", task.getUserId());
                    assertEquals("waiting", task.getStatus());
                }

                con.setTransacted(true);
                {
                    SubmitTaskRequest req = new SubmitTaskRequest();
                    req.setTasktype("mytype");
                    req.setUserid("myuser");
                    req.setData("test1");

                    SubmitTaskResponse resp = con.submitTask(req);
                    assertFalse(resp.getTaskId().isEmpty());
                    String taskId = resp.getTaskId();
                    con.commit();
                    TaskStatus task = con.getTaskStatus(taskId);
                    assertEquals("mytype", task.getTasktype());
                    assertEquals(taskId, task.getTaskId());
                    assertEquals("test1", task.getData());
                    assertEquals("myuser", task.getUserId());
                    assertEquals("waiting", task.getStatus());
                    con.setTransacted(false);
                }
                
                {
                    List<SubmitTaskRequest> multi = new ArrayList<>();
                    for (int i = 0; i < 100; i++) {
                        SubmitTaskRequest req = new SubmitTaskRequest();
                        req.setTasktype("mytype");
                        req.setUserid("myuser");
                        req.setData("test1_" + i);
                        multi.add(req);
                    }
                    List<SubmitTaskResponse> responses = con.submitTasks(multi);
                    for (int i = 0; i < 100; i++) {
                        SubmitTaskResponse resp = responses.get(i);
                        System.out.println("resp:"+resp);
                        assertFalse(resp.getTaskId().isEmpty());
                        String taskId = resp.getTaskId();
                        TaskStatus task = con.getTaskStatus(taskId);
                        assertEquals("mytype", task.getTasktype());
                        assertEquals(taskId, task.getTaskId());
                        assertEquals("test1_" + i, task.getData());
                        assertEquals("myuser", task.getUserId());
                        assertEquals("waiting", task.getStatus());
                    }
                }

                {
                    con.setTransacted(true);
                    List<SubmitTaskRequest> multi = new ArrayList<>();
                    for (int i = 0; i < 100; i++) {
                        SubmitTaskRequest req = new SubmitTaskRequest();
                        req.setTasktype("mytype");
                        req.setUserid("myuser");
                        req.setData("test1_" + i);
                        multi.add(req);
                    }
                    List<SubmitTaskResponse> responses = con.submitTasks(multi);
                    for (int i = 0; i < 100; i++) {
                        SubmitTaskResponse resp = responses.get(i);
                        assertFalse(resp.getTaskId().isEmpty());
                        String taskId = resp.getTaskId();
                        TaskStatus task = con.getTaskStatus(taskId);
                        assertNull(task);
                    }
                    con.commit();
                    for (int i = 0; i < 100; i++) {
                        SubmitTaskResponse resp = responses.get(i);
                        assertFalse(resp.getTaskId().isEmpty());
                        String taskId = resp.getTaskId();
                        TaskStatus task = con.getTaskStatus(taskId);
                        assertEquals("mytype", task.getTasktype());
                        assertEquals(taskId, task.getTaskId());
                        assertEquals("test1_" + i, task.getData());
                        assertEquals("myuser", task.getUserId());
                        assertEquals("waiting", task.getStatus());
                    }
                }

            }

        }

    }

}

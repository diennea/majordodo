/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.embedded;

import java.util.ArrayList;
import java.util.List;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BrokerEmbeddedClientTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        EmbeddedBrokerConfiguration ee = new EmbeddedBrokerConfiguration();
        ee.getProperties().put(EmbeddedBrokerConfiguration.KEY_LOGSDIRECTORY, folder.newFolder().getAbsolutePath());
        ee.getProperties().put(EmbeddedBrokerConfiguration.KEY_SNAPSHOTSDIRECTORY, folder.newFolder().getAbsolutePath());

        try (EmbeddedBroker main = new EmbeddedBroker(ee);) {
            main.start();

            try (EmbeddedClient client = new EmbeddedClient();
                    ClientConnection con = client.openConnection()) {
                {
                    SubmitTaskRequest req = new SubmitTaskRequest();
                    req.setTasktype("mytype");
                    req.setUserid("myuser");
                    req.setData("test1");
                    req.setAttempt(3);

                    SubmitTaskResponse resp = con.submitTask(req);
                    assertFalse(resp.getTaskId().isEmpty());
                    String taskId = resp.getTaskId();

                    TaskStatus task = con.getTaskStatus(taskId);
                    assertEquals("mytype", task.getTasktype());
                    assertEquals(taskId, task.getTaskId());
                    assertEquals("test1", task.getData());
                    assertEquals("myuser", task.getUserId());
                    assertEquals("waiting", task.getStatus());
                    assertEquals(3, task.getAttempts());
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

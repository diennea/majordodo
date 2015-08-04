/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.embedded;

import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

public class BrokerEmbeddedClientTest {

    @Test
    public void test() throws Exception {
        try (EmbeddedBroker main = new EmbeddedBroker(new EmbeddedBrokerConfiguration());) {            
            main.start();
            
            try (EmbeddedClient client = new EmbeddedClient();
                    ClientConnection con = client.openConnection()) {
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

        }

    }

}

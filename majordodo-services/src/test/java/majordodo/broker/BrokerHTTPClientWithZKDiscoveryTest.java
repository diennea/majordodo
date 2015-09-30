/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.discovery.ZookeeperDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import org.apache.zookeeper.ZooKeeper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BrokerHTTPClientWithZKDiscoveryTest {

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.SEVERE;
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
            }
        });
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        SimpleFormatter f = new SimpleFormatter();
        ch.setFormatter(f);
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test() throws Exception {
        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();
            ZooKeeper zkClient = new ZooKeeper(zkServer.getAddress(), zkServer.getTimeout(), null);

            Properties pp = new Properties();
            pp.setProperty("clustering.mode", "clustered");
            pp.setProperty("zk.address", zkServer.getAddress());
            pp.setProperty("zk.path", zkServer.getPath());
            pp.put("logs.dir", folder.newFolder().getAbsolutePath());
            pp.put("data.dir", folder.newFolder().getAbsolutePath());

            try (BrokerMain main = new BrokerMain(pp)) {
                main.start();
                main.waitForLeadership();
               
                ClientConfiguration configuration = ClientConfiguration
                        .defaultConfiguration()
                        .setUsername("admin")
                        .setPassword("password")
                        .setBrokerDiscoveryService(new ZookeeperDiscoveryService(zkClient).setZkPath(zkServer.getPath()));
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
                            System.out.println("resp:" + resp);
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

}

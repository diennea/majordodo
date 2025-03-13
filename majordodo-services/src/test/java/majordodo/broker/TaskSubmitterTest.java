/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;
import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TaskSubmitterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void tearDown() throws Exception {
        Files.walkFileTree(folder.getRoot().toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file); // Delete file
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir); // Delete directory after files inside are deleted
                return FileVisitResult.CONTINUE;
            }
        });

    }

    @Test
    public void testTaskSubmitterSelfSignedCert() throws Exception {
        Properties props = new Properties();
        doTest(props);
    }

    @Test
    public void testTaskSubmitterNoSSL() throws Exception {
        Properties props = new Properties();
        props.setProperty("broker.ssl", "false");
        doTest(props);
    }

    @Test
    public void testTaskSubmitterCertificate() throws Exception {
        Path certFile = folder.getRoot().toPath().resolve("cert1.key");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.key"))) {
            Files.copy(in, certFile);
        }

        Path certChainFile = folder.getRoot().toPath().resolve("cert1.pem");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.pem"))) {
            Files.copy(in, certChainFile);
        }

        Properties props = new Properties();
        props.setProperty("broker.ssl", "true");
        props.setProperty("broker.ssl.certificatefile", certFile.toFile().getAbsolutePath());
        props.setProperty("broker.ssl.certificatefilepassword", "majordodo1");
        props.setProperty("broker.ssl.certificatechainfile", certChainFile.toFile().getAbsolutePath());
        doTest(props);
    }

    @Test
    public void testTaskSubmitterPKCS12Certificate() throws Exception {
        Path certFile = folder.getRoot().toPath().resolve("cert1.p12");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.p12"))) {
            Files.copy(in, certFile);
        }

        Properties props = new Properties();
        props.setProperty("broker.ssl", "true");
        props.setProperty("broker.ssl.certificatefile", certFile.toFile().getAbsolutePath());
        props.setProperty("broker.ssl.certificatefilepassword", "majordodo1");
        doTest(props);
    }

    public void doTest(Properties pp) throws Exception {
        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
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

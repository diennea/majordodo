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
package majordodo.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getILoggerFactory;
import static org.slf4j.LoggerFactory.getLogger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import majordodo.broker.StandaloneHttpAPIServlet;
import majordodo.client.BrokerAddress;
import majordodo.client.ClientConnection;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.discovery.StaticBrokerDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import majordodo.clientfacade.AuthenticatedUser;
import majordodo.clientfacade.AuthenticationManager;
import majordodo.clientfacade.UserRole;
import majordodo.executors.TaskExecutor;
import majordodo.network.netty.NettyChannelAcceptor;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BrokerEmbeddedCertificateTest {

    private static final String HTTP_HOST = "localhost";
    private static final int HTTP_PORT = 7844;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final ch.qos.logback.classic.Logger NETTY_LOGGER = (ch.qos.logback.classic.Logger) getLogger(NettyChannelAcceptor.class);
    private static ListAppender<ILoggingEvent> listAppender;

    @BeforeClass
    public static void setUpClass() throws Exception {
        listAppender = interceptNettyLogs();
    }

    @After
    public void tearDown() throws Exception {
        listAppender.list.clear();
        Files.walkFileTree(folder.getRoot().toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });

    }

    @Test
    public void testTaskSubmitterSelfSignedCert() throws Exception {
        EmbeddedBrokerConfiguration config = new EmbeddedBrokerConfiguration();
        config.getProperties().put(EmbeddedBrokerConfiguration.KEY_SSL, "true");
        doTest(config);

        boolean found = listAppender.list.stream()
                .anyMatch(line -> line.getFormattedMessage().contains("start SSL with self-signed auto-generated certificate"));
        assertTrue(found);
    }

    @Test
    public void testTaskSubmitterNoSSL() throws Exception {
        EmbeddedBrokerConfiguration config = new EmbeddedBrokerConfiguration();
        config.getProperties().put(EmbeddedBrokerConfiguration.KEY_SSL, "false");
        doTest(config);

        boolean found = listAppender.list.stream()
                .anyMatch(line -> line.getFormattedMessage().contains("start SSL"));
        assertFalse(found);
    }

    @Test
    public void testTaskSubmitterCertificateAsFile() throws Exception {
        Path certFile = folder.getRoot().toPath().resolve("cert1.key");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.key"))) {
            Files.copy(in, certFile);
        }

        Path certChainFile = folder.getRoot().toPath().resolve("cert1.pem");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.pem"))) {
            Files.copy(in, certChainFile);
        }

        EmbeddedBrokerConfiguration config = new EmbeddedBrokerConfiguration();
        config.getProperties().put(EmbeddedBrokerConfiguration.KEY_SSL, "true");
        config.getProperties().put(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_FILE, certFile.toFile());
        config.getProperties().put(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_PASSWORD, "majordodo1");
        config.getProperties().put(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_CHAIN_FILE, certChainFile.toFile());
        doTest(config);

        boolean found = listAppender.list.stream()
                .anyMatch(line -> line.getFormattedMessage().contains("start SSL with certificate") && line.getFormattedMessage().contains(certFile.toFile().getAbsolutePath()));
        assertTrue(found);
    }

    @Test
    public void testTaskSubmitterCertificateAsFilePath() throws Exception {
        Path certFile = folder.getRoot().toPath().resolve("cert1.key");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.key"))) {
            Files.copy(in, certFile);
        }

        Path certChainFile = folder.getRoot().toPath().resolve("cert1.pem");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.pem"))) {
            Files.copy(in, certChainFile);
        }

        EmbeddedBrokerConfiguration config = new EmbeddedBrokerConfiguration();
        config.getProperties().put(EmbeddedBrokerConfiguration.KEY_SSL, "true");
        config.getProperties().put(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_FILE, certFile.toFile().getAbsolutePath());
        config.getProperties().put(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_PASSWORD, "majordodo1");
        config.getProperties().put(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_CHAIN_FILE, certChainFile.toFile().getAbsolutePath());
        doTest(config);

        boolean found = listAppender.list.stream()
                .anyMatch(line -> line.getFormattedMessage().contains("start SSL with certificate") && line.getFormattedMessage().contains(certFile.toFile().getAbsolutePath()));
        assertTrue(found);
    }

    @Test
    public void testTaskSubmitterPKCS12Certificate() throws Exception {
        Path certFile = folder.getRoot().toPath().resolve("cert1.p12");
        try (BufferedInputStream in = new BufferedInputStream(this.getClass().getClassLoader().getResourceAsStream("cert1.p12"))) {
            Files.copy(in, certFile);
        }

        EmbeddedBrokerConfiguration config = new EmbeddedBrokerConfiguration();
        config.getProperties().put("broker.ssl", "true");
        config.getProperties().put("broker.ssl.certificatefile", certFile.toFile().getAbsolutePath());
        config.getProperties().put("broker.ssl.certificatefilepassword", "majordodo1");
        doTest(config);

        boolean found = listAppender.list.stream()
                .anyMatch(line -> line.getFormattedMessage().contains("start SSL with certificate") && line.getFormattedMessage().contains(certFile.toFile().getAbsolutePath()));
        assertTrue(found);
    }

    public void doTest(EmbeddedBrokerConfiguration config) throws Exception {
        config.getProperties().put(EmbeddedBrokerConfiguration.KEY_SNAPSHOTSDIRECTORY, folder.newFolder().getAbsolutePath());
        config.getProperties().put(EmbeddedBrokerConfiguration.KEY_LOGSDIRECTORY, folder.newFolder().getAbsolutePath());

        // start the broker
        try (EmbeddedBroker embeddedBroker = new EmbeddedBroker(config)) {
            embeddedBroker.setAuthenticationManager(new AuthenticationManager() {
                @Override
                public AuthenticatedUser login(String username, String password) {
                    return new AuthenticatedUser("1", UserRole.ADMINISTRATOR);
                }
            });
            embeddedBroker.start();

            // start the httpserver which will receive the task submit requests
            Server httpserver = new Server(new InetSocketAddress(HTTP_HOST, HTTP_PORT));
            try {
                ContextHandlerCollection contexts = new ContextHandlerCollection();
                httpserver.setHandler(contexts);
                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.GZIP);
                context.setContextPath("/");
                ServletHolder jerseyServlet = new ServletHolder(new StandaloneHttpAPIServlet());
                jerseyServlet.setInitOrder(0);
                context.addServlet(jerseyServlet, "/majordodo");
                contexts.addHandler(context);
                httpserver.start();

                // create the client for submitting tasks
                ClientConfiguration configuration = ClientConfiguration
                        .defaultConfiguration()
                        .setUsername("admin")
                        .setPassword("password")
                        .setDisableHttpsVerification(true)
                        .setBrokerDiscoveryService(new StaticBrokerDiscoveryService(BrokerAddress.http(HTTP_HOST, HTTP_PORT)));
                try (Client client = new Client(configuration);
                     ClientConnection con = client.openConnection()) {
                    con.submitter().tasktype("mytype");
                    SubmitTaskResponse resp1 = con.submitter().submitTask(new MyExecutor());

                    assertFalse(resp1.getTaskId().isEmpty());

                    TaskStatus task1 = con.getTaskStatus(resp1.getTaskId());
                    assertEquals("mytype", task1.getTasktype());

                }
            } finally {
                httpserver.stop();
            }
        }
    }

    private static ListAppender<ILoggingEvent> interceptNettyLogs() {
        NETTY_LOGGER.setLevel(Level.ALL);
        LoggerContext ctx = (LoggerContext) getILoggerFactory();

        ListAppender<ILoggingEvent> list = new ListAppender<>();
        list.setContext(ctx);
        list.start();
        NETTY_LOGGER.addAppender(list);

        return list;
    }

    private static final class MyExecutor extends TaskExecutor implements Serializable {
        @Override
        public String executeTask(Map<String, Object> parameters) throws Exception {
            return "OK!";
        }
    }

}

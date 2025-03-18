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
package majordodo.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import majordodo.client.discovery.ZookeeperDiscoveryService;
import majordodo.client.http.Client;
import majordodo.client.http.ClientConfiguration;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.replication.ZKTestEnv;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import majordodo.task.TaskProperties;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.task.TasksHeap;
import majordodo.utils.TestUtils;
import org.apache.zookeeper.ZooKeeper;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HttpClientLeaderRecoveryTest {

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

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            return new TaskProperties(group1, null);
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderSnapshots2 = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderSnapshots3 = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void simpleBrokerReplicationTest() throws Exception {

        try (ZKTestEnv zkServer = new ZKTestEnv(folderZk.getRoot().toPath());) {
            zkServer.startBookie();

            String taskParams = "param";

            String host = "localhost";
            int port = 7000;
            String host2 = "localhost";
            int port2 = 7001;
            String httppath = "/my-http-path";
            String httphost = "localhost";
            int httpport = 7002;
            String httppath2 = "/my-http-path2";
            String httphost2 = "localhost";
            int httpport2 = 7003;

            Map<String, String> additionalInfo1 = new HashMap<>();
            String clientapiurl1 = "http://" + httphost + ":" + httpport + httppath;
            additionalInfo1.put("client.api.url", clientapiurl1);

            Map<String, String> additionalInfo2 = new HashMap<>();
            String clientapiurl2 = "http://" + httphost2 + ":" + httpport2 + httppath2;
            additionalInfo2.put("client.api.url", clientapiurl2);

            BrokerConfiguration brokerConfig = new BrokerConfiguration();

            ZooKeeper zkClient = new ZooKeeper(zkServer.getAddress(), zkServer.getTimeout(), null);
            try {

                ClientConfiguration configuration = ClientConfiguration
                        .defaultConfiguration()
                        .setUsername("admin")
                        .setPassword("password")
                        .setBrokerDiscoveryService(new ZookeeperDiscoveryService(zkClient).setZkPath(zkServer.getPath()));

                try (Client client = new Client(configuration);
                     ClientConnection con = client.openConnection()) {

                    Broker broker1 = null;
                    Broker broker2 = null;
                    Server httpserver1 = null;
                    Server httpserver2 = null;
                    try {
                        broker1 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host, port, "", false, additionalInfo1)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));
                        broker1.startAsWritable();
                        try (NettyChannelAcceptor server1 = new NettyChannelAcceptor(broker1.getAcceptor(), host, port)) {
                            server1.start();

                            httpserver1 = startHttpServer(httphost, httpport, httppath, broker1);
                            try {
                                broker2 = new Broker(brokerConfig, new ReplicatedCommitLog(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(), folderSnapshots2.getRoot().toPath(), BrokerHostData.formatHostdata(new BrokerHostData(host2, port2, "", false, additionalInfo2)), false), new TasksHeap(1000, createTaskPropertiesMapperFunction()));
                                broker2.start();
                                httpserver2 = startHttpServer(httphost2, httpport2, httppath2, broker2);
                                Broker _broker2 = broker2;
                                try (NettyChannelAcceptor server2 = new NettyChannelAcceptor(broker2.getAcceptor(), host2, port2)) {
                                    server2.start();

                                    long taskId = submitTask(taskParams, con);
                                    broker1.noop();
                                    assertNotNull(broker1.getClient().getTask(taskId));
                                    TestUtils.waitForCondition(() -> {
                                        return _broker2.getClient().getTask(taskId) != null;
                                    }, null, 100);
                                }
                                broker1.close();
                                broker1 = null;

                                TestUtils.waitForCondition(() -> {
                                    return _broker2.isWritable();
                                }, null, 100);

                                // client will failover to broker2
                                long taskId = submitTask(taskParams, con);

                                assertNotNull(_broker2.getClient().getTask(taskId));
                                TestUtils.waitForCondition(() -> {
                                    return _broker2.getClient().getTask(taskId) != null;
                                }, null, 100);

                            } finally {
                                if (broker2 != null) {
                                    broker2.close();
                                    broker2 = null;
                                }
                            }
                        }
                    } finally {
                        if (broker1 != null) {
                            broker1.close();
                        }
                        if (httpserver1 != null) {
                            httpserver1.stop();
                        }
                        if (httpserver2 != null) {
                            httpserver2.stop();
                        }
                    }
                }
            } finally {
                zkClient.close();
            }
        }
    }

    private Server startHttpServer(String httphost, int httpport, String httppath, Broker broker) throws Exception {
        Server httpserver = new Server(new InetSocketAddress(httphost, httpport));
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        httpserver.setHandler(contexts);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.GZIP);
        context.setContextPath("/");
        ServletHolder jerseyServlet = new ServletHolder(new StandaloneHttpAPIServlet(broker));
        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, httppath);
        contexts.addHandler(context);
        httpserver.start();
        return httpserver;
    }

    private long submitTask(String taskParams, final ClientConnection con) throws ClientException, NumberFormatException {
        long taskId;
        SubmitTaskRequest req = new SubmitTaskRequest();
        req.setTasktype(TASKTYPE_MYTYPE);
        req.setUserid(userId);
        req.setData(taskParams);
        req.setAttempt(0);
        SubmitTaskResponse resp = con.submitTask(req);
        assertFalse(resp.getTaskId().isEmpty());
        taskId = Long.parseLong(resp.getTaskId());
        return taskId;
    }

    private static class StandaloneHttpAPIServlet extends HttpServlet {

        private Broker broker;

        public StandaloneHttpAPIServlet(Broker broker) {
            this.broker = broker;
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            majordodo.clientfacade.HttpAPIImplementation.doGet(req, resp, broker);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            majordodo.clientfacade.HttpAPIImplementation.doPost(req, resp, broker);
        }
    }
}

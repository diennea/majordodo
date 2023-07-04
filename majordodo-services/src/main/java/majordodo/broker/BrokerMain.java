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
package majordodo.broker;

import majordodo.task.FileCommitLog;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.task.StatusChangesLog;
import majordodo.task.TasksHeap;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import majordodo.daemons.PidFileLocker;
import majordodo.network.BrokerHostData;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.task.SingleUserAuthenticationManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Created by enrico.olivelli on 23/03/2015.
 */
public class BrokerMain implements AutoCloseable {

    private Broker broker;
    private Server httpserver;
    private NettyChannelAcceptor server;
    private final Properties configuration;
    private final PidFileLocker pidFileLocker;

    private static BrokerMain runningInstance;
    private static final String BOOKKEEPER_ADDITIONAL_PREFIX = "bookkeeper.additional.";

    public Broker getBroker() {
        return broker;
    }

    public BrokerMain(Properties configuration) {
        this.configuration = configuration;
        this.pidFileLocker = new PidFileLocker(Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath());
    }

    @Override
    public void close() {
        if (server != null) {
            try {
                server.close();
            } catch (Exception ex) {
                Logger.getLogger(BrokerMain.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                server = null;
            }
        }
        if (httpserver != null) {
            try {
                httpserver.stop();
                httpserver.join();
            } catch (Exception ex) {
                Logger.getLogger(BrokerMain.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                httpserver = null;
            }
        }
        if (broker != null) {
            try {
                broker.stop();
            } catch (Exception ex) {
                Logger.getLogger(BrokerMain.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                broker = null;
            }
        }
        pidFileLocker.close();
        running.countDown();
    }

    public static void main(String... args) {
        try {

            Properties configuration = new Properties();
            File configFile;
            if (args.length > 0) {
                configFile = new File(args[0]);
                try (FileInputStream reader = new FileInputStream(configFile)) {
                    configuration.load(reader);
                }
            } else {
                configFile = new File("conf/broker.properties");
                if (configFile.isFile()) {
                    try (FileInputStream reader = new FileInputStream(configFile)) {
                        configuration.load(reader);
                    }
                } else {
                    throw new Exception("Cannot find " + configFile.getAbsolutePath());
                }
            }

            Runtime.getRuntime().addShutdownHook(new Thread("ctrlc-hook") {

                @Override
                public void run() {
                    System.out.println("Ctrl-C trapped. Shutting down");
                    BrokerMain _brokerMain = runningInstance;
                    if (_brokerMain != null) {
                        _brokerMain.close();
                    }
                }

            });
            runningInstance = new BrokerMain(configuration);
            runningInstance.start();
            runningInstance.join();

        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    private final static CountDownLatch running = new CountDownLatch(1);

    public void join() {
        try {
            running.await();
        } catch (InterruptedException discard) {
        }
    }

    public void start() throws Exception {
        pidFileLocker.lock();
        String id = configuration.getProperty("broker.id", "");
        if (id.isEmpty()) {
            id = UUID.randomUUID().toString();
        }
        String host = configuration.getProperty("broker.host", "127.0.0.1");
        int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
        boolean ssl = Boolean.parseBoolean(configuration.getProperty("broker.ssl", "true"));
        boolean sslunsecure = Boolean.parseBoolean(configuration.getProperty("broker.ssl.unsecure", "true"));
        String certfile = configuration.getProperty("broker.ssl.certificatefile", "");
        String certchainfile = configuration.getProperty("broker.ssl.certificatechainfile", "");
        String certpassword = configuration.getProperty("broker.ssl.certificatefilepassword", null);
        String sslciphers = configuration.getProperty("broker.ssl.ciphers", "");
        String httphost = configuration.getProperty("broker.http.host", "0.0.0.0");
        int httpport = Integer.parseInt(configuration.getProperty("broker.http.port", "7364"));
        int taskheapsize = Integer.parseInt(configuration.getProperty("broker.tasksheap.size", "1000000"));
        String assigner = configuration.getProperty("tasks.taskpropertiesmapperfunction", "");
        String sharedsecret = configuration.getProperty("sharedsecret", "dodo");
        String clusteringmode = configuration.getProperty("clustering.mode", "singleserver");
        int workerthreads = Integer.parseInt(configuration.getProperty("io.worker.threads", "16"));

        String adminuser = configuration.getProperty("admin.username", "admin");
        String adminpassword = configuration.getProperty("admin.password", "password");

        System.out.println("Starting MajorDodo Broker " + Broker.VERSION());
        TaskPropertiesMapperFunction mapper;
        if (assigner.isEmpty()) {
            mapper = new DefaultTaskPropertiesMapperFunction();
        } else {
            mapper = (TaskPropertiesMapperFunction) Class.forName(assigner).newInstance();
            System.out.println("TaskPropertiesMapperFunction Mapper:" + mapper);
        }
        String httppath = "/majordodo";
        Map<String, String> additionalInfo = new HashMap<>();
        String clientapiurl = "http://" + host + ":" + httpport + httppath;
        String uiurl = "http://" + host + ":" + httpport + "/ui/#/home?brokerUrl=../majordodo";
        additionalInfo.put("client.api.url", clientapiurl);
        additionalInfo.put("broker.id", id);

        StatusChangesLog log;
        switch (clusteringmode) {
            case "singleserver": {
                String logsdir = configuration.getProperty("logs.dir", "txlog");
                String snapdir = configuration.getProperty("data.dir", "data");
                long maxFileSize = Long.parseLong(configuration.getProperty("logs.maxfilesize", (1024 * 1024) + ""));
                log = new FileCommitLog(Paths.get(snapdir), Paths.get(logsdir), maxFileSize);
                break;
            }
            case "clustered": {
                String zkAddress = configuration.getProperty("zk.address", "localhost:1281");
                int zkSessionTimeout = Integer.parseInt(configuration.getProperty("zk.sessiontimeout", "40000"));
                String zkPath = configuration.getProperty("zk.path", "/majordodo");
                String snapdir = configuration.getProperty("data.dir", "data");
                boolean zkSecure = Boolean.parseBoolean(configuration.getProperty("zk.secure", "false"));
                Map<String, String> additionalBookKeeperConfig = new HashMap<>();
                for (Object _key : configuration.keySet()) {
                    String key = _key + "";
                    if (key.startsWith(BOOKKEEPER_ADDITIONAL_PREFIX)) {
                        additionalBookKeeperConfig
                            .put(key.substring(BOOKKEEPER_ADDITIONAL_PREFIX.length()), configuration.getProperty(key, null));
                    }
                }
                ReplicatedCommitLog _log = new ReplicatedCommitLog(zkAddress, zkSessionTimeout, zkPath, Paths.get(snapdir),
                    BrokerHostData.formatHostdata(new BrokerHostData(host, port, Broker.VERSION(), ssl, additionalInfo)),
                    zkSecure, additionalBookKeeperConfig, id
                );
                log = _log;
                int ensemble = Integer.parseInt(configuration.getProperty("bookkeeper.ensemblesize", _log.getEnsemble() + ""));
                int writeQuorumSize = Integer.parseInt(configuration.getProperty("bookkeeper.writequorumsize", _log.getWriteQuorumSize() + ""));
                int ackQuorumSize = Integer.parseInt(configuration.getProperty("bookkeeper.ackquorumsize", _log.getAckQuorumSize() + ""));
                long ledgersRetentionPeriod = Long.parseLong(configuration.getProperty("bookkeeper.ledgersretentionperiod", _log.getLedgersRetentionPeriod() + ""));
                _log.setAckQuorumSize(ackQuorumSize);
                _log.setEnsemble(ensemble);
                _log.setLedgersRetentionPeriod(ledgersRetentionPeriod);
                _log.setWriteQuorumSize(writeQuorumSize);
                _log.setSslUnsecure(sslunsecure);
                break;
            }
            default:
                throw new RuntimeException("bad value for clustering.mode property, only valid values are singleserver|clustered");
        }

        BrokerConfiguration config = new BrokerConfiguration();
        Map<String, Object> props = new HashMap<>();
        configuration.keySet().forEach(k -> props.put(k.toString(), configuration.get(k)));
        config.setSharedSecret(sharedsecret);
        config.read(props);
        broker = new Broker(config, log, new TasksHeap(taskheapsize, mapper));
        broker.setAuthenticationManager(new SingleUserAuthenticationManager(adminuser, adminpassword));
        broker.setBrokerId(id);
        broker.setExternalProcessChecker(() -> {
            pidFileLocker.check();
            return null;
        });
        broker.start();

        System.out.println("Listening for workers connections on " + host + ":" + port + " ssl=" + ssl);
        this.server = new NettyChannelAcceptor(broker.getAcceptor());
        this.server.setWorkerThreads(workerthreads);
        server.setHost(host);
        server.setPort(port);
        server.setSsl(ssl);
        if (!certfile.isEmpty()) {
            server.setSslCertFile(new File(certfile));
        }
        if (!certchainfile.isEmpty()) {
            server.setSslCertChainFile(new File(certchainfile));
        }
        if (certpassword != null) {
            server.setSslCertPassword(certpassword);
        }
        if (sslciphers != null && !sslciphers.isEmpty()) {
            server.setSslCiphers(Stream.of(sslciphers.split(",")).map(s -> s.trim()).filter(s -> !s.isEmpty()).collect(Collectors.toList()));
        }
        server.start();

        httpserver = new Server(new InetSocketAddress(httphost, httpport));
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        httpserver.setHandler(contexts);

        ServletContextHandler context = new ServletContextHandler();
        context.insertHandler(new GzipHandler());
        context.setContextPath("/");
        ServletHolder jerseyServlet = new ServletHolder(new StandaloneHttpAPIServlet());
        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, httppath);
        contexts.addHandler(context);

        File webUi = new File("web/ui");
        if (webUi.isDirectory()) {
            WebAppContext webApp = new WebAppContext(new File("web/ui").getAbsolutePath(), "/ui");
            contexts.addHandler(webApp);
        } else {
            System.out.println("Cannot find " + webUi.getAbsolutePath() + " directory. Web UI will not be deployed");
        }

        System.out.println("Listening for client (http) connections on " + httphost + ":" + httpport);
        System.out.println("Base client url: " + clientapiurl);
        System.out.println("Web Interface: " + uiurl);
        httpserver.start();
        System.out.println("Broker starter");
    }

    public void waitForLeadership() throws Exception {
        for (int i = 0; i < 100; i++) {
            System.out.println("Waiting for leadership");
            if (broker.isWritable()) {
                return;
            }
            Thread.sleep(1000);
        }
    }

}

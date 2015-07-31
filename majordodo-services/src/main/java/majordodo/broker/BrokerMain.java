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
import majordodo.task.GroupMapperFunction;
import majordodo.task.MemoryCommitLog;
import majordodo.task.StatusChangesLog;
import majordodo.task.TasksHeap;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;

import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import majordodo.replication.ReplicatedCommitLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Created by enrico.olivelli on 23/03/2015.
 */
public class BrokerMain implements AutoCloseable {

    private Broker broker;
    private Server httpserver;
    private NettyChannelAcceptor server;
    private final Properties configuration;

    private static BrokerMain runningInstance;

    public Broker getBroker() {
        return broker;
    }

    public BrokerMain(Properties configuration) {
        this.configuration = configuration;
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
        running.countDown();
    }

    public static void main(String... args) {
        try {
            Properties configuration = new Properties();
            File configFile;
            if (args.length > 0) {
                configFile = new File(args[0]);
                try (FileReader reader = new FileReader(configFile)) {
                    configuration.load(reader);
                }
            } else {
                configFile = new File("conf/broker.properties");
                if (configFile.isFile()) {
                    try (FileReader reader = new FileReader(configFile)) {
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
        String host = configuration.getProperty("broker.host", "127.0.0.1");
        int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
        String httphost = configuration.getProperty("broker.http.host", "0.0.0.0");
        int httpport = Integer.parseInt(configuration.getProperty("broker.http.port", "7364"));
        int taskheapsize = Integer.parseInt(configuration.getProperty("broker.tasksheap.size", "1000000"));
        String assigner = configuration.getProperty("tasks.groupmapper", "");
        String clusteringmode = configuration.getProperty("clustering.mode", "singleserver");
        System.out.println("Starting MajorDodo Broker");
        GroupMapperFunction mapper;
        if (assigner.isEmpty()) {
            mapper = new DefaultGroupMapperFunction();
        } else {
            mapper = (GroupMapperFunction) Class.forName(assigner).newInstance();
            System.out.println("GroupMapperFunction Mapper:" + mapper);
        }

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

                ReplicatedCommitLog _log = new ReplicatedCommitLog(zkAddress, zkSessionTimeout, zkPath, Paths.get(snapdir), Broker.formatHostdata(host, port));
                log = _log;
                int ensemble = Integer.parseInt(configuration.getProperty("bookeeper.ensemblesize", _log.getEnsemble() + ""));
                int writeQuorumSize = Integer.parseInt(configuration.getProperty("bookeeper.writequorumsize", _log.getWriteQuorumSize() + ""));
                int ackQuorumSize = Integer.parseInt(configuration.getProperty("bookeeper.ackquorumsize", _log.getAckQuorumSize() + ""));
                long ledgersRetentionPeriod = Long.parseLong(configuration.getProperty("bookeeper.ledgersretentionperiod", _log.getLedgersRetentionPeriod() + ""));
                _log.setAckQuorumSize(ackQuorumSize);
                _log.setEnsemble(ensemble);
                _log.setLedgersRetentionPeriod(ledgersRetentionPeriod);
                _log.setWriteQuorumSize(writeQuorumSize);
                break;
            }
            default:
                throw new RuntimeException("bad value for clustering.mode property, only valid values are singleserver|clustered");
        }

        BrokerConfiguration config = new BrokerConfiguration();
        Map<String, Object> props = new HashMap<>();
        configuration.keySet().forEach(k -> props.put(k.toString(), configuration.get(k)));
        config.read(props);
        broker = new Broker(config, log, new TasksHeap(taskheapsize, mapper));
        broker.start();

        System.out.println("Listening for workers connections on " + host + ":" + port);
        this.server = new NettyChannelAcceptor(broker.getAcceptor());
        server.setHost(host);
        server.setPort(port);
        server.start();

        httpserver = new Server(new InetSocketAddress(httphost, httpport));
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        httpserver.setHandler(context);
        ServletHolder jerseyServlet = new ServletHolder(new StandaloneHttpAPIServlet());
        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, "/majordodo");
        System.out.println("Listening for client (http) connections on " + httphost + ":" + httpport);
        httpserver.start();
        System.out.println("Broker starter");
    }

}

package dodo.broker;

import dodo.broker.http.HttpAPI;
import dodo.task.FileCommitLog;
import dodo.task.GroupMapperFunction;
import dodo.task.MemoryCommitLog;
import dodo.task.StatusChangesLog;
import dodo.task.TasksHeap;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.task.Broker;
import dodo.task.BrokerConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;

/**
 * Created by enrico.olivelli on 23/03/2015.
 */
public class BrokerMain implements AutoCloseable {

    private Broker broker;
    private Server httpserver;
    private NettyChannelAcceptor server;
    private final Properties configuration;

    public static BrokerMain runningInstance;

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
        if (BrokerMain.runningInstance == null) {
            // TODO: used by ClientAPI for tests
            BrokerMain.runningInstance = this;
        }
        String host = configuration.getProperty("broker.host", "127.0.0.1");
        int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
        String httphost = configuration.getProperty("broker.http.host", "0.0.0.0");
        int httpport = Integer.parseInt(configuration.getProperty("broker.http.port", "7364"));
        int taskheapsize = Integer.parseInt(configuration.getProperty("tasksheap.size", "1000000"));
        String assigner = configuration.getProperty("tasks.groupmapper", "");
        System.out.println("Starting MajorDodo Broker");
        GroupMapperFunction mapper;
        if (assigner.isEmpty()) {
            mapper = new GroupMapperFunction() {

                @Override
                public int getGroup(long taskid, String taskType, String assignerData) {
                    if (assignerData == null || assignerData.isEmpty()) {
                        return 0;
                    } else {
                        try {
                            return Integer.parseInt(assignerData);
                        } catch (NumberFormatException err) {
                            return 0;
                        }
                    }
                }

            };
        } else {
            mapper = (GroupMapperFunction) Class.forName(assigner).newInstance();
            System.out.println("GroupMapperFunction Mapper:" + mapper);
        }

        String logtype = configuration.getProperty("logtype", "file");
        StatusChangesLog log;
        switch (logtype) {
            case "file":
                String logsdir = configuration.getProperty("logs.dir", "txlog");
                String snapdir = configuration.getProperty("data.dir", "data");
                log = new FileCommitLog(Paths.get(snapdir), Paths.get(logsdir));
                break;
            case "mem":
                log = new MemoryCommitLog();
                break;
            default:
                throw new RuntimeException("bad value for logtype property, only valid values are file|mem");
        }
        BrokerConfiguration config = new BrokerConfiguration();
        broker = new Broker(config, new MemoryCommitLog(), new TasksHeap(taskheapsize, mapper));

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
        ServletHolder jerseyServlet = new ServletHolder(new org.glassfish.jersey.servlet.ServletContainer());
        jerseyServlet.setInitParameter(ServerProperties.PROVIDER_CLASSNAMES, HttpAPI.class.getCanonicalName());
        jerseyServlet.setInitOrder(0);
        context.addServlet(jerseyServlet, "/*");
        System.out.println("Listening for client (http) connections on " + httphost + ":" + httpport);
        httpserver.start();
        System.out.println("Broker starter");
    }

}

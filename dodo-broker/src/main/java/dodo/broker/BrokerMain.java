package dodo.broker;

import dodo.broker.http.HttpAPI;
import dodo.clustering.MemoryCommitLog;
import dodo.clustering.TasksHeap;
import dodo.clustering.TenantMapperFunction;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.task.Broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
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

            try (BrokerMain main = new BrokerMain(configuration)) {
                main.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Type ENTER to exit...");
                reader.readLine();
                System.out.println("Shutting down");
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void start() throws Exception {
        runningInstance = this;
        String host = configuration.getProperty("broker.host", "127.0.0.1");
        int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
        String httphost = configuration.getProperty("broker.http.host", "0.0.0.0");
        int httpport = Integer.parseInt(configuration.getProperty("broker.http.port", "7364"));
        int taskheapsize = Integer.parseInt(configuration.getProperty("tasksheap.size", "1000000"));
        String assigner = configuration.getProperty("tasks.tenantmapper", "");
        System.out.println("Starting MajorDodo Broker");
        TenantMapperFunction mapper;
        if (assigner.isEmpty()) {
            mapper = new TenantMapperFunction() {

                @Override
                public int getActualTenant(long taskid, String assignerData) {
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
            mapper = (TenantMapperFunction) Class.forName(assigner).newInstance();
            System.out.println("Teant Mapper:" + mapper);
        }

        broker = new Broker(new MemoryCommitLog(), new TasksHeap(taskheapsize, mapper));

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

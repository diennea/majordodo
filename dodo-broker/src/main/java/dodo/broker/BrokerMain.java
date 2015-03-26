package dodo.broker;

import dodo.broker.http.HttpAPI;
import dodo.clustering.MemoryCommitLog;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.task.Broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Properties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Created by enrico.olivelli on 23/03/2015.
 */
public class BrokerMain {

    public static Broker broker;
    
    public static void main(String... args) {
        try {
            Properties configuration = new Properties();
            File configFile;
            if (args.length > 0) {
                configFile = new File(args[0]);
            } else {
                configFile = new File("conf/broker.properties");
            }
            try (FileReader reader = new FileReader(configFile)) {
                configuration.load(reader);
            }
            String host = configuration.getProperty("broker.host", "127.0.0.1");
            int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
            String httphost = configuration.getProperty("broker.http.host", "0.0.0.0");
            int httpport = Integer.parseInt(configuration.getProperty("broker.http.port", "7364"));
            System.out.println("Starting MajorDodo Broker");
            broker = new Broker(new MemoryCommitLog());
            broker.start();
            System.out.println("Listening for workers connections on " + host + ":" + port);
            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.setHost(host);
                server.setPort(port);
                server.start();
                Server httpserver = new Server(new InetSocketAddress(httphost, httpport));
                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
                httpserver.setHandler(context);
                context.setContextPath("/");
                ServletHolder jerseyServlet = context.addServlet(
                        org.glassfish.jersey.servlet.ServletContainer.class, "/*");
                jerseyServlet.setInitOrder(0);

                // Tells the Jersey Servlet which REST service/class to load.
                jerseyServlet.setInitParameter("jersey.config.server.provider.classnames",
                        HttpAPI.class.getCanonicalName());

                System.out.println("Listening for client (http) connections on " + httphost + ":" + httpport);
                httpserver.start();
                try {
                    httpserver.join();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    System.out.println("Type ENTER to exit...");
                    reader.readLine();
                    System.out.println("Shitting down");
                } finally {
                    httpserver.stop();
                }
            }
            broker.stop();

        } catch (Throwable throwable) {
            throwable.printStackTrace();
            System.exit(0);
        }
    }

}

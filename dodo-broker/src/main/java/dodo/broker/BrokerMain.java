package dodo.broker;

import dodo.clustering.MemoryCommitLog;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.task.Broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by enrico.olivelli on 23/03/2015.
 */
public class BrokerMain {

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
            String host = configuration.getProperty("broker.host","127.0.0.1");
            int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
            System.out.println("Starting MajorDodo Broker");
            Broker broker = new Broker(new MemoryCommitLog());
            broker.start();
            System.out.println("Listening for workers connections on "+host+":"+port);
            try (NettyChannelAcceptor server = new NettyChannelAcceptor(broker.getAcceptor());) {
                server.setHost(host);
                server.setPort(port);
                server.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Type ENTER to exit...");
                reader.readLine();
                System.out.println("Shitting down");
            }
            broker.stop();

        } catch (Throwable throwable) {
            throwable.printStackTrace();
            System.exit(0);
        }
    }
}

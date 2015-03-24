package dodo.worker;

import dodo.clustering.MemoryCommitLog;
import dodo.network.BrokerLocator;
import dodo.network.netty.NettyBrokerLocator;
import dodo.network.netty.NettyChannelAcceptor;
import dodo.task.Broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class WorkerMain {
    public static void main(String... args) {
        try {
            Properties configuration = new Properties();
            File configFile;
            if (args.length > 0) {
                configFile = new File(args[0]);
            } else {
                configFile = new File("conf/worker.properties");
            }
            try (FileReader reader = new FileReader(configFile)) {
                configuration.load(reader);
            }
            // TODO: location with ZK
            String host = configuration.getProperty("broker.host","127.0.0.1");
            int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));

            String workerid = configuration.getProperty("worker.id","localhost");
            int maxthreads = Integer.parseInt(configuration.getProperty("worker.maxthreads","100"));
            String processid = ManagementFactory.getRuntimeMXBean().getName();
            String location = InetAddress.getLocalHost().getCanonicalHostName();
            Map<String, Integer> maximumThreadPerTag = new HashMap<>();
            for (Object key : configuration.keySet()) {
                String k = key.toString();
                if (k.startsWith("tag.")) {
                    String tag = k.substring(4);
                    int maxThreadPerTag = Integer.parseInt(configuration.getProperty(key+""));
                    maximumThreadPerTag.put(tag,maxThreadPerTag);
                }
            }
            BrokerLocator brokerLocator = new NettyBrokerLocator(host,port);
            System.out.println("Starting MajorDodo Worker, workerid="+workerid);
            WorkerStatusListener listener = new WorkerStatusListener() {
                @Override
                public void connectionEvent(String event, WorkerCore core) {
                    System.out.println("connectionEvent:"+event);
                }
            };
            WorkerCore workerCore = new WorkerCore(maxthreads,processid,workerid,
                    location, maximumThreadPerTag,brokerLocator,  listener);
            workerCore.start();
            System.out.println("Started worker, broker is at " + host + ":" + port + " maxthread " + maxthreads + " maxThreadPerTag:" + maximumThreadPerTag);
            System.out.println("WorkerID:"+workerid+", processid:"+processid+" location:"+location);
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Type ENTER to exit...");
            reader.readLine();
            workerCore.stop();

        } catch (Throwable throwable) {
            throwable.printStackTrace();
            System.exit(0);
        }
    }
}

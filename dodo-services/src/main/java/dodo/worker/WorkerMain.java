package dodo.worker;

import dodo.clustering.Task;
import dodo.executors.TaskExecutorFactory;
import dodo.network.BrokerLocator;
import dodo.network.netty.NettyBrokerLocator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
            String host = configuration.getProperty("broker.host", "127.0.0.1");
            int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));

            String workerid = configuration.getProperty("worker.id", "localhost");
            String groups = configuration.getProperty("worker.groups", Task.GROUP_ANY + "");
            int maxthreads = Integer.parseInt(configuration.getProperty("worker.maxthreads", "100"));
            int tasksRequestTimeout = Integer.parseInt(configuration.getProperty("worker.tasksrequesttimeout", "60000"));
            String executorFactory = configuration.getProperty("worker.executorfactory", "dodo.worker.DefaultExecutorFactory");
            String processid = ManagementFactory.getRuntimeMXBean().getName();
            String location = InetAddress.getLocalHost().getCanonicalHostName();
            Map<String, Integer> maximumThreadPerTaskType = new HashMap<>();
            boolean notag = true;
            for (Object key : configuration.keySet()) {
                String k = key.toString();
                if (k.startsWith("tasktype.")) {
                    String tag = k.substring(9);
                    notag = false;
                    int maxThreadPerTag = Integer.parseInt(configuration.getProperty(key + ""));
                    maximumThreadPerTaskType.put(tag, maxThreadPerTag);
                }
            }
            if (notag) {
                System.out.println("No configuration line tag.xxx found, defaulting to tasktype 'any', with max threads = 100");
                maximumThreadPerTaskType.put(Task.TASKTYPE_ANY, 100);
            }
            BrokerLocator brokerLocator = new NettyBrokerLocator(host, port);
            System.out.println("Starting MajorDodo Worker, workerid=" + workerid);
            WorkerStatusListener listener = new WorkerStatusListener() {
                @Override
                public void connectionEvent(String event, WorkerCore core) {
                    System.out.println("connectionEvent:" + event);
                }
            };
            List<Integer> groupsList = new ArrayList<>();
            for (String s : groups.split(",")) {
                if (!s.trim().isEmpty()) {
                    groupsList.add(Integer.parseInt(s));
                }
            }
            WorkerCoreConfiguration config = new WorkerCoreConfiguration();
            config.setMaxThreads(maxthreads);
            config.setWorkerId(workerid);
            config.setMaximumThreadByTaskType(maximumThreadPerTaskType);
            config.setGroups(groupsList);
            config.setLocation(location);
            config.setTasksRequestTimeout(tasksRequestTimeout);

            WorkerCore workerCore = new WorkerCore(config, processid, brokerLocator, listener);
            System.out.println("worker.executorfactory=" + executorFactory);
            workerCore.setExecutorFactory((TaskExecutorFactory) Class.forName(executorFactory, true, Thread.currentThread().getContextClassLoader()).newInstance());
            workerCore.start();
            System.out.println("Started worker, broker is at " + host + ":" + port + " maxthread " + maxthreads + " maxThreadPerTaskType:" + maximumThreadPerTaskType + ", tenantid=" + groups);
            System.out.println("WorkerID:" + workerid + ", processid:" + processid + " location:" + location);
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

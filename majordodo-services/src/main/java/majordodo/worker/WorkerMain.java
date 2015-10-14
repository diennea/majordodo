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
package majordodo.worker;

import majordodo.task.Task;
import majordodo.executors.TaskExecutorFactory;
import majordodo.network.BrokerLocator;
import majordodo.network.netty.NettyBrokerLocator;

import java.io.File;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import majordodo.daemons.PidFileLocker;
import majordodo.replication.ZKBrokerLocator;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class WorkerMain implements AutoCloseable {

    private static WorkerMain runningInstance;
    private final Properties configuration;
    private WorkerCore workerCore;
    private final PidFileLocker pidFileLocker;

    public static void main(String... args) throws Exception {
        try {
            Properties configuration = new Properties();
            File configFile;
            if (args.length > 0) {
                configFile = new File(args[0]);
                try (FileReader reader = new FileReader(configFile)) {
                    configuration.load(reader);
                }
            } else {
                configFile = new File("conf/worker.properties");
                if (configFile.isFile()) {
                    try (FileReader reader = new FileReader(configFile)) {
                        configuration.load(reader);
                    }
                } else {
                    throw new Exception("Cannot find " + configFile.getAbsolutePath());
                }
            }
            System.out.println("Configuration:" + configuration);

            Runtime.getRuntime().addShutdownHook(new Thread("ctrlc-hook") {

                @Override
                public void run() {
                    System.out.println("Ctrl-C trapped. Shutting down");
                    WorkerMain _brokerMain = runningInstance;
                    if (_brokerMain != null) {
                        _brokerMain.close();
                    }
                }

            });
            runningInstance = new WorkerMain(configuration);
            runningInstance.start();
            runningInstance.join();

        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    public WorkerMain(Properties configuration) {
        this.configuration = configuration;
        this.pidFileLocker = new PidFileLocker(Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath());
    }

    public void start() throws Exception {
        pidFileLocker.lock();
        BrokerLocator brokerLocator;
        String mode = configuration.getProperty("clustering.mode", "singleserver");
        switch (mode) {
            case "singleserver":
                String host = configuration.getProperty("broker.host", "localhost");
                int port = Integer.parseInt(configuration.getProperty("broker.port", "7363"));
                boolean ssl = Boolean.parseBoolean(configuration.getProperty("broker.ssl", "true"));
                brokerLocator = new NettyBrokerLocator(host, port, ssl);
                break;
            case "clustered":
                String zkAddress = configuration.getProperty("zk.address", "localhost:1281");
                int zkSessionTimeout = Integer.parseInt(configuration.getProperty("zk.sessiontimeout", "40000"));
                String zkPath = configuration.getProperty("zk.path", "/majordodo");
                brokerLocator = new ZKBrokerLocator(zkAddress, zkSessionTimeout, zkPath);
                break;
            default:
                throw new RuntimeException("invalid clustering.mode=" + mode);
        }

        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        String workerid = configuration.getProperty("worker.id", hostname);
        String sharedsecret = configuration.getProperty("sharedsecret", "dodo");
        if (workerid.isEmpty()) {
            workerid = hostname;
        }
        String groups = configuration.getProperty("worker.groups", Task.GROUP_ANY + "");
        String executorFactory = configuration.getProperty("worker.executorfactory", "majordodo.worker.DefaultExecutorFactory");
        String processid = ManagementFactory.getRuntimeMXBean().getName();
        String location = InetAddress.getLocalHost().getCanonicalHostName();
        Map<String, Integer> maximumThreadPerTaskType = new HashMap<>();
        boolean notag = true;
        for (Object key : configuration.keySet()) {
            String k = key.toString();
            if (k.startsWith("tasktype.") && k.endsWith(".maxthreads")) {
                String tasktype = k.replace("tasktype.", "").replace(".maxthreads", "");
                notag = false;
                int maxThreadPerTag = Integer.parseInt(configuration.getProperty(key + ""));
                maximumThreadPerTaskType.put(tasktype, maxThreadPerTag);
            }
        }
        if (notag) {
            System.out.println("No configuration line tasktype.xxx.maxthreads found, defaulting to tasktype 'any', with max threads = 100");
            maximumThreadPerTaskType.put(Task.TASKTYPE_ANY, 100);
        }
        int maxthreads = Integer.parseInt(configuration.getProperty("worker.maxthreads", "100"));
        System.out.println("Starting MajorDodo Worker, workerid=" + workerid);
        WorkerStatusListener listener = new WorkerStatusListener() {
            @Override
            public void connectionEvent(String event, WorkerCore core) {
                System.out.println("ConnectionEvent:" + event);
            }
        };
        List<Integer> groupsList = new ArrayList<>();
        for (String s : groups.split(",")) {
            if (!s.trim().isEmpty()) {
                groupsList.add(Integer.parseInt(s));
            }
        }
        WorkerCoreConfiguration config = new WorkerCoreConfiguration();
        config.setSharedSecret(sharedsecret);
        config.setMaxThreads(maxthreads);
        config.setWorkerId(workerid);
        config.setMaxThreadsByTaskType(maximumThreadPerTaskType);
        config.setGroups(groupsList);
        config.setLocation(location);
        Map<String, Object> props = new HashMap<>();
        configuration.keySet().forEach(k -> props.put(k.toString(), configuration.get(k)));
        config.read(props);

        workerCore = new WorkerCore(config, processid, brokerLocator, listener);
        workerCore.setExecutorFactory((TaskExecutorFactory) Class.forName(executorFactory, true, Thread.currentThread().getContextClassLoader()).newInstance());
        workerCore.setExternalProcessChecker(() -> {
            pidFileLocker.check();
            return null;
        });
        workerCore.setKillWorkerHandler(KillWorkerHandler.SHUTDOWN_JVM);
        workerCore.start();
        System.out.println("Started worker, maxthread " + maxthreads + " maxThreadPerTaskType:" + maximumThreadPerTaskType + ", groups=" + groups);
        System.out.println("WorkerID:" + workerid + ", processid:" + processid + " location:" + location);

    }

    private final static CountDownLatch running = new CountDownLatch(1);

    public void join() {
        try {
            running.await();
        } catch (InterruptedException discard) {
        }
    }

    @Override
    public void close() {

        if (workerCore != null) {
            try {
                workerCore.stop();
            } catch (Exception ex) {
                Logger.getLogger(WorkerMain.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                workerCore = null;
            }
        }
        pidFileLocker.close();
        running.countDown();

    }
}

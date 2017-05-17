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
package majordodo.task;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import org.junit.After;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import majordodo.network.BrokerHostData;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.replication.ZKBrokerLocator;
import majordodo.replication.ZKTestEnv;
import org.apache.commons.lang.exception.ExceptionUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;



/**
 *
 * @author francesco.caliumi
 */
public abstract class BrokerTestUtils {
    
    protected static Level logLevel = Level.SEVERE;
    
    protected static final String TASKTYPE_MYTYPE = "mytype";
    protected static final String userId = "queue1";
    protected static final int group = 12345;
    protected static final String[] RESOURCES = new String[]{"resource1", "resource2"};
    
    protected Path workDir;
    protected Map<String, Integer> groupsMap = new HashMap<>();
    
    protected static boolean ignoreUnhandledExceptions;
    protected final List<Throwable> unhandledExceptions = Collections.synchronizedList(new ArrayList<>());
    
    // Single broker
    protected static boolean startBroker = false;
    protected static BrokerConfiguration brokerConfig = null;
    protected Broker broker = null;
    private StatusChangesLog brokerLog = null;
    protected NettyChannelAcceptor server = null;
    
    // Replicated brokers
    protected static boolean startReplicatedBrokers = false;
    protected ZKTestEnv zkServer = null;
    protected ZKBrokerLocator brokerLocator = null;
    
    protected static BrokerConfiguration broker1Config = null;
    protected String broker1Host = "localhost";
    protected int broker1Port = 7000;
    protected Broker broker1 = null;
    protected NettyChannelAcceptor server1 = null;
    
    protected static BrokerConfiguration broker2Config = null;
    protected String broker2Host = "localhost";
    protected int broker2Port = 7001;
    protected Broker broker2 = null;
    protected NettyChannelAcceptor server2 = null;
    
    @Rule
    public TemporaryFolder folderSnapshots = new TemporaryFolder();
    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();
    
    // Annotations
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface StartBroker {

    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface StartReplicatedBrokers {

    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface IgnoreUnhandledExceptions {

    }
    
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface LogLevel {
        String level() default "SEVERE";
    }
    
    @ClassRule
    public static TestRule testStarter = new TestRule() {
        @Override
        public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base, Description description) {
            try {
//                System.out.println("starting test class " + description.getClassName());
                Class clazz = Class.forName(description.getClassName(), false, Thread.currentThread().getContextClassLoader());
                
                if (clazz.isAnnotationPresent(StartBroker.class)) {
                    startBroker = true;
                }
                
                if (clazz.isAnnotationPresent(StartReplicatedBrokers.class)) {
                    startReplicatedBrokers = true;
                }
                
                if (clazz.isAnnotationPresent(IgnoreUnhandledExceptions.class)) {
                    ignoreUnhandledExceptions = true;
                }
                
                if (clazz.isAnnotationPresent(LogLevel.class)) {
                    LogLevel l = (LogLevel) clazz.getAnnotation(LogLevel.class);
                    logLevel = Level.parse(l.level());
                }
                
                return base;
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }
    };
    
    @BeforeClass
    public static void brokerTestUtilsBeforeClass() throws Exception {
        if (startBroker) {
            brokerConfig = new BrokerConfiguration();
        }
        if (startReplicatedBrokers) {
            broker1Config = new BrokerConfiguration();
            broker2Config = new BrokerConfiguration();
        }
    }
    
    @Before
    public void brokerTestUtilsBefore() throws Exception {
        // Setup exception handler
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
                unhandledExceptions.add(e);
            }
        });
        
        // Setup Logger
        System.out.println("Setup logger to level "+logLevel.getName());
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(logLevel);
        ch.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return "" + new java.sql.Timestamp(record.getMillis()) + " " + record.getLevel() + " [" + getThreadName(record.getThreadID()) + "<" + record.getThreadID() + ">] " + record.getLoggerName() + ": " + formatMessage(record) + "\n";
            }
        });
        java.util.logging.Logger.getLogger("").setLevel(logLevel);
        java.util.logging.Logger.getLogger("").addHandler(ch);
        
        // Initialize groupsMap
        groupsMap.clear();
        groupsMap.put(userId, group);
        
        // Setup workdir
        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime()); 
        
        if (startBroker) {
            broker = new Broker(
                    brokerConfig, 
                    new FileCommitLog(workDir, workDir, 1024 * 1024), 
                    new TasksHeap(1000, createTaskPropertiesMapperFunction())
            );
            broker.startAsWritable();
                
            server = new NettyChannelAcceptor(broker.getAcceptor());
            server.start();
        }
        if (startReplicatedBrokers) {
            zkServer = new ZKTestEnv(folderZk.getRoot().toPath());
            zkServer.startBookie();
            
            // Broker 1
            broker1 = new Broker(
                broker1Config, 
                new ReplicatedCommitLog(
                    zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(),
                    folderSnapshots.newFolder().toPath(),
                    BrokerHostData.formatHostdata(new BrokerHostData(broker1Host, broker1Port, "", false, null)),
                    false),
                new TasksHeap(1000, createTaskPropertiesMapperFunction()));
        
            broker1.startAsWritable();
            
            server1 = new NettyChannelAcceptor(broker1.getAcceptor(), broker1Host, broker1Port);
            server1.start();
            
            // Broker 2
            broker2 = new Broker(
                broker2Config, 
                new ReplicatedCommitLog(
                    zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath(),
                    folderSnapshots.newFolder().toPath(),
                    BrokerHostData.formatHostdata(new BrokerHostData(broker2Host, broker2Port, "", false, null)),
                    false),
                new TasksHeap(1000, createTaskPropertiesMapperFunction()));
            
            broker2.start();
            
            server2 = new NettyChannelAcceptor(broker2.getAcceptor(), broker2Host, broker2Port);
            server2.start();
            
            // Broker locator
            brokerLocator = new ZKBrokerLocator(zkServer.getAddress(), zkServer.getTimeout(), zkServer.getPath());
        }
        
    }
    
    @After
    public void brokerTestUtilsAfter() throws Exception {
        if (startBroker) {
            server.close();
            broker.close();
            server = null;
            broker = null;
        }
        if (startReplicatedBrokers) {
            brokerLocator.close();
            server2.close();
            broker2.close();
            server1.close();
            broker1.close();
            zkServer.close();
            brokerLocator = null;
            server2 = null;
            broker2 = null;
            server1 = null;
            broker1 = null;
            zkServer = null;
        }
        
        // Resetting brokers config
        if (startBroker) {
            brokerConfig = new BrokerConfiguration();
        }
        if (startReplicatedBrokers) {
            broker1Config = new BrokerConfiguration();
            broker2Config = new BrokerConfiguration();
        }
        
        if (workDir != null) {
            Files.walkFileTree(workDir, new FileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

            });
        }
        
        if (!ignoreUnhandledExceptions && !unhandledExceptions.isEmpty()) {
            System.out.println("Errors occurred during excecution:");
            for (Throwable e: unhandledExceptions) {
                System.out.println("\n" + ExceptionUtils.getStackTrace(e) + "\n");
            }
            fail("There are "+unhandledExceptions.size()+" unhandled exceptions!");
        }
    }
    
    protected void restartSingleBroker(boolean badly) throws Exception {
        if (!startBroker) {
            throw new IllegalStateException("Not in single broker mode");
        }
        
        if (badly) {
            broker.die();
        }
        
        server.close();
        broker.close();
        
        broker = new Broker(
                brokerConfig, 
                new FileCommitLog(workDir, workDir, 1024 * 1024), 
                new TasksHeap(1000, createTaskPropertiesMapperFunction())
        );
        broker.startAsWritable();

        server = new NettyChannelAcceptor(broker.getAcceptor());
        server.start();
    }
    
    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return new TaskPropertiesMapperFunction() {
            @Override
            public TaskProperties getTaskProperties(long taskid, String taskType, String userid) {
                int group1 = groupsMap.getOrDefault(userid, 0);
                return new TaskProperties(group1, RESOURCES);
            }
        };
    }
    
    public static String getThreadName(long threadid) {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getId() == threadid) {
                return t.getName();
            }
        }
        return null;
    }
    
    public static void checkResourcesUsage(Broker broker, String workerId, String[] resources, int expectedVal) {
        // a little dirty, this function should be called inside TasksHeap writeLock
        broker.getGlobalResourceUsageCounters().updateResourceCounters();
                    
        Map<String, Integer> countersGlobal = broker.getGlobalResourceUsageCounters().getCountersView();
        for (String resource : Arrays.asList(resources)) {
            System.out.println("Global counter resource=" + resource + "; counter=" + countersGlobal.get(resource));
            if (expectedVal > 0) {
                assertNotNull(countersGlobal.get(resource));
            }
            if (countersGlobal.get(resource) != null) {
                assertEquals(expectedVal, countersGlobal.get(resource).intValue());
            }
        }
        
        if (workerId != null) {
            broker.getWorkers().getWorkerManager(workerId).getResourceUsageCounters().updateResourceCounters();
            
            Map<String, Integer> countersWorker = broker.getWorkers().getWorkerManager(workerId)
                .getResourceUsageCounters().getCountersView();
            for (String resource : Arrays.asList(resources)) {
                System.out.println("Worker "+workerId+" counter resource=" + resource + "; counter=" + countersWorker.get(resource));
                if (expectedVal > 0) {
                    assertNotNull(countersWorker.get(resource));
                }
                if (countersWorker.get(resource) != null) {
                    assertEquals(expectedVal, countersWorker.get(resource).intValue());
                }
            }
        }
    }
    
    public static void checkTaskStatus(Broker broker, long taskId, int status, Object expectedResult, long waitTime, boolean checkTaskExistence) throws Exception {
        long waitForCycle = 1000;
        long cycles = waitTime / waitForCycle;
        
        boolean ok = false;
        for (int i = 0; i < cycles; i++) {
            Task task = broker.getBrokerStatus().getTask(taskId);
            System.out.println("task:" + task);
            if (checkTaskExistence) {
                assertNotEquals(null, task);
            }
            if (task != null && (status == -1 || task.getStatus() == status)) {
                ok = true;
                if (expectedResult != null) {
                    assertEquals(expectedResult, task.getResult());
                }
                break;
            }
            Thread.sleep(waitForCycle);
        }
        assertTrue(ok);
    }
    
    public static void waitForBrokerToBecomeLeader(Broker broker, long waitTime) throws InterruptedException {
        long waitForCycle = 1000;
        long cycles = waitTime / waitForCycle;
        
        boolean ok = false;
        for (int i = 0; i < cycles; i++) {
            if (broker.isWritable()) {
                ok = true;
                break;
            }
            Thread.sleep(waitForCycle);
        }
        assertTrue(ok);
    }
}

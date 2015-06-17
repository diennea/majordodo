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
package dodo.task;

import dodo.client.ClientFacade;
import dodo.clustering.MemoryCommitLog;
import dodo.clustering.TasksHeap;
import dodo.clustering.GroupMapperFunction;
import dodo.clustering.StatusChangesLog;
import dodo.network.BrokerLocator;
import dodo.network.jvm.JVMBrokerLocator;
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
import java.util.logging.SimpleFormatter;
import org.junit.After;
import org.junit.Before;

/**
 * Basic Utils to have a broker
 *
 * @author enrico.olivelli
 */
public abstract class BasicBrokerEnv {

    protected Broker broker;
    private BrokerLocator locator;
    protected Path workDir;

    private void setupWorkdir() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());
        System.out.println("SETUPWORKDIR:" + workDir);
    }

//    @After
    public void deleteWorkdir() throws Exception {
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

    }

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.SEVERE;
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
            }
        });
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        SimpleFormatter f = new SimpleFormatter();
        ch.setFormatter(f);
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }

    public BrokerLocator getBrokerLocator() {
        if (locator == null) {
            locator = createBrokerLocator();
        }
        return locator;
    }

    public ClientFacade getClient() {
        return broker.getClient();
    }

    protected BrokerLocator createBrokerLocator() {
        return new JVMBrokerLocator(broker);
    }

    protected StatusChangesLog createStatusChangesLog() {
        return new MemoryCommitLog();
    }

    protected GroupMapperFunction createGroupMapperFunction() {
        return new GroupMapperFunction() {

            @Override
            public int getGroup(long taskid, int tasktype, String userid) {
                return groupsMap.getOrDefault(userid, 0);

            }
        };
    }

    protected int getTasksHeapsSize() {
        return 1000;
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    @Before
    public void startBroker() throws Exception {
        setupWorkdir();
        broker = new Broker(createStatusChangesLog(), new TasksHeap(getTasksHeapsSize(), createGroupMapperFunction()));
        broker.start();
    }

    @After
    public void stopBroker() {
        if (locator != null) {
            locator.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }
}

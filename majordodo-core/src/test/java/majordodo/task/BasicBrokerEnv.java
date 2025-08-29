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
import majordodo.clientfacade.ClientFacade;
import majordodo.network.BrokerLocator;
import majordodo.network.jvm.JVMBrokerLocator;
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

    protected void beforeStartBroker() throws Exception {
    }

    protected void afterStartBroker() throws Exception {
    }

    private void setupWorkdir() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());

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

    public BrokerLocator getBrokerLocator() throws Exception {
        if (locator == null) {
            locator = createBrokerLocator();
        }
        return locator;
    }

    public ClientFacade getClient() {
        return broker.getClient();
    }

    protected BrokerLocator createBrokerLocator() throws Exception {
        return new JVMBrokerLocator(broker.getBrokerId());
    }

    protected StatusChangesLog createStatusChangesLog() throws Exception {
        return new MemoryCommitLog();
    }

    protected GlobalResourceLimitsConfiguration createGlobalResourceLimitsConfiguration() throws Exception {
        return new NoLimitsGlobalResourceLimitsConfiguration();
    }

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            String[] resources = resourcesMap.getOrDefault(userid, null);
            return new TaskProperties(group1, resources);
        };
    }

    protected int getTasksHeapsSize() {
        return 1000;
    }

    private Map<String, Integer> groupsMap = new HashMap<>();
    private Map<String, String[]> resourcesMap = new HashMap<>();

    protected void declareResourcesForUser(String userId, String[] resources) {
        resourcesMap.put(userId, resources);
    }

    protected void declareGroupForUser(String userId, int group) {
        groupsMap.put(userId, group);
    }

    protected BrokerConfiguration createBrokerConfiguration() {
        return new BrokerConfiguration();
    }

    @Before
    public void startBroker() throws Exception {
        groupsMap.clear();
        resourcesMap.clear();
        setupWorkdir();
        beforeStartBroker();
        broker = new Broker(createBrokerConfiguration(), createStatusChangesLog(), new TasksHeap(getTasksHeapsSize(), createTaskPropertiesMapperFunction()));
        broker.setGlobalResourceLimitsConfiguration(createGlobalResourceLimitsConfiguration());
        broker.startAsWritable();
        afterStartBroker();
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

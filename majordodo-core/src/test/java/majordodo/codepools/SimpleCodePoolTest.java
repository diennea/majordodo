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
package majordodo.codepools;

import majordodo.task.*;
import majordodo.clientfacade.SubmitTaskResult;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.CreateCodePoolRequest;
import majordodo.clientfacade.CreateCodePoolResult;
import org.junit.After;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Basic tests for codepools
 *
 * @author enrico.olivelli
 */
public class SimpleCodePoolTest {

    protected Path workDir;

    @After
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

    protected TaskPropertiesMapperFunction createTaskPropertiesMapperFunction() {
        return (long taskid, String taskType, String userid) -> {
            int group1 = groupsMap.getOrDefault(userid, 0);
            return new TaskProperties(group1, null);
        };
    }

    protected Map<String, Integer> groupsMap = new HashMap<>();

    private static final String TASKTYPE_MYTYPE = "mytype";
    private static final String userId = "queue1";
    private static final int group = 12345;

    @Before
    public void before() throws Exception {
        groupsMap.clear();
        groupsMap.put(userId, group);
    }

    @Test
    public void createDeleteCodePoolTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());
        
        long taskId;
        String workerId = "abc";
        String taskParams = "param";
        final String CODEPOOL = "codepool";
        byte[] mockCodePoolData = "test".getBytes();

        // startAsWritable a broker and request a task, with slot
        try (Broker broker = new Broker(new BrokerConfiguration(), new MemoryCommitLog(), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();
            CreateCodePoolResult res1 = broker.getClient().createCodePool(new CreateCodePoolRequest(CODEPOOL, System.currentTimeMillis(), 0, mockCodePoolData));
            assertTrue(res1.ok);
            CreateCodePoolResult res2 = broker.getClient().createCodePool(new CreateCodePoolRequest(CODEPOOL, System.currentTimeMillis(), 0, mockCodePoolData));
            assertFalse(res2.ok);
            CreateCodePoolResult res3 = broker.getClient().createCodePool(new CreateCodePoolRequest("", System.currentTimeMillis(), 0, mockCodePoolData));
            assertFalse(res3.ok);
            CreateCodePoolResult res4 = broker.getClient().createCodePool(new CreateCodePoolRequest(null, System.currentTimeMillis(), 0, mockCodePoolData));
            assertFalse(res4.ok);
            broker.getClient().deleteCodePool(CODEPOOL);
            CreateCodePoolResult res5 = broker.getClient().createCodePool(new CreateCodePoolRequest(CODEPOOL, System.currentTimeMillis(), 0, mockCodePoolData));
            assertTrue(res5.ok);

            SubmitTaskResult res = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, 0, 0, null, 0, CODEPOOL, null));
            taskId = res.getTaskId();
            assertTrue(taskId > 0);
            assertTrue(res.getOutcome() == null);

        }

    }

}

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

import majordodo.clientfacade.TaskStatusView;
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
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Basic tests for recovery
 *
 * @author enrico.olivelli
 */
public class TaskDeadlineWithoutRequestTest {

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
    public void deadlineOnRequestTaskTest() throws Exception {

        Path mavenTargetDir = Paths.get("target").toAbsolutePath();
        workDir = Files.createTempDirectory(mavenTargetDir, "test" + System.nanoTime());
        
        long taskId;
        String workerId = "abc";
        String taskParams = "param";

        // startAsWritable a broker and do some work
        try (Broker broker = new Broker(new BrokerConfiguration(), new FileCommitLog(workDir, workDir, 1024 * 1024), new TasksHeap(1000, createTaskPropertiesMapperFunction()));) {
            broker.startAsWritable();

            Map<String, Integer> tags = new HashMap<>();
            tags.put(TASKTYPE_MYTYPE, 1);
            taskId = broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, System.currentTimeMillis() - 1000 * 60 * 60, null, 0, null, null)).getTaskId();

            broker.purgeTasks();

            boolean okFinishedForBroker = false;
            for (int i = 0; i < 100; i++) {
                TaskStatusView task = broker.getClient().getTask(taskId);
                if (task.getStatus() == Task.STATUS_ERROR && "deadline_expired".equals(task.getResult())) {
                    okFinishedForBroker = true;
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue(okFinishedForBroker);
        }

    }

}

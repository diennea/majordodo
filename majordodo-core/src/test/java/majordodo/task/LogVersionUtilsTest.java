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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertEquals;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author francesco.caliumi
 */
//@BrokerTestUtils.StartBroker
public class LogVersionUtilsTest {
    
    public static final String V10 = "v10";
    public static final String V20 = "v20";
    
    Path mavenTargetDir = Paths.get("target").toAbsolutePath();
    
    
    private static List<StatusEdit> createEditSequence(String version) {
        switch (version) {
            case V10:
                return createEditSequenceV10();
            case V20:
                return createEditSequenceV20();
            default:
                throw new IllegalArgumentException(version);
        }
    }
    
    private static List<StatusEdit> createEditSequenceV10() {
        List<StatusEdit> edits = new ArrayList<>();
        
        edits.add(StatusEdit.NOOP());
        edits.add(StatusEdit.DELETE_CODEPOOL("codepool"));
        edits.add(StatusEdit.CREATE_CODEPOOL("codepool", 123, "payload".getBytes(), 321));
        edits.add(StatusEdit.NOOP());
        edits.add(StatusEdit.BEGIN_TRANSACTION(431, 123));
        edits.add(StatusEdit.ROLLBACK_TRANSACTION(432));
        edits.add(StatusEdit.COMMIT_TRANSACTION(433));
        edits.add(StatusEdit.ASSIGN_TASK_TO_WORKER(123, "nodeId", 12, "resources"));
        edits.add(StatusEdit.TASK_STATUS_CHANGE(234, "workerId", 4, "result"));
        edits.add(StatusEdit.ADD_TASK(12, "taskType", "taskParameter", "userid", 1, 0, 3, "slot", 4, "codePool", "mode"));
        edits.add(StatusEdit.PREPARE_ADD_TASK(51, 13, "taskType", "taskParameter", "userid", 1, 0, 3, "slot", 4, "codePool", "mode"));
        edits.add(StatusEdit.WORKER_CONNECTED("workerId", "processid", "nodeLocation", new HashSet<Long>(Arrays.asList(3L,4L)), 32));
        edits.add(StatusEdit.WORKER_DISCONNECTED("workerId", 16));
        edits.add(StatusEdit.WORKER_DIED("workerId", 45));
        
        return edits;
    }
    
    private static List<StatusEdit> createEditSequenceV20() {
        List<StatusEdit> edits = new ArrayList<>();
        
        edits.add(StatusEdit.NOOP());
        edits.add(StatusEdit.DELETE_CODEPOOL("codepool"));
        edits.add(StatusEdit.CREATE_CODEPOOL("codepool", Long.MAX_VALUE-123, "payload".getBytes(), 321));
        edits.add(StatusEdit.NOOP());
        edits.add(StatusEdit.BEGIN_TRANSACTION(431, 123));
        edits.add(StatusEdit.ROLLBACK_TRANSACTION(Long.MAX_VALUE-432));
        edits.add(StatusEdit.COMMIT_TRANSACTION(433));
        edits.add(StatusEdit.ASSIGN_TASK_TO_WORKER(Long.MAX_VALUE-123, "nodeId", 12, "resources"));
        edits.add(StatusEdit.TASK_STATUS_CHANGE(234, "workerId", Integer.MIN_VALUE+4, "result"));
        edits.add(StatusEdit.ADD_TASK(Integer.MAX_VALUE-12, "taskType", "taskParameter", "userid", 1, 744, Long.MIN_VALUE+3, "slot", 4, "codePool", "mode"));
        edits.add(StatusEdit.PREPARE_ADD_TASK(51, 13, "taskType", "taskParameter", "userid", 1, Long.MAX_VALUE-744, 3, "slot", 4, "codePool", "mode"));
        edits.add(StatusEdit.WORKER_CONNECTED("workerId", "processid", "nodeLocation", new HashSet<Long>(Arrays.asList(3L,4L)), 32));
        edits.add(StatusEdit.WORKER_DISCONNECTED("workerId", 16));
        edits.add(StatusEdit.WORKER_DIED("workerId", Long.MAX_VALUE-45));
        
        return edits;
    }
    
    /* ----------------------------------------------------- */
    
    private void generateLogFile(String version) throws Exception {
        
        Path workDir = Files.createTempDirectory(mavenTargetDir, "generateLogFile"+version.toUpperCase());
        
        List<StatusEdit> edits = createEditSequence(version);
        Path copyFile;
        Path logFile;
        
        try (FileCommitLog log = new FileCommitLog(workDir, workDir, 1024 * 1024)) {
            
            log.startWriting();

            for (StatusEdit edit: edits) {
                LogSequenceNumber seqNum = log.logStatusEdit(edit);
                System.out.println("Added entry "+seqNum+": "+edit);
            }
            
            logFile = log.getCurrentLedgerFilePath();
            copyFile = mavenTargetDir.resolve(logFile.getFileName());
        }
        Files.delete(copyFile);
        Files.copy(logFile, copyFile);

        System.out.println("VERSION "+version+" FILE: " + copyFile.toAbsolutePath());
        
    }
    
    public void testLogFile(String version) throws Exception {
        
        Path workDir = Files.createTempDirectory(mavenTargetDir, "consumeLogFile"+version.toUpperCase());
        
        File srcDir = new File("src/test/resources/majordodo/task/logversion/"+version);
        
        for (File srcFile: Arrays.asList(srcDir.listFiles())) {
            Files.copy(srcFile.toPath(), workDir.resolve(srcFile.toPath().getFileName()));
        }
        
        List<StatusEdit> edits = createEditSequence(version);
        
        try (FileCommitLog log = new FileCommitLog(workDir, workDir, 1024 * 1024)) {
            BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
            System.out.println("snapshot:" + snapshot);
            // no snapshot was taken...
            assertEquals(snapshot.getActualLogSequenceNumber().ledgerId, -1);
            assertEquals(snapshot.getActualLogSequenceNumber().sequenceNumber, -1);
            
            AtomicInteger i = new AtomicInteger(0);
            log.recovery(snapshot.getActualLogSequenceNumber(), (seqNum, edit) -> {
                System.out.println("Read entry " + seqNum + ": " + edit);
                assertEquals(edits.get(i.getAndIncrement()), edit);
            }, false);
            
            assertEquals(edits.size(), i.get());
        }
    }
    
    /* ----------------------------------------------------- */
    
    @Ignore
    @Test
    public void generateLogFileV10() throws Exception {
        generateLogFile(V10);
    }
    
    @Test
    public void testLogFileV10() throws Exception {
        testLogFile(V10);
    }
    
    /* ----------------------------------------------------- */
    
    @Ignore
    @Test
    public void generateLogFileV20() throws Exception {
        generateLogFile(V20);
    }
    
    
    @Test
    public void testLogFileV20() throws Exception {
        testLogFile(V20);
    }
    
    /* ----------------------------------------------------- */
    
}
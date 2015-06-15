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
package dodo.clustering;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Log data and snapshots are stored on the local disk. Suitable for single
 * broker setups
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends StatusChangesLog {

    Path snapshotsDirectory;
    Path logDirectory;

    public FileCommitLog(Path snapshotsDirectory, Path logDirectory) {
        this.snapshotsDirectory = snapshotsDirectory;
        this.logDirectory = logDirectory;
    }

    @Override
    public LogSequenceNumber logStatusEdit(StatusEdit edit) throws LogNotAvailableException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer) throws LogNotAvailableException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void checkpointDone(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {
        LogSequenceNumber actualLogSequenceNumber = snapshotData.getActualLogSequenceNumber();
        String filename = actualLogSequenceNumber.ledgerId + "_" + actualLogSequenceNumber.sequenceNumber;
        Path snapshotfilename = snapshotsDirectory.resolve(filename + ".json");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> snapshotdata = new HashMap<>();
        snapshotdata.put("ledgerid", actualLogSequenceNumber.ledgerId);
        snapshotdata.put("sequenceNumber", actualLogSequenceNumber.sequenceNumber);
        snapshotdata.put("maxTaskId", snapshotData.maxTaskId);
        List<Map<String, Object>> tasksStatus = new ArrayList<>();
        snapshotdata.put("tasks", tasksStatus);

        List<Map<String, Object>> workersStatus = new ArrayList<>();
        snapshotdata.put("workers", workersStatus);
        snapshotData.getWorkers().forEach(worker -> {
            Map<String, Object> workerData = new HashMap<>();
            workerData.put("workerId", worker.getWorkerId());
            workerData.put("location", worker.getWorkerLocation());
            workerData.put("processId", worker.getProcessId());
            workerData.put("lastConnectionTs", worker.getLastConnectionTs());
            workerData.put("status", worker.getStatus());
            workersStatus.add(workerData);
        });

        snapshotData.getTasks().forEach(
                task -> {
                    Map<String, Object> taskData = new HashMap<>();
                    taskData.put("id", task.getTaskId());
                    taskData.put("status", task.getStatus());
                    taskData.put("parameter", task.getParameter());
                    taskData.put("result", task.getResult());
                    taskData.put("userId", task.getUserId());
                    taskData.put("createdTimestamp", task.getCreatedTimestamp());
                    taskData.put("type", task.getType());
                    taskData.put("workerId", task.getWorkerId());
                    tasksStatus.add(taskData);
                }
        );

        try (OutputStream out = Files.newOutputStream(snapshotfilename)) {
            mapper.writeValue(out, snapshotData);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException {
        Path snapshotfilename = null;
        LogSequenceNumber latest = null;

        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(logDirectory)) {
            for (Path path : allfiles) {
                String filename = path.getFileName().toString();
                if (filename.endsWith(".json")) {
                    System.out.println("Processing snapshot file: " + path);
                    try {
                        filename = filename.substring(0, filename.length() - 5);

                        int pos = filename.indexOf('_');
                        if (pos > 0) {
                            long ledgerId = Long.parseLong(filename.substring(0, pos));
                            long sequenceNumber = Long.parseLong(filename.substring(pos + 1));
                            LogSequenceNumber number = new LogSequenceNumber(ledgerId, sequenceNumber);
                            if (latest == null || number.after(latest)) {
                                latest = number;
                                snapshotfilename = path;
                            }
                        }
                    } catch (NumberFormatException invalidName) {
                        System.out.println("Error:" + invalidName);
                    }
                }
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        if (snapshotfilename == null) {
            System.out.println("No snapshot available Starting with a brand new status");
            return new BrokerStatusSnapshot(1, new LogSequenceNumber(0, 0));
        } else {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> snapshotdata;
            try (InputStream in = Files.newInputStream(snapshotfilename)) {
                snapshotdata = mapper.readValue(in, Map.class);
                long ledgerId = Long.parseLong(snapshotdata.get("ledgerid") + "");
                long sequenceNumber = Long.parseLong(snapshotdata.get("sequenceNumber") + "");
                long maxTaskId = Long.parseLong(snapshotdata.get("maxTaskId") + "");
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
            throw new RuntimeException();
        }
//
//        Map<String, Object> snapshotdata = new HashMap<>();
//        snapshotdata.put("ledgerid", actualLogSequenceNumber.ledgerId);
//        snapshotdata.put("sequenceNumber", actualLogSequenceNumber.sequenceNumber);
//        snapshotdata.put("maxTaskId", snapshotData.maxTaskId);
//        List<Map<String, Object>> tasksStatus = new ArrayList<>();
//        snapshotdata.put("tasks", tasksStatus);
//
//        List<Map<String, Object>> workersStatus = new ArrayList<>();
//        snapshotdata.put("workers", workersStatus);
//        snapshotData.getWorkers().forEach(worker -> {
//            Map<String, Object> workerData = new HashMap<>();
//            workerData.put("workerId", worker.getWorkerId());
//            workerData.put("location", worker.getWorkerLocation());
//            workerData.put("processId", worker.getProcessId());
//            workerData.put("lastConnectionTs", worker.getLastConnectionTs());
//            workerData.put("status", worker.getStatus());
//            workersStatus.add(workerData);
//        });
//
//        snapshotData.getTasks().forEach(
//                task -> {
//                    Map<String, Object> taskData = new HashMap<>();
//                    taskData.put("id", task.getTaskId());
//                    taskData.put("status", task.getStatus());
//                    taskData.put("parameter", task.getParameter());
//                    taskData.put("result", task.getResult());
//                    taskData.put("userId", task.getUserId());
//                    taskData.put("createdTimestamp", task.getCreatedTimestamp());
//                    taskData.put("type", task.getType());
//                    taskData.put("workerId", task.getWorkerId());
//                    tasksStatus.add(taskData);
//                }
//        );
//
//        try (OutputStream out = Files.newOutputStream(snapshotfilename)) {
//            mapper.writeValue(out, snapshotData);
//        } catch (IOException err) {
//            throw new LogNotAvailableException(err);
//        }

    }

}

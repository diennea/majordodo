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

import dodo.scheduler.WorkerStatus;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Log data and snapshots are stored on the local disk. Suitable for single
 * broker setups
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends StatusChangesLog {

    private static final Logger LOGGER = Logger.getLogger(FileCommitLog.class.getName());

    Path snapshotsDirectory;
    Path logDirectory;

    long currentLedgerId = 0;
    long currentSequenceNumber = 0;

    CommitFileWriter writer;

    private final ReentrantLock writeLock = new ReentrantLock();
    private final ReentrantLock snapshotLock = new ReentrantLock();

    private final static byte ENTRY_START = 13;
    private final static byte ENTRY_END = 25;

    private class CommitFileWriter implements AutoCloseable {

        DataOutputStream out;

        private CommitFileWriter(long ledgerId) throws IOException {
            Path filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION);
            // in case of IOException the stream is not opened, not need to close it
            LOGGER.log(Level.INFO, "starting new file {0} ", filename);
            this.out = new DataOutputStream(Files.newOutputStream(filename));
        }

        public void writeEntry(long seqnumber, StatusEdit edit) throws IOException {
            byte[] serialize = edit.serialize();
            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            this.out.writeInt(serialize.length);
            this.out.write(serialize);
            this.out.writeByte(ENTRY_END);
        }

        public void flush() throws LogNotAvailableException {
            // TODO: FD.synch ??
            try {
                this.out.flush();
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        }

        public void close() throws LogNotAvailableException {
            try {
                out.close();
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        }
    }

    private static final class StatusEditWithSequenceNumber {

        LogSequenceNumber logSequenceNumber;
        StatusEdit statusEdit;

        public StatusEditWithSequenceNumber(LogSequenceNumber logSequenceNumber, StatusEdit statusEdit) {
            this.logSequenceNumber = logSequenceNumber;
            this.statusEdit = statusEdit;
        }

    }

    private class CommitFileReader implements AutoCloseable {

        DataInputStream in;
        long ledgerId;

        private CommitFileReader(long ledgerId) throws IOException {
            this.ledgerId = ledgerId;
            Path filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION);
            // in case of IOException the stream is not opened, not need to close it
            this.in = new DataInputStream(Files.newInputStream(filename, StandardOpenOption.READ));
        }

        public StatusEditWithSequenceNumber nextEntry() throws IOException {
            byte entryStart;
            try {
                entryStart = in.readByte();
            } catch (EOFException okEnd) {
                return null;
            }
            if (entryStart != ENTRY_START) {
                throw new IOException("corrupted stream");
            }
            long seqNumber = this.in.readLong();
            int len = this.in.readInt();
            byte[] data = new byte[len];
            int rr = this.in.read(data);
            if (rr != data.length) {
                throw new IOException("corrupted read");
            }
            int entryEnd = this.in.readByte();
            if (entryEnd != ENTRY_END) {
                throw new IOException("corrupted stream");
            }
            StatusEdit edit = StatusEdit.read(data);
            return new StatusEditWithSequenceNumber(new LogSequenceNumber(ledgerId, seqNumber), edit);
        }

        public void close() throws IOException {
            in.close();
        }
    }

    private void openNewLedger() throws LogNotAvailableException {
        writeLock.lock();
        try {
            currentLedgerId++;
            currentSequenceNumber = 0;
            writer = new CommitFileWriter(currentLedgerId);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        } finally {
            writeLock.unlock();
        }
    }

    public FileCommitLog(Path snapshotsDirectory, Path logDirectory) {
        this.snapshotsDirectory = snapshotsDirectory;
        this.logDirectory = logDirectory;
        LOGGER.log(Level.INFO, "snapshotdirectory:{0}, logdirectory:{1}", new Object[]{snapshotsDirectory.toAbsolutePath(), logDirectory.toAbsolutePath()});
    }

    @Override
    public LogSequenceNumber logStatusEdit(StatusEdit edit) throws LogNotAvailableException {
        writeLock.lock();
        try {
            long newSequenceNumber = ++currentSequenceNumber;
            writer.writeEntry(newSequenceNumber, edit);
            return new LogSequenceNumber(currentLedgerId, newSequenceNumber);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer) throws LogNotAvailableException {
        LOGGER.log(Level.INFO, "recovery, snapshotSequenceNumber: {0}", snapshotSequenceNumber);
        // no lock is needed, we are at boot time
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
            List<Path> names = new ArrayList<>();
            for (Path path : stream) {                
                if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(LOGFILEEXTENSION)) {                    
                    names.add(path);
                }
            }
            names.sort(Comparator.comparing(Path::toString));
            for (Path p : names) {
                LOGGER.log(Level.INFO, "logfile is {0}", p.toAbsolutePath());
                String name = p.getFileName().toString().replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name);
                if (ledgerId > currentLedgerId) {
                    currentLedgerId = ledgerId;
                }
                try (CommitFileReader reader = new CommitFileReader(ledgerId)) {
                    StatusEditWithSequenceNumber n = reader.nextEntry();
                    while (n != null) {

                        if (n.logSequenceNumber.after(snapshotSequenceNumber)) {
                            LOGGER.log(Level.INFO, "RECOVER ENTRY {0}, {1}", new Object[]{n.logSequenceNumber, n.statusEdit});
                            consumer.accept(n.logSequenceNumber, n.statusEdit);
                        } else {
                            LOGGER.log(Level.INFO, "SKIP ENTRY {0}, {1}", new Object[]{n.logSequenceNumber, n.statusEdit});
                        }
                        n = reader.nextEntry();
                    }
                }
            }
            LOGGER.log(Level.INFO, "Max ledgerId is ", new Object[]{currentLedgerId});
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        openNewLedger();
    }
    private static final String LOGFILEEXTENSION = ".txlog";

    @Override
    public void checkpoint(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {

        snapshotLock.lock();
        try {
            LogSequenceNumber actualLogSequenceNumber = snapshotData.getActualLogSequenceNumber();
            String filename = actualLogSequenceNumber.ledgerId + "_" + actualLogSequenceNumber.sequenceNumber;
            Path snapshotfilename = snapshotsDirectory.resolve(filename + SNAPSHOTFILEXTENSION);
            LOGGER.log(Level.INFO, "checkpoint, file:{0}", snapshotfilename.toAbsolutePath());
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> filedata = new HashMap<>();
            filedata.put("ledgerid", actualLogSequenceNumber.ledgerId);
            filedata.put("sequenceNumber", actualLogSequenceNumber.sequenceNumber);
            filedata.put("maxTaskId", snapshotData.maxTaskId);
            List<Map<String, Object>> tasksStatus = new ArrayList<>();
            filedata.put("tasks", tasksStatus);

            List<Map<String, Object>> workersStatus = new ArrayList<>();
            filedata.put("workers", workersStatus);
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
                mapper.writeValue(out, filedata);
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        } finally {
            snapshotLock.unlock();
        }

    }
    private static final String SNAPSHOTFILEXTENSION = ".snap.json";

    @Override
    public BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException {
        Path snapshotfilename = null;
        LogSequenceNumber latest = null;

        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(logDirectory)) {
            for (Path path : allfiles) {
                String filename = path.getFileName().toString();
                if (filename.endsWith(SNAPSHOTFILEXTENSION)) {
                    System.out.println("Processing snapshot file: " + path);
                    try {
                        filename = filename.substring(0, filename.length() - SNAPSHOTFILEXTENSION.length());

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
                        invalidName.printStackTrace();
                    }
                }
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        if (snapshotfilename == null) {
            System.out.println("No snapshot available Starting with a brand new status");
            currentLedgerId = 0;
            return new BrokerStatusSnapshot(0, new LogSequenceNumber(0, 0));
        } else {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> snapshotdata;
            try (InputStream in = Files.newInputStream(snapshotfilename)) {

                snapshotdata = mapper.readValue(in, Map.class);
                long ledgerId = Long.parseLong(snapshotdata.get("ledgerid") + "");
                long sequenceNumber = Long.parseLong(snapshotdata.get("sequenceNumber") + "");
                currentLedgerId = ledgerId;

                long maxTaskId = Long.parseLong(snapshotdata.get("maxTaskId") + "");
                BrokerStatusSnapshot result = new BrokerStatusSnapshot(maxTaskId, new LogSequenceNumber(ledgerId, sequenceNumber));
                List<Map<String, Object>> tasksStatus = (List<Map<String, Object>>) snapshotdata.get("tasks");
                if (tasksStatus != null) {
                    tasksStatus.forEach(taskData -> {
                        Task task = new Task();
                        task.setTaskId(Long.parseLong(taskData.get("id") + ""));
                        task.setStatus(Integer.parseInt(taskData.get("status") + ""));
                        task.setParameter((String) taskData.get("parameter"));
                        task.setResult((String) taskData.get("result"));
                        task.setUserId((String) taskData.get("result"));
                        task.setCreatedTimestamp(Long.parseLong(taskData.get("createdTimestamp") + ""));
                        task.setType(Integer.parseInt(taskData.get("type") + ""));
                        task.setWorkerId((String) taskData.get("workerId"));
                        result.getTasks().add(task);
                    });
                }
                List<Map<String, Object>> workersStatus = (List<Map<String, Object>>) snapshotdata.get("workers");
                if (workersStatus != null) {
                    workersStatus.forEach(w -> {
                        WorkerStatus workerStatus = new WorkerStatus();
                        workerStatus.setWorkerId((String) w.get("workerId"));
                        workerStatus.setWorkerLocation((String) w.get("location"));
                        workerStatus.setProcessId((String) w.get("processId"));
                        workerStatus.setLastConnectionTs(Long.parseLong(w.get("lastConnectionTs") + ""));
                        workerStatus.setStatus(Integer.parseInt(w.get("status") + ""));
                        result.getWorkers().add(workerStatus);
                    });
                }

                return result;
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        }
    }

    @Override
    public void close() throws LogNotAvailableException {
        if (writer != null) {
            writer.close();
        }
    }

}

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import majordodo.utils.FileUtils;

/**
 * Log data and snapshots are stored on the local disk. Suitable for single broker setups
 *
 * @author enrico.olivelli
 */
public class FileCommitLog extends StatusChangesLog {

    private static final Logger LOGGER = Logger.getLogger(FileCommitLog.class.getName());

    private final Path snapshotsDirectory;
    private final Path logDirectory;
    private LogSequenceNumber recoveredLogSequence;

    private long currentLedgerId = 0;
    private boolean writable = false;    
    private long maxLogFileSize = 1024 * 1024;
    private long writtenBytes = 0;

    private final int MAX_UNSYNCHED_BATCH = 1000;
    private final int MAX_SYNCH_TIME = 10;

    private volatile CommitFileWriter writer;
    private Thread spool;

    private final BlockingQueue<StatusEditHolderFuture> writeQueue = new LinkedBlockingQueue<>(100000);

    private final ReentrantLock snapshotLock = new ReentrantLock();

    private final static byte ENTRY_START = 13;
    private final static byte ENTRY_END = 25;

    private class CommitFileWriter implements AutoCloseable {

        final long ledgerId;
        long sequenceNumber;
        FileOutputStream fOut;
        DataOutputStream out;
        Path filename;

        private CommitFileWriter(long ledgerId, long sequenceNumber) throws IOException {
            this.ledgerId = ledgerId;
            this.sequenceNumber = sequenceNumber;
            filename = logDirectory.resolve(String.format("%016x", ledgerId) + LOGFILEEXTENSION).toAbsolutePath();
            // in case of IOException the stream is not opened, not need to close it
            LOGGER.log(Level.SEVERE, "starting new file {0} ", filename);
            File file = filename.toFile();
            if (file.isFile()) {
                throw new IOException("File " + file.getAbsolutePath() + " already exists");
            }
            this.fOut = new FileOutputStream(file);
            this.out = new DataOutputStream(
                new BufferedOutputStream(
                    this.fOut
                )
            );
            writtenBytes = 0;
        }

        public void writeEntry(long seqnumber, StatusEdit edit) throws IOException {
            byte[] serialize = edit.serialize();
            this.out.writeByte(ENTRY_START);
            this.out.writeLong(seqnumber);
            this.out.writeInt(serialize.length);
            this.out.write(serialize);
            this.out.writeByte(ENTRY_END);
            writtenBytes += (1 + 8 + 4 + serialize.length + 1);
        }

        public void synch() throws IOException {
            this.out.flush();
            this.fOut.getFD().sync();
        }

        @Override
        public void close() throws LogNotAvailableException {
            try {
                out.close();
                fOut.close();
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
        boolean lastFile;

        private CommitFileReader(long ledgerId, boolean lastFile) throws IOException {
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
            try {
                if (entryStart != ENTRY_START) {
                    throw new IOException("corrupted stream");
                }
                long seqNumber = this.in.readLong();
                int len = this.in.readInt();
                byte[] data = new byte[len];
                int rr = this.in.read(data);
                if (rr != data.length) {
                    return null;
                }
                int entryEnd = this.in.readByte();
                if (entryEnd != ENTRY_END) {
                    throw new IOException("corrupted stream");
                }
                StatusEdit edit = StatusEdit.read(data);
                return new StatusEditWithSequenceNumber(new LogSequenceNumber(ledgerId, seqNumber), edit);
            } catch (EOFException truncatedLog) {
                // if we hit EOF the entry has not been written, and so not acked, we can ignore it and say that the file is finished
                // it is important that this is the last file in the set
                if (lastFile) {
                    LOGGER.log(Level.SEVERE, "found unfinished entry in file " + this.ledgerId + ". entry was not acked. ignoring " + truncatedLog);
                    return null;
                } else {
                    throw truncatedLog;
                }
            }
        }

        public void close() throws IOException {
            in.close();
        }
    }

    private void openNewLedger() throws LogNotAvailableException {

        try {
            if (writer != null) {
                LOGGER.log(Level.SEVERE, "closing actual file {0}", writer.filename);
                writer.close();
            }
            ensureDirectories();

            writer = new CommitFileWriter(++currentLedgerId, -1);

        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public FileCommitLog(Path snapshotsDirectory, Path logDirectory, long maxLogFileSize) {
        this.maxLogFileSize = maxLogFileSize;
        this.snapshotsDirectory = snapshotsDirectory.toAbsolutePath();
        this.logDirectory = logDirectory.toAbsolutePath();
        this.spool = new Thread(new SpoolTask(), "commitlog-" + logDirectory);
        this.spool.setDaemon(true);
        LOGGER.log(Level.SEVERE, "snapshotdirectory:{0}, logdirectory:{1},maxLogFileSize {2} bytes", new Object[]{snapshotsDirectory, logDirectory, maxLogFileSize});
    }

    private class SpoolTask implements Runnable {

        @Override
        public void run() {
            try {
                openNewLedger();
                int count = 0;
                List<StatusEditHolderFuture> doneEntries = new ArrayList<>();
                while (!closed || !writeQueue.isEmpty()) {
                    StatusEditHolderFuture entry = writeQueue.poll(MAX_SYNCH_TIME, TimeUnit.MILLISECONDS);
                    boolean timedOut = false;
                    if (entry != null) {
                        writeEntry(entry);
                        doneEntries.add(entry);
                        count++;
                    } else {
                        timedOut = true;
                    }
                    if (timedOut || count >= MAX_UNSYNCHED_BATCH) {
                        count = 0;
                        if (!doneEntries.isEmpty()) {
                            synch();
                            for (StatusEditHolderFuture e : doneEntries) {
                                if (e.synch) {
                                    e.synchDone();
                                }
                            }
                            doneEntries.clear();
                        }

                    }
                }
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "general commit log failure on " + FileCommitLog.this.logDirectory);
            }
        }

    }

    private static class StatusEditHolderFuture {

        final CompletableFuture<LogSequenceNumber> ack = new CompletableFuture<>();
        final StatusEdit entry;
        LogSequenceNumber sequenceNumber;
        Throwable error;
        final boolean synch;

        public StatusEditHolderFuture(StatusEdit entry, boolean synch) {
            this.entry = entry;
            this.synch = synch;
        }

        public void error(Throwable error) {
            this.error = error;
            if (!synch) {
                synchDone();
            }
        }

        public void done(LogSequenceNumber sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            if (!synch) {
                synchDone();
            }
        }

        private void synchDone() {
            if (sequenceNumber == null && error == null) {
                throw new IllegalStateException();
            }
            if (error != null) {
                ack.completeExceptionally(error);
            } else {
                ack.complete(sequenceNumber);
            }
        }

    }

    private void writeEntry(StatusEditHolderFuture entry) {
        try {
            CommitFileWriter writer = this.writer;

            if (writer == null) {
                throw new IOException("not yet writable");
            }

            long newSequenceNumber = ++writer.sequenceNumber;
            writer.writeEntry(newSequenceNumber, entry.entry);

            if (writtenBytes > maxLogFileSize) {
                openNewLedger();
            }

            entry.done(new LogSequenceNumber(writer.ledgerId, newSequenceNumber));
        } catch (IOException | LogNotAvailableException err) {
            entry.error(err);
        }
    }

    private void synch() throws IOException {
        if (writer == null) {
            return;
        }
        writer.synch();
    }

    @Override
    public LogSequenceNumber logStatusEdit(StatusEdit edit) throws LogNotAvailableException {
        return logStatusEdit(edit, true);
    }

    private LogSequenceNumber logStatusEdit(StatusEdit edit, boolean synch) throws LogNotAvailableException {

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "log {0}", edit);
        }
        StatusEditHolderFuture future = new StatusEditHolderFuture(edit, synch);
        try {
            writeQueue.put(future);
            return future.ack.get();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new LogNotAvailableException(err);
        } catch (ExecutionException err) {
            throw new LogNotAvailableException(err.getCause());
        }
    }

    @Override
    public List<LogSequenceNumber> logStatusEditBatch(List<StatusEdit> edits) throws LogNotAvailableException {
        List<LogSequenceNumber> res = new ArrayList<>();
        int size = edits.size();
        for (int i = 0; i < size; i++) {
            boolean synchLast = i == size - 1;
            res.add(logStatusEdit(edits.get(i), synchLast));
        }
        return res;
    }

    @Override
    public boolean isWritable() {
        return writable && !closed;
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer, boolean fencing) throws LogNotAvailableException {
        LOGGER.log(Level.SEVERE, "recovery, snapshotSequenceNumber: {0}", snapshotSequenceNumber);
        // no lock is needed, we are at boot time
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
            List<Path> names = new ArrayList<>();
            for (Path path : stream) {
                if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(LOGFILEEXTENSION)) {
                    names.add(path);
                }
            }
            names.sort(Comparator.comparing(Path::toString));
            final Path last = names.isEmpty() ? null : names.get(names.size() - 1);

            for (Path p : names) {
                boolean lastFile = p.equals(last);

                LOGGER.log(Level.SEVERE, "logfile is {0}, lastFile {1}", new Object[]{p.toAbsolutePath(), lastFile});

                String name = p.getFileName().toString().replace(LOGFILEEXTENSION, "");
                long ledgerId = Long.parseLong(name, 16);
                if (ledgerId > currentLedgerId) {
                    currentLedgerId = ledgerId;
                }
                try (CommitFileReader reader = new CommitFileReader(ledgerId, lastFile)) {
                    StatusEditWithSequenceNumber n = reader.nextEntry();
                    while (n != null) {

                        if (n.logSequenceNumber.after(snapshotSequenceNumber)) {
                            LOGGER.log(Level.FINE, "RECOVER ENTRY {0}, {1}", new Object[]{n.logSequenceNumber, n.statusEdit});
                            consumer.accept(n.logSequenceNumber, n.statusEdit);
                        } else {
                            LOGGER.log(Level.FINE, "SKIP ENTRY {0}, {1}", new Object[]{n.logSequenceNumber, n.statusEdit});
                        }
                        n = reader.nextEntry();
                    }
                }
            }
            LOGGER.log(Level.SEVERE, "Max ledgerId is {0}", new Object[]{currentLedgerId});
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }

    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        spool.start();
        writable = true;
    }

    @Override
    public void clear() throws LogNotAvailableException {
        this.currentLedgerId = 0;
        try {
            FileUtils.cleanDirectory(logDirectory);
            FileUtils.cleanDirectory(snapshotsDirectory);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    private void ensureDirectories() throws LogNotAvailableException {
        try {
            if (!Files.isDirectory(snapshotsDirectory)) {
                LOGGER.log(Level.SEVERE, "directory " + snapshotsDirectory + " does not exist. creating");
                Files.createDirectories(snapshotsDirectory);
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        try {
            if (!Files.isDirectory(logDirectory)) {
                LOGGER.log(Level.SEVERE, "directory " + logDirectory + " does not exist. creating");
                Files.createDirectories(logDirectory);
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    private static final String LOGFILEEXTENSION = ".txlog";

    private Path writeSnapshotOnDisk(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {
        ensureDirectories();
        LogSequenceNumber actualLogSequenceNumber = snapshotData.getActualLogSequenceNumber();
        String filename = actualLogSequenceNumber.ledgerId + "_" + actualLogSequenceNumber.sequenceNumber;
        Path snapshotfilename_tmp = snapshotsDirectory.resolve(filename + SNAPSHOTFILEXTENSION + ".tmp");
        Path snapshotfilename = snapshotsDirectory.resolve(filename + SNAPSHOTFILEXTENSION);
        LOGGER.log(Level.INFO, "checkpoint, file:{0}", snapshotfilename.toAbsolutePath());

        try (OutputStream out = Files.newOutputStream(snapshotfilename_tmp);
            BufferedOutputStream bout = new BufferedOutputStream(out, 64 * 1024);
            GZIPOutputStream zout = new GZIPOutputStream(bout)) {
            BrokerStatusSnapshot.serializeSnapshot(snapshotData, zout);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        try {
            Files.move(snapshotfilename_tmp, snapshotfilename, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        return snapshotfilename;
    }

    private void deleteOldSnapshots(Path snapshotfilename) throws LogNotAvailableException {
        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(snapshotsDirectory)) {
            for (Path path : allfiles) {
                String other_filename = path.getFileName().toString();
                if (other_filename.endsWith(SNAPSHOTFILEXTENSION)) {
                    LOGGER.log(Level.SEVERE, "Processing snapshot file: " + path);
                    try {
                        other_filename = other_filename.substring(0, other_filename.length() - SNAPSHOTFILEXTENSION.length());

                        int pos = other_filename.indexOf('_');
                        if (pos > 0) {
                            if (!snapshotfilename.equals(path)) {
                                LOGGER.log(Level.SEVERE, "Deleting old snapshot file: " + path);
                                Files.delete(path);
                            }
                        }
                    } catch (NumberFormatException invalidName) {
                        LOGGER.log(Level.SEVERE, "Error:" + invalidName, invalidName);
                    }
                }
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void checkpoint(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {

        snapshotLock.lock();
        try {

            Path snapshotfilename = writeSnapshotOnDisk(snapshotData);
            deleteOldSnapshots(snapshotfilename);

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDirectory)) {
                List<Path> names = new ArrayList<>();
                for (Path path : stream) {
                    if (Files.isRegularFile(path) && path.getFileName().toString().endsWith(LOGFILEEXTENSION)) {
                        names.add(path);
                    }
                }
                names.sort(Comparator.comparing(Path::toString));
                for (Path p : names) {

                    String name = p.getFileName().toString().replace(LOGFILEEXTENSION, "");
                    long ledgerId = Long.parseLong(name, 16);

                    if (ledgerId < snapshotData.actualLogSequenceNumber.ledgerId
                        && ledgerId < currentLedgerId) {
                        LOGGER.log(Level.SEVERE, "snapshot, logfile is {0}, ledgerId {1}. to be removed (snapshot ledger id is {2})", new Object[]{p.toAbsolutePath(), ledgerId, snapshotData.actualLogSequenceNumber.ledgerId});
                        Files.deleteIfExists(p);
                    } else {
                        LOGGER.log(Level.SEVERE, "snapshot, logfile is {0}, ledgerId {1}. to be kept (snapshot ledger id is {2})", new Object[]{p.toAbsolutePath(), ledgerId, snapshotData.actualLogSequenceNumber.ledgerId});
                    }
                }
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }

        } finally {
            snapshotLock.unlock();
        }

    }

    private static final String SNAPSHOTFILEXTENSION = ".snap.json.gz";

    @Override
    public BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException {
        Path snapshotfilename = null;
        LogSequenceNumber latest = null;
        ensureDirectories();
        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(snapshotsDirectory)) {
            for (Path path : allfiles) {
                String filename = path.getFileName().toString();
                if (filename.endsWith(SNAPSHOTFILEXTENSION)) {
                    LOGGER.severe("Processing snapshot file: " + path);
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
                        LOGGER.severe("Error:" + invalidName);
                        invalidName.printStackTrace();
                    }
                }
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        if (snapshotfilename == null) {
            LOGGER.severe("No snapshot available Starting with a brand new status");
            currentLedgerId = 0;
            return new BrokerStatusSnapshot(0, 0, new LogSequenceNumber(-1, -1));
        } else {

            try (InputStream in = Files.newInputStream(snapshotfilename);
                BufferedInputStream bin = new BufferedInputStream(in);
                GZIPInputStream gzip = new GZIPInputStream(bin)) {
                BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(gzip);
                currentLedgerId = result.getActualLogSequenceNumber().ledgerId;
                return result;
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        }
    }
    private volatile boolean closed = false;

    @Override
    public void close() throws LogNotAvailableException {
        closed = true;
        try {
            spool.join();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new LogNotAvailableException(err);
        }
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public LogSequenceNumber getLastSequenceNumber() {
        final CommitFileWriter writer = this.writer;
        if (writer == null) {
            return (recoveredLogSequence == null) ? new LogSequenceNumber(currentLedgerId, -1) : recoveredLogSequence;
        } else {
            return new LogSequenceNumber(writer.ledgerId, writer.sequenceNumber);
        }
    }

}

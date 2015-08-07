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
package majordodo.replication;

import java.io.ByteArrayInputStream;
import majordodo.task.BrokerStatusSnapshot;
import majordodo.task.LogNotAvailableException;
import majordodo.task.LogSequenceNumber;
import majordodo.task.StatusChangesLog;
import majordodo.task.StatusEdit;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.ws.Holder;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.ChannelEventListener;
import majordodo.network.Message;
import majordodo.network.netty.NettyChannel;
import majordodo.network.netty.NettyConnector;
import majordodo.task.Broker;
import majordodo.utils.FileUtils;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Commit log replicated on Apache Bookeeper
 *
 * @author enrico.olivelli
 */
public class ReplicatedCommitLog extends StatusChangesLog {

    private static final Logger LOGGER = Logger.getLogger(ReplicatedCommitLog.class.getName());

    private static final byte[] magic = "dodo".getBytes(StandardCharsets.UTF_8);
    private BookKeeper bookKeeper;
    private ZKClusterManager zKClusterManager;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ReentrantLock snapshotLock = new ReentrantLock();
    private CommitFileWriter writer;
    private long currentLedgerId = 0;
    private long lastSequenceNumber = -1;
    private Path snapshotsDirectory;
    private LedgersInfo actualLedgersList;
    private int ensemble = 1;
    private int writeQuorumSize = 1;
    private int ackQuorumSize = 1;
    private long ledgersRetentionPeriod = 1000 * 60 * 60 * 24;
    private long maxLogicalLogFileSize = 1024 * 1024 * 256;
    private long writtenBytes = 0;

    public long getMaxLogicalLogFileSize() {
        return maxLogicalLogFileSize;
    }

    public void setMaxLogicalLogFileSize(long maxLogicalLogFileSize) {
        this.maxLogicalLogFileSize = maxLogicalLogFileSize;
    }

    public LedgersInfo getActualLedgersList() {
        return actualLedgersList;
    }

    private byte[] downloadSnapshotFromMaster(byte[] actualMaster) throws Exception {
        InetSocketAddress hostdata = Broker.parseHostdata(actualMaster);
        LOGGER.log(Level.SEVERE, "Downloading snapshot from " + hostdata);
        boolean ok = false;

        try (NettyConnector connector = new NettyConnector(new ChannelEventListener() {
            @Override
            public void messageReceived(Message message) {
            }

            @Override
            public void channelClosed() {

            }
        })) {
            connector.setPort(hostdata.getPort());
            connector.setHost(hostdata.getAddress().getHostAddress());
            try (NettyChannel channel = connector.connect();) {

                Message acceptMessage = Message.SNAPSHOT_DOWNLOAD_REQUEST();
                try {
                    Message connectionResponse = channel.sendMessageWithReply(acceptMessage, 10000);
                    if (connectionResponse.type == Message.TYPE_SNAPSHOT_DOWNLOAD_RESPONSE) {
                        byte[] data = (byte[]) connectionResponse.parameters.get("data");
                        return data;
                    } else {
                        throw new BrokerRejectedConnectionException("Broker rejected snapshot request, response message:" + connectionResponse);
                    }
                } catch (TimeoutException err) {
                    throw new BrokerNotAvailableException(err);
                }
            }
        }
    }

    private class CommitFileWriter implements AutoCloseable {

        private LedgerHandle out;

        private CommitFileWriter() throws LogNotAvailableException {
            try {
                this.out = bookKeeper.createLedger(ensemble, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.MAC, magic);
                writtenBytes = 0;
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            }
        }

        public long getLedgerId() {
            return this.out.getId();
        }

        public long writeEntry(StatusEdit edit) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException {
            try {
                byte[] serialize = edit.serialize();
                writtenBytes += serialize.length;
                long res = this.out.addEntry(serialize);
                if (writtenBytes > maxLogicalLogFileSize) {
                    LOGGER.log(Level.SEVERE, "{0} bytes written to ledger. need to open a new one", writtenBytes);
                    openNewLedger();
                }
                return res;
            } catch (BKException.BKLedgerClosedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (BKException.BKLedgerFencedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            }
        }

        public void close() throws LogNotAvailableException {
            if (out == null) {
                return;
            }
            try {
                out.close();
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            } finally {
                out = null;
            }
        }

        private List<Long> writeEntries(List<StatusEdit> edits) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException {

            try {
                Holder<Exception> exception = new Holder<>();
                CountDownLatch latch = new CountDownLatch(edits.size());
                List<Long> res = new ArrayList<>(edits.size());
                for (StatusEdit edit : edits) {
                    res.add(null);
                }
                for (int i = 0; i < edits.size(); i++) {
                    StatusEdit edit = edits.get(i);
                    byte[] serialize = edit.serialize();
                    writtenBytes += serialize.length;
                    this.out.asyncAddEntry(serialize, new AsyncCallback.AddCallback() {
                        @Override
                        public void addComplete(int rc, LedgerHandle lh, long entryId, Object i) {
                            int index = (Integer) i;
                            if (rc != BKException.Code.OK) {
                                BKException error = BKException.create(rc);
                                exception.value = error;
                                res.set(index, null);
                                for (int j = 0; j < edits.size(); j++) {
                                    // early exit
                                    latch.countDown();
                                }
                            } else {
                                res.set(index, entryId);
                                latch.countDown();
                            }

                        }
                    }, i);
                }
                latch.await();
                if (exception.value != null) {
                    throw exception.value;
                }
                for (Long l : res) {
                    if (l == null) {
                        throw new RuntimeException("bug ! " + res);
                    }
                }
                if (writtenBytes > maxLogicalLogFileSize) {
                    LOGGER.log(Level.SEVERE, "{0} bytes written to ledger. need to open a new one", writtenBytes);
                    openNewLedger();
                }
                return res;
            } catch (BKException.BKLedgerClosedException err) {
                // corner case, if some entry has been written ?? it will be duplicated on retry
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (BKException.BKLedgerFencedException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            }
        }
    }

    private final LeaderShipChangeListener leaderShiplistener = new LeaderShipChangeListener() {

        @Override
        public void leadershipLost() {
            LOGGER.log(Level.SEVERE, "leadershipLost");
        }

        @Override
        public void leadershipAcquired() {
            LOGGER.log(Level.SEVERE, "leadershipAcquired");
        }

    };

    public ReplicatedCommitLog(String zkAddress, int zkTimeout, String zkPath, Path snapshotsDirectory, byte[] localhostdata) throws Exception {
        if (localhostdata == null) {
            localhostdata = new byte[0];
        }
        ClientConfiguration config = new ClientConfiguration();
        try {
            this.zKClusterManager = new ZKClusterManager(zkAddress, zkTimeout, zkPath, leaderShiplistener, localhostdata);
            this.zKClusterManager.waitForConnection();
            this.bookKeeper = new BookKeeper(config, zKClusterManager.getZooKeeper());
            this.snapshotsDirectory = snapshotsDirectory;
            this.zKClusterManager.start();
        } catch (Exception t) {
            close();
            throw t;
        }
    }

    public int getEnsemble() {
        return ensemble;
    }

    public void setEnsemble(int ensemble) {
        this.ensemble = ensemble;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public void setWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    public void setAckQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
    }

    public long getLedgersRetentionPeriod() {
        return ledgersRetentionPeriod;
    }

    public void setLedgersRetentionPeriod(long ledgersRetentionPeriod) {
        this.ledgersRetentionPeriod = ledgersRetentionPeriod;
    }

    @Override
    public List<LogSequenceNumber> logStatusEditBatch(List<StatusEdit> edits) throws LogNotAvailableException {
        if (edits.isEmpty()) {
            return Collections.emptyList();
        }
        while (true) {
            writeLock.lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                try {
                    List<Long> newSequenceNumbers = writer.writeEntries(edits);
                    lastSequenceNumber = newSequenceNumbers.stream().max(Comparator.naturalOrder()).get();
                    List<LogSequenceNumber> res = new ArrayList<>();
                    for (Long newSequenceNumber : newSequenceNumbers) {
                        res.add(new LogSequenceNumber(currentLedgerId, newSequenceNumber));
                    }
                    return res;
                } catch (BKException.BKLedgerClosedException closed) {
                    LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger", closed);
                    Thread.sleep(1000);
                    openNewLedger();
                } catch (BKException.BKLedgerFencedException fenced) {
                    LOGGER.log(Level.SEVERE, "this broker was fenced!", fenced);
                    zKClusterManager.close();
                    close();
                    throw new LogNotAvailableException(fenced);
                }
            } catch (InterruptedException err) {
                throw new LogNotAvailableException(err);
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public LogSequenceNumber logStatusEdit(StatusEdit edit) throws LogNotAvailableException {
        while (true) {
            writeLock.lock();
            try {
                if (writer == null) {
                    throw new LogNotAvailableException(new Exception("no ledger opened for writing"));
                }
                try {
                    long newSequenceNumber = writer.writeEntry(edit);
                    lastSequenceNumber = newSequenceNumber;
                    return new LogSequenceNumber(currentLedgerId, newSequenceNumber);
                } catch (BKException.BKLedgerClosedException closed) {
                    LOGGER.log(Level.SEVERE, "ledger has been closed, need to open a new ledger", closed);
                    Thread.sleep(1000);
                    openNewLedger();
                } catch (BKException.BKLedgerFencedException fenced) {
                    LOGGER.log(Level.SEVERE, "this broker was fenced!", fenced);
                    zKClusterManager.close();
                    close();
                    throw new LogNotAvailableException(fenced);
                }
            } catch (InterruptedException err) {
                throw new LogNotAvailableException(err);
            } finally {
                writeLock.unlock();
            }
        }

    }

    private void openNewLedger() throws LogNotAvailableException {
        writeLock.lock();
        try {
            if (writer != null) {
                writer.close();
            }
            writer = new CommitFileWriter();
            currentLedgerId = writer.getLedgerId();
            LOGGER.log(Level.SEVERE, "Opened new ledger:" + currentLedgerId);
            if (actualLedgersList.getFirstLedger() < 0) {
                actualLedgersList.setFirstLedger(currentLedgerId);
            }
            actualLedgersList.addLedger(currentLedgerId);
            zKClusterManager.saveActualLedgersList(actualLedgersList);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer) throws LogNotAvailableException {
        this.actualLedgersList = zKClusterManager.getActualLedgersList();
        LOGGER.log(Level.SEVERE, "Actual ledgers list:" + actualLedgersList);
        this.currentLedgerId = snapshotSequenceNumber.ledgerId;
        LOGGER.log(Level.SEVERE, "Latest snapshotSequenceNumber:" + snapshotSequenceNumber);
        if (currentLedgerId > 0 && !this.actualLedgersList.getActiveLedgers().contains(currentLedgerId)) {
            // TODO: download snapshot from another remote broker
            throw new LogNotAvailableException(new Exception("Actual ledgers list does not include latest snapshot ledgerid:" + currentLedgerId + ". manual recoveryis needed (pickup a recent snapshot from a live broker please)"));
        }
        try {
            for (long ledgerId : actualLedgersList.getActiveLedgers()) {
                LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId);
                LedgerHandle handle = bookKeeper.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.MAC, magic);
                try {
                    long lastAddConfirmed = handle.getLastAddConfirmed();
                    LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId + ", lastAddConfirmed=" + lastAddConfirmed);
                    if (lastAddConfirmed >= 0) {
                        for (Enumeration<LedgerEntry> en = handle.readEntries(0, lastAddConfirmed); en.hasMoreElements();) {
                            LedgerEntry entry = en.nextElement();

                            LogSequenceNumber number = new LogSequenceNumber(ledgerId, entry.getEntryId());
                            StatusEdit statusEdit = StatusEdit.read(entry.getEntry());
                            if (number.after(snapshotSequenceNumber)) {
                                LOGGER.log(Level.FINEST, "RECOVER ENTRY {0}, {1}", new Object[]{number, statusEdit});
                                consumer.accept(number, statusEdit);
                            } else {
                                LOGGER.log(Level.FINEST, "SKIP ENTRY {0}<{1}, {2}", new Object[]{number, snapshotSequenceNumber, statusEdit});
                            }
                        }
                    }
                } finally {
                    handle.close();
                }
            }
        } catch (Exception err) {
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void startWriting() throws LogNotAvailableException {
        actualLedgersList = zKClusterManager.getActualLedgersList();
        openNewLedger();
    }

    @Override
    public void clear() throws LogNotAvailableException {
        this.currentLedgerId = 0;
        try {
            FileUtils.cleanDirectory(snapshotsDirectory);
            zKClusterManager.saveActualLedgersList(new LedgersInfo());
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
        ensureDirectories();
    }

    @Override
    public boolean isWritable() {
        return writer != null;
    }

    private void ensureDirectories() throws LogNotAvailableException {
        try {
            if (!Files.isDirectory(snapshotsDirectory)) {
                Files.createDirectories(snapshotsDirectory);
            }
        } catch (IOException err) {
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public void checkpoint(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException {
        snapshotLock.lock();
        try {
            ensureDirectories();
            LogSequenceNumber actualLogSequenceNumber = snapshotData.getActualLogSequenceNumber();
            String filename = actualLogSequenceNumber.ledgerId + "_" + actualLogSequenceNumber.sequenceNumber;
            Path snapshotfilename = snapshotsDirectory.resolve(filename + SNAPSHOTFILEXTENSION);
            LOGGER.log(Level.INFO, "checkpoint, file:{0}", snapshotfilename.toAbsolutePath());
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> filedata = BrokerStatusSnapshot.serializeSnapshot(snapshotData);

            try (OutputStream out = Files.newOutputStream(snapshotfilename)) {
                mapper.writeValue(out, filedata);
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        } finally {
            snapshotLock.unlock();
        }
        if (zKClusterManager.isLeader()) {
            dropOldLedgers();
        }
    }

    private void dropOldLedgers() throws LogNotAvailableException {
        if (ledgersRetentionPeriod > 0) {
            long min_timestamp = System.currentTimeMillis() - ledgersRetentionPeriod;
            List<Long> oldLedgers;
            writeLock.lock();
            try {
                oldLedgers = actualLedgersList.getOldLedgers(min_timestamp);
                oldLedgers.remove(this.currentLedgerId);
            } finally {
                writeLock.unlock();
            }
            if (oldLedgers.isEmpty()) {
                return;
            }
            LOGGER.log(Level.SEVERE, "dropping ledgers before ", new java.sql.Timestamp(min_timestamp) + ": " + oldLedgers);
            for (long ledgerId : oldLedgers) {
                writeLock.lock();
                try {
                    LOGGER.log(Level.SEVERE, "dropping ledger {0}", ledgerId);
                    actualLedgersList.removeLedger(ledgerId);
                    bookKeeper.deleteLedger(ledgerId);
                    zKClusterManager.saveActualLedgersList(actualLedgersList);
                    LOGGER.log(Level.SEVERE, "dropping ledger {0}, finished", ledgerId);
                } catch (BKException | InterruptedException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId, error);
                    throw new LogNotAvailableException(error);
                } catch (LogNotAvailableException error) {
                    LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId, error);
                    throw error;
                } finally {
                    writeLock.unlock();
                }
            }

        }
    }

    private static final String SNAPSHOTFILEXTENSION = ".snap.json";

    @Override
    public BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException {
        Path snapshotfilename = null;
        LogSequenceNumber latest = null;
        ensureDirectories();
        try (DirectoryStream<Path> allfiles = Files.newDirectoryStream(snapshotsDirectory)) {
            for (Path path : allfiles) {
                String filename = path.getFileName().toString();
                if (filename.endsWith(SNAPSHOTFILEXTENSION)) {
                    LOGGER.log(Level.SEVERE, "Processing snapshot file: " + path);
                    try {
                        filename = filename.substring(0, filename.length() - SNAPSHOTFILEXTENSION.length());

                        int pos = filename.indexOf('_');
                        if (pos > 0) {
                            long ledgerId = Long.parseLong(filename.substring(0, pos));
                            long sequenceNumber = Long.parseLong(filename.substring(pos + 1));
                            LOGGER.log(Level.SEVERE, "File " + path + " contains snapshot, ledgerId:" + ledgerId + ",sequenceNumber:" + sequenceNumber);
                            LogSequenceNumber number = new LogSequenceNumber(ledgerId, sequenceNumber);
                            if (latest == null || number.after(latest)) {
                                latest = number;
                                snapshotfilename = path;
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
        LedgersInfo _actualLedgersList = zKClusterManager.getActualLedgersList();
        LOGGER.log(Level.SEVERE, "ActualLedgersList " + actualLedgersList);

        if (snapshotfilename != null) {
            LOGGER.log(Level.SEVERE, "Loading snapshot from " + snapshotfilename);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> snapshotdata;

            try (InputStream in = Files.newInputStream(snapshotfilename)) {
                snapshotdata = mapper.readValue(in, Map.class
                );
                BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(snapshotdata);
                currentLedgerId = result.getActualLogSequenceNumber().ledgerId;
                LOGGER.log(Level.SEVERE, "Snapshot has been taken at ledgerId=" + result.getActualLogSequenceNumber().ledgerId + ", sequenceNumber=" + result.getActualLogSequenceNumber().sequenceNumber);
                if (_actualLedgersList.getActiveLedgers().contains(currentLedgerId)) {
                    return result;
                }
                LOGGER.log(Level.SEVERE, "Actually the loaded snapshot is not recoveable given the actual ledgers list. This file cannot be used for recovery");
            } catch (IOException err) {
                throw new LogNotAvailableException(err);
            }
        }

        currentLedgerId = -1;
        if (_actualLedgersList.getFirstLedger() < 0) {
            LOGGER.log(Level.SEVERE, "No snapshot present and no ledger registered on ZK. Starting with a brand new status");
            return new BrokerStatusSnapshot(0, 0, new LogSequenceNumber(-1, -1));
        } else if (_actualLedgersList.getActiveLedgers().contains(_actualLedgersList.getFirstLedger())) {
            LOGGER.log(Level.SEVERE, "No valid snapshot present, But the first ledger of history " + _actualLedgersList.getFirstLedger() + ", is still present in active ledgers list. I can use an empty snapshot in order to boot");
            return new BrokerStatusSnapshot(0, 0, new LogSequenceNumber(-1, -1));
        } else {
            LOGGER.log(Level.SEVERE, "No valid snapshot present, I will try to download a snapshot from the actual master");

            byte[] actualMaster;
            try {
                actualMaster = zKClusterManager.getActualMaster();
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            }
            if (actualMaster == null || actualMaster.length == 0) {
                LOGGER.log(Level.SEVERE, "No snapshot present, no master is present, cannot boot");
                throw new LogNotAvailableException(new Exception("No valid snapshot present, no master is present, cannot boot"));
            } else {
                byte[] snapshot;
                try {
                    snapshot = downloadSnapshotFromMaster(actualMaster);
                } catch (Exception err) {
                    throw new LogNotAvailableException(err);
                }
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> snapshotdata;

                try (InputStream in = new ByteArrayInputStream(snapshot)) {
                    snapshotdata = mapper.readValue(in, Map.class
                    );
                    BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(snapshotdata);
                    currentLedgerId = result.getActualLogSequenceNumber().ledgerId;
                    return result;
                } catch (IOException err) {
                    throw new LogNotAvailableException(err);
                }
            }

        }

    }

    private volatile boolean closed = false;

    @Override
    public final void close() {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            if (writer != null) {

                try {
                    writer.close();
                } catch (Exception err) {
                    err.printStackTrace();
                } finally {
                    writer = null;
                }
            }
            if (zKClusterManager != null) {
                try {
                    zKClusterManager.close();
                } finally {
                    zKClusterManager = null;
                }
            }
            closed = true;
            LOGGER.severe("closed");
        } finally {
            writer = null;
            writeLock.unlock();
        }

    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void requestLeadership() throws LogNotAvailableException {
        zKClusterManager.requestLeadership();
    }

    @Override
    public void followTheLeader(LogSequenceNumber skipPast, BiConsumer<LogSequenceNumber, StatusEdit> consumer) throws LogNotAvailableException {

        List<Long> actualList = zKClusterManager.getActualLedgersList().getActiveLedgers();

        List<Long> toRead = actualList;
        if (skipPast.ledgerId != -1) {
            toRead = toRead.stream().filter(l -> l >= skipPast.ledgerId).collect(Collectors.toList());
        }
        try {
            long nextEntry = skipPast.sequenceNumber + 1;
            LOGGER.log(Level.SEVERE, "followTheLeader skipPast:" + skipPast + " toRead: " + toRead + " actualList:" + actualList + ", nextEntry:" + nextEntry);
            for (Long previous : toRead) {
                //LOGGER.log(Level.SEVERE, "followTheLeader openLedger " + previous + " nextEntry:" + nextEntry);
                LedgerHandle lh;
                try {
                    lh = bookKeeper.openLedgerNoRecovery(previous,
                            BookKeeper.DigestType.MAC, magic);
                } catch (BKException.BKLedgerRecoveryException e) {
                    LOGGER.log(Level.SEVERE, "error", e);
                    return;
                }
                long lastAddConfirmed = lh.getLastAddConfirmed();
                LOGGER.log(Level.SEVERE, "followTheLeader openLedger " + previous + " -> lastAddConfirmed:" + lastAddConfirmed + ", nextEntry:" + nextEntry);
                if (nextEntry > lastAddConfirmed) {
                    nextEntry = 0;
                    continue;
                }
                Enumeration<LedgerEntry> entries
                        = lh.readEntries(nextEntry, lh.getLastAddConfirmed());

                while (entries.hasMoreElements()) {
                    LedgerEntry e = entries.nextElement();
                    long entryId = e.getEntryId();

                    byte[] entryData = e.getEntry();
                    StatusEdit statusEdit = StatusEdit.read(entryData);
                    LOGGER.log(Level.SEVERE, "entry " + previous + "," + entryId + " -> " + statusEdit);
                    LogSequenceNumber number = new LogSequenceNumber(previous, entryId);
                    consumer.accept(number, statusEdit);
                    lastSequenceNumber = number.sequenceNumber;
                    currentLedgerId = number.ledgerId;

                }
            }
        } catch (InterruptedException | IOException | BKException err) {
            err.printStackTrace();
            throw new LogNotAvailableException(err);
        }
    }

    @Override
    public boolean isLeader() {
        return zKClusterManager != null && zKClusterManager.isLeader();
    }

    @Override
    public long getCurrentLedgerId() {
        return currentLedgerId;
    }

    @Override
    public long getCurrentSequenceNumber() {
        return lastSequenceNumber;
    }

}

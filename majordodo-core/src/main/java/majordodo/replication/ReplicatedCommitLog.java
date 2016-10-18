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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.xml.ws.Holder;
import majordodo.network.BrokerHostData;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.ConnectionRequestInfo;
import static majordodo.network.ConnectionRequestInfo.CLIENT_TYPE_BROKER;
import majordodo.network.Message;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.utils.FileUtils;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * Commit log replicated on Apache Bookkeeper
 *
 * @author enrico.olivelli
 */
public class ReplicatedCommitLog extends StatusChangesLog {

    private static final Logger LOGGER = Logger.getLogger(ReplicatedCommitLog.class.getName());
    private static final long DOWNLOAD_FROM_MASTER_TIMEOUT = Long.parseLong(System.getProperty("majordodo.downloadfrommaster.timeout", "240000"));

    private String sharedSecret = "dodo";
    private BookKeeper bookKeeper;
    private ZKClusterManager zKClusterManager;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final ReentrantLock snapshotLock = new ReentrantLock();
    private volatile CommitFileWriter writer;
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

    @Override
    public String getSharedSecret() {
        return sharedSecret;
    }

    @Override
    public void setSharedSecret(String sharedSecret) {
        this.sharedSecret = sharedSecret;
    }

    public long getMaxLogicalLogFileSize() {
        return maxLogicalLogFileSize;
    }

    public void setMaxLogicalLogFileSize(long maxLogicalLogFileSize) {
        this.maxLogicalLogFileSize = maxLogicalLogFileSize;
    }

    public LedgersInfo getActualLedgersList() {
        return actualLedgersList;
    }

    private byte[] downloadSnapshotFromMaster(BrokerHostData brokerData) throws Exception {

        InetSocketAddress hostdata = brokerData.getSocketAddress();
        boolean ssl = brokerData.isSsl();
        LOGGER.log(Level.SEVERE, "Downloading snapshot from " + hostdata + " ssl=" + ssl);
        boolean ok = false;

        try (NettyBrokerLocator connector = new NettyBrokerLocator(hostdata.getAddress().getHostAddress(), hostdata.getPort(), brokerData.isSsl())) {

            try (Channel channel = connector.connect(new ChannelEventListener() {
                @Override
                public void messageReceived(Message message) {

                }

                @Override
                public void channelClosed() {

                }
            }, brokerConnectionRequestInfo);) {

                Message acceptMessage = Message.SNAPSHOT_DOWNLOAD_REQUEST();
                try {
                    Message connectionResponse = channel.sendMessageWithReply(acceptMessage, DOWNLOAD_FROM_MASTER_TIMEOUT);
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
                this.out = bookKeeper.createLedger(ensemble, writeQuorumSize, ackQuorumSize, BookKeeper.DigestType.MAC, sharedSecret.getBytes(StandardCharsets.UTF_8));
                writtenBytes = 0;
            } catch (Exception err) {
                throw new LogNotAvailableException(err);
            }
        }

        public long getLedgerId() {
            return this.out.getId();
        }

        public long writeEntry(StatusEdit edit) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException, BKNotEnoughBookiesException {
            long _start = System.currentTimeMillis();
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
            } catch (BKException.BKNotEnoughBookiesException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntry {0} time " + (_end - _start) + " ms", new Object[]{edit});
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

        private List<Long> writeEntries(List<StatusEdit> edits) throws LogNotAvailableException, BKException.BKLedgerClosedException, BKException.BKLedgerFencedException, BKNotEnoughBookiesException {
            int size = edits.size();
            if (size == 0) {
                return Collections.emptyList();
            } else if (size == 1) {
                return Arrays.asList(writeEntry(edits.get(0)));
            }
            long _start = System.currentTimeMillis();
            try {
                Holder<Exception> exception = new Holder<>();
                CountDownLatch latch = new CountDownLatch(edits.size());
                List<Long> res = new ArrayList<>(edits.size());
                for (int i = 0; i < size; i++) {
                    res.add(null);
                }
                for (int i = 0; i < size; i++) {
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
            } catch (BKException.BKNotEnoughBookiesException err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw err;
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while writing to ledger " + out, err);
                throw new LogNotAvailableException(err);
            } finally {
                long _end = System.currentTimeMillis();
                LOGGER.log(Level.FINEST, "writeEntries " + edits.size() + " time " + (_end - _start) + " ms");
            }
        }
    }

    private final ConnectionRequestInfo brokerConnectionRequestInfo = new ConnectionRequestInfo() {
        @Override
        public Set<Long> getRunningTaskIds() {
            return Collections.emptySet();
        }

        @Override
        public String getWorkerId() {
            return "broker";
        }

        @Override
        public String getProcessId() {
            return "";
        }

        @Override
        public String getLocation() {
            return "";
        }

        @Override
        public String getSharedSecret() {
            return sharedSecret;
        }

        @Override
        public int getMaxThreads() {
            return 0;
        }

        @Override
        public Map<String, Integer> getMaxThreadsByTaskType() {
            return Collections.emptyMap();
        }

        @Override
        public List<Integer> getGroups() {
            return Collections.emptyList();
        }

        @Override
        public Set<Integer> getExcludedGroups() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, Integer> getResourceLimits() {
            return Collections.emptyMap();
        }

        @Override
        public String getClientType() {
            return CLIENT_TYPE_BROKER;
        }

    };

    private final LeaderShipChangeListener leaderShiplistener = new LeaderShipChangeListener() {

        @Override
        public void leadershipLost() {
            LOGGER.log(Level.SEVERE, "leadershipLost");
            signalBrokerFailed();

        }

        @Override
        public void leadershipAcquired() {
            LOGGER.log(Level.SEVERE, "leadershipAcquired");
        }

    };

    public ReplicatedCommitLog(String zkAddress, int zkTimeout, String zkPath, Path snapshotsDirectory, byte[] localhostdata, boolean writeacls) throws Exception {
        if (localhostdata == null) {
            localhostdata = new byte[0];
        }
        ClientConfiguration config = new ClientConfiguration();
        config.setThrottleValue(0);
        try {
            this.zKClusterManager = new ZKClusterManager(zkAddress, zkTimeout, zkPath, leaderShiplistener, localhostdata, writeacls);
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
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
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
                    signalBrokerFailed();
                    throw new LogNotAvailableException(fenced);
                } catch (BKException.BKNotEnoughBookiesException missingBk) {
                    LOGGER.log(Level.SEVERE, "bookkeeper failure", missingBk);
                    zKClusterManager.close();
                    close();
                    signalBrokerFailed();
                    throw new LogNotAvailableException(missingBk);
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
            if (closed) {
                throw new LogNotAvailableException(new Exception("closed"));
            }
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
                    signalBrokerFailed();
                    throw new LogNotAvailableException(fenced);
                } catch (BKException.BKNotEnoughBookiesException missingBk) {
                    LOGGER.log(Level.SEVERE, "bookkeeper failure", missingBk);
                    zKClusterManager.close();
                    close();
                    signalBrokerFailed();
                    throw new LogNotAvailableException(missingBk);
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
            closeCurrentWriter();
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

    public ZKClusterManager getClusterManager() {
        return zKClusterManager;
    }

    @Override
    public void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer, boolean fencing) throws LogNotAvailableException {
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

                if (ledgerId < snapshotSequenceNumber.ledgerId) {
                    LOGGER.log(Level.SEVERE, "Skipping ledger " + ledgerId);
                    continue;
                }
                LedgerHandle handle;
                if (fencing) {
                    handle = bookKeeper.openLedger(ledgerId, BookKeeper.DigestType.MAC, sharedSecret.getBytes(StandardCharsets.UTF_8));
                } else {
                    handle = bookKeeper.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.MAC, sharedSecret.getBytes(StandardCharsets.UTF_8));
                }
                try {
                    long first;
                    if (ledgerId == snapshotSequenceNumber.ledgerId) {
                        first = snapshotSequenceNumber.sequenceNumber;
                        LOGGER.log(Level.SEVERE, "Recovering from latest snapshot ledger " + ledgerId + ", starting from entry " + first);
                    } else {
                        first = 0;
                        LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId + ", starting from entry " + first);
                    }
                    long lastAddConfirmed = handle.getLastAddConfirmed();
                    LOGGER.log(Level.SEVERE, "Recovering from ledger " + ledgerId + ", first=" + first, " lastAddConfirmed=" + lastAddConfirmed);
                    final int BATCH_SIZE = 10000;
                    if (lastAddConfirmed > 0) {

                        for (long b = first; b <= lastAddConfirmed;) {
                            long start = b;
                            long end = b + BATCH_SIZE;
                            if (end > lastAddConfirmed) {
                                end = lastAddConfirmed;
                            }
                            b = end + 1;
                            double percent = ((start - first) * 100.0 / (lastAddConfirmed + 1));
                            LOGGER.log(Level.SEVERE, "From entry {0}, to entry {1} ({2} %)", new Object[]{start, end, percent});
                            Holder<Throwable> error = new Holder<>();
                            CountDownLatch count = new CountDownLatch(1);
                            handle.asyncReadEntries(start, end, new AsyncCallback.ReadCallback() {
                                @Override
                                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object o) {
                                    if (rc != BKException.Code.OK) {
                                        error.value = BKException.create(rc).fillInStackTrace();
                                        count.countDown();
                                        return;
                                    }
                                    try {
                                        while (seq.hasMoreElements()) {
                                            LedgerEntry entry = seq.nextElement();
                                            LogSequenceNumber number = new LogSequenceNumber(ledgerId, entry.getEntryId());
                                            StatusEdit statusEdit = StatusEdit.read(entry.getEntry());
                                            if (number.after(snapshotSequenceNumber)) {
                                                LOGGER.log(Level.FINEST, "RECOVER ENTRY {0}, {1}", new Object[]{number, statusEdit});
                                                consumer.accept(number, statusEdit);
                                            } else {
                                                LOGGER.log(Level.FINEST, "SKIP ENTRY {0}<{1}, {2}", new Object[]{number, snapshotSequenceNumber, statusEdit});
                                            }
                                        }
                                    } catch (Throwable errorOccurred) {
                                        error.value = errorOccurred;
                                    }
                                    count.countDown();
                                }
                            }, null);
                            count.await();
                        }
                    }
                } finally {
                    handle.close();
                }
            }
        } catch (Exception err) {
            LOGGER.log(Level.SEVERE, "Fatal error during recovery", err);
            signalBrokerFailed();
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
            Path snapshotfilename = writeSnapshotOnDisk(snapshotData);

            deleteOldSnapshots(snapshotfilename);
        } finally {
            snapshotLock.unlock();
        }

        if (zKClusterManager != null && zKClusterManager.isLeader()) {
            dropOldLedgers(snapshotData.getActualLogSequenceNumber());
        }
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

    private void dropOldLedgers(LogSequenceNumber latestSnapshotPosition) throws LogNotAvailableException {
        if (ledgersRetentionPeriod > 0) {
            long min_timestamp = System.currentTimeMillis() - ledgersRetentionPeriod;
            List<Long> oldLedgers;
            writeLock.lock();
            try {
                oldLedgers = actualLedgersList.getOldLedgers(min_timestamp);
            } finally {
                writeLock.unlock();
            }
            if (oldLedgers.isEmpty()) {
                return;
            }
            LOGGER.log(Level.SEVERE, "dropping ledgers before " + new java.sql.Timestamp(min_timestamp) + ", oldLedgers " + oldLedgers + ", currentLedgerId:" + currentLedgerId + ", latestSnapshotLedgerId:" + latestSnapshotPosition.ledgerId);
            for (long ledgerId : oldLedgers) {
                if (ledgerId >= latestSnapshotPosition.ledgerId
                    || ledgerId >= currentLedgerId) {
                    LOGGER.log(Level.SEVERE, "ledger " + ledgerId + " cannot be dropped");
                    continue;
                }

                writeLock.lock();
                try {
                    LOGGER.log(Level.SEVERE, "remove ledger " + ledgerId + " from the actualLedgersList");
                    actualLedgersList.removeLedger(ledgerId);
                    zKClusterManager.saveActualLedgersList(actualLedgersList);
                    LOGGER.log(Level.SEVERE, "dropping ledger " + ledgerId + " on BookKeeper");
                    try {
                        bookKeeper.deleteLedger(ledgerId);
                    } catch (BKNoSuchLedgerExistsException error) {
                        LOGGER.log(Level.SEVERE, "error while dropping ledger " + ledgerId + ": " + error, error);
                    }
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

    private static final String SNAPSHOTFILEXTENSION = ".snap.json.gz";

    @Override
    public BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException {
        Path snapshotfilename = null;
        ensureDirectories();

        // download a snapshot from the actual leaer if present (this will be generally faster then recoverying from BK)                
        byte[] actualLeader;
        BrokerHostData leaderData = null;
        try {
            actualLeader = zKClusterManager.getActualMaster();
            if (actualLeader != null && actualLeader.length > 0) {
                leaderData = BrokerHostData.parseHostdata(actualLeader);
                LOGGER.log(Level.SEVERE, "actual leader is at " + leaderData.getHost() + ":" + leaderData.getPort());
            } else {
                LOGGER.log(Level.SEVERE, "no leader is present");
            }

        } catch (Exception err) {
            throw new LogNotAvailableException(err);
        }
        if (leaderData != null && !isLeader()) {
            byte[] snapshot;
            try {
                snapshot = downloadSnapshotFromMaster(leaderData);
                LOGGER.log(Level.SEVERE, "downloaded " + snapshot.length + " snapshot data from actual leader");
                try (InputStream in = new ByteArrayInputStream(snapshot);
                    GZIPInputStream gzip = new GZIPInputStream(in)) {
                    BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(gzip);
                    writeSnapshotOnDisk(result);
                    currentLedgerId = result.getActualLogSequenceNumber().ledgerId;
                    return result;
                }
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while reading snapshot from network", err);
            }

        }

        LogSequenceNumber latest = null;
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
        LOGGER.log(Level.SEVERE, "ActualLedgersList from ZK: " + _actualLedgersList);

        if (snapshotfilename != null) {
            LOGGER.log(Level.SEVERE, "Loading snapshot from " + snapshotfilename);
            try (InputStream in = Files.newInputStream(snapshotfilename);
                BufferedInputStream bin = new BufferedInputStream(in);
                GZIPInputStream gzip = new GZIPInputStream(bin)) {
                BrokerStatusSnapshot result = BrokerStatusSnapshot.deserializeSnapshot(gzip);
                currentLedgerId = result.getActualLogSequenceNumber().ledgerId;

                LOGGER.log(Level.SEVERE,
                    "Snapshot has been taken at ledgerId=" + result.getActualLogSequenceNumber().ledgerId + ", sequenceNumber=" + result.getActualLogSequenceNumber().sequenceNumber);
                if (_actualLedgersList.getActiveLedgers()
                    .contains(currentLedgerId)) {
                    return result;
                }

                LOGGER.log(Level.SEVERE,
                    "Actually the loaded snapshot is not recoveable given the actual ledgers list. This file cannot be used for recovery");
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "error while reading snapshot data", err);
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
            LOGGER.log(Level.SEVERE, "No snapshot present, no leader is present, cannot boot");
            throw new LogNotAvailableException(new Exception("No valid snapshot present, no leader is present, cannot boot"));
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
            closeCurrentWriter();
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

    private void closeCurrentWriter() {
        if (writer != null) {

            try {
                writer.close();
            } catch (Exception err) {
                LOGGER.log(Level.SEVERE, "error while closing ledger", err);
            } finally {
                writer = null;
            }
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

        List<Long> actualList;
        try {
            actualList = zKClusterManager.getActualLedgersList().getActiveLedgers();
        } catch (LogNotAvailableException temporaryError) {
            LOGGER.log(Level.SEVERE, "temporary error " + temporaryError, temporaryError);
            return;
        }

        List<Long> toRead = actualList;
        if (skipPast.ledgerId != -1) {
            toRead = toRead.stream().filter(l -> l >= skipPast.ledgerId).collect(Collectors.toList());
        }

        try {
            long nextEntry = skipPast.sequenceNumber + 1;
            LOGGER.log(Level.SEVERE, "followTheLeader skipPast:{0} toRead: {1} actualList:{2}, nextEntry:{3}", new Object[]{skipPast, toRead, actualList, nextEntry});
            for (Long previous : toRead) {
                //LOGGER.log(Level.SEVERE, "followTheLeader openLedger " + previous + " nextEntry:" + nextEntry);

                List<Map.Entry<Long, StatusEdit>> buffer = new ArrayList<>();

                // first of all we read data from the leader
                try (LedgerHandle lh = bookKeeper.openLedgerNoRecovery(previous,
                    BookKeeper.DigestType.MAC, sharedSecret.getBytes(StandardCharsets.UTF_8));) {
                    long lastAddConfirmed = lh.getLastAddConfirmed();
                    LOGGER.log(Level.FINE, "followTheLeader openLedger {0} -> lastAddConfirmed:{1}, nextEntry:{2}", new Object[]{previous, lastAddConfirmed, nextEntry});
                    if (nextEntry > lastAddConfirmed) {
                        nextEntry = 0;
                        continue;
                    }
                    Enumeration<LedgerEntry> entries = lh.readEntries(nextEntry, lh.getLastAddConfirmed());
                    if (entries != null) {
                        while (entries.hasMoreElements()) {
                            LedgerEntry e = entries.nextElement();
                            long entryId = e.getEntryId();
                            byte[] entryData = e.getEntry();
                            StatusEdit statusEdit = StatusEdit.read(entryData);
                            buffer.add(new AbstractMap.SimpleImmutableEntry<>(entryId, statusEdit));
                        }
                    }
                } catch (BKException.BKLedgerRecoveryException | BKBookieHandleNotAvailableException temporaryError) {
                    LOGGER.log(Level.SEVERE, "temporary error " + temporaryError, temporaryError);
                    return;
                } catch (InterruptedException err) {
                    LOGGER.log(Level.SEVERE, "error while reading ledger " + err, err);
                    Thread.currentThread().interrupt();
                    throw err;
                }

                // use the entry
                for (Map.Entry<Long, StatusEdit> entry : buffer) {
                    long entryId = entry.getKey();
                    StatusEdit statusEdit = entry.getValue();
                    LOGGER.log(Level.FINEST, "entry {0},{1} -> {2}", new Object[]{previous, entryId, statusEdit});
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

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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Log of mofications, this is the base of the replication system
 *
 * @author enrico.olivelli
 */
public abstract class StatusChangesLog implements AutoCloseable {

    protected BrokerFailureListener failureListener;

    public BrokerFailureListener getFailureListener() {
        return failureListener;
    }

    public void setFailureListener(BrokerFailureListener failureListener) {
        this.failureListener = failureListener;
    }

    public final void signalBrokerFailed() {
        if (failureListener != null) {
            failureListener.brokerFailed();
        }
    }

    public abstract LogSequenceNumber logStatusEdit(StatusEdit edit) throws LogNotAvailableException;

    public abstract void recovery(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer, boolean fencing) throws LogNotAvailableException;

    public void clear() throws LogNotAvailableException {
    }

    public void startWriting() throws LogNotAvailableException {
    }

    public abstract void checkpoint(BrokerStatusSnapshot snapshotData) throws LogNotAvailableException;

    public abstract BrokerStatusSnapshot loadBrokerStatusSnapshot() throws LogNotAvailableException;

    @Override
    public void close() throws LogNotAvailableException {
    }

    public abstract long getCurrentLedgerId();

    public abstract long getCurrentSequenceNumber();

    public boolean isLeader() {
        return true;
    }

    public void followTheLeader(LogSequenceNumber snapshotSequenceNumber, BiConsumer<LogSequenceNumber, StatusEdit> consumer) throws LogNotAvailableException {
    }

    public abstract boolean isClosed();

    public abstract boolean isWritable();

    public void requestLeadership() throws LogNotAvailableException {
    }

    public List<LogSequenceNumber> logStatusEditBatch(List<StatusEdit> edits) throws LogNotAvailableException {
        List<LogSequenceNumber> batch = new ArrayList<>();
        for (StatusEdit edit : edits) {
            batch.add(logStatusEdit(edit));
        }
        return batch;
    }

}

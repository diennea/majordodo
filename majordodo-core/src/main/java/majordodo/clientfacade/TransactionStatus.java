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
package majordodo.clientfacade;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Set;

/**
 * Status of a Transaction
 *
 * @author enrico.olivelli
 */
public class TransactionStatus {

    private final long transactionId;
    private final long creationTimestamp;
    private final long preparedTasks;
    private final Set<String> taskTypes;

    public TransactionStatus(long transactionId, long creationTimestamp, long preparedTasks, Set<String> taskTypes) {
        this.transactionId = transactionId;
        this.creationTimestamp = creationTimestamp;
        this.preparedTasks = preparedTasks;
        this.taskTypes = taskTypes;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public long getPreparedTasks() {
        return preparedTasks;
    }

    public Set<String> getTaskTypes() {
        return taskTypes;
    }

    @Override
    public String toString() {
        return "TransactionStatus{" + "transactionId=" + transactionId + ", creationTimestamp=" + DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).format(Instant.ofEpochMilli(creationTimestamp)) + ", preparedTasks=" + preparedTasks + ", taskTypes=" + taskTypes + '}';
    }

}

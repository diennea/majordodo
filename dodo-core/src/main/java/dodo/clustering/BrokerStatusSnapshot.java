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
import java.util.ArrayList;
import java.util.List;

/**
 * Snapshot of the status of the broker
 *
 * @author enrico.olivelli
 */
public class BrokerStatusSnapshot {

    List<Task> tasks = new ArrayList<>();
    List<WorkerStatus> workers = new ArrayList<>();
    long maxTaskId;
    LogSequenceNumber actualLogSequenceNumber;

    public BrokerStatusSnapshot(long maxTaskId, LogSequenceNumber actualLogSequenceNumber) {
        this.maxTaskId = maxTaskId;
        this.actualLogSequenceNumber = actualLogSequenceNumber;
    }

    public LogSequenceNumber getActualLogSequenceNumber() {
        return actualLogSequenceNumber;
    }

    public void setActualLogSequenceNumber(LogSequenceNumber actualLogSequenceNumber) {
        this.actualLogSequenceNumber = actualLogSequenceNumber;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public List<WorkerStatus> getWorkers() {
        return workers;
    }

    public void setWorkers(List<WorkerStatus> workers) {
        this.workers = workers;
    }

    public long getMaxTaskId() {
        return maxTaskId;
    }

    public void setMaxTaskId(long maxTaskId) {
        this.maxTaskId = maxTaskId;
    }

}

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
package dodo.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Status of a worker
 *
 * @author enrico.olivelli
 */
public final class WorkerStatus {

    public static final int STATUS_CONNECTED = 0;
    public static final int STATUS_DEAD = 1;
    public static final int STATUS_DISCONNECTED = 2;

    private String workerId;
    private int status;
    private String workerLocation;
    private long lastConnectionTs;
    private Map<String, Integer> maximumNumberOfTasks;
    private final Map<String, AtomicInteger> actualNumberOfTasks = new HashMap<>();

    public long getLastConnectionTs() {
        return lastConnectionTs;
    }

    public void setLastConnectionTs(long lastConnectionTs) {
        this.lastConnectionTs = lastConnectionTs;
    }

    public Map<String, AtomicInteger> getActualNumberOfTasks() {
        return actualNumberOfTasks;
    }

    public Map<String, Integer> getMaximumNumberOfTasks() {
        return maximumNumberOfTasks;
    }

    public void setMaximumNumberOfTasks(Map<String, Integer> maximumNumberOfTasks) {
        this.maximumNumberOfTasks = maximumNumberOfTasks;
    }

    public WorkerStatus() {
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getWorkerLocation() {
        return workerLocation;
    }

    public void setWorkerLocation(String workerLocation) {
        this.workerLocation = workerLocation;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

}

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
package dodo.task;

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
    private String processId;
    private long lastConnectionTs;

    public long getLastConnectionTs() {
        return lastConnectionTs;
    }

    public void setLastConnectionTs(long lastConnectionTs) {
        this.lastConnectionTs = lastConnectionTs;
    }

    public WorkerStatus() {
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
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

    public WorkerStatus cloneForSnapshot() {
        WorkerStatus copy = new WorkerStatus();
        copy.lastConnectionTs = this.lastConnectionTs;
        copy.processId = this.processId;
        copy.status = this.status;
        copy.workerId = this.workerId;
        copy.workerLocation = this.workerLocation;
        return copy;
    }

}

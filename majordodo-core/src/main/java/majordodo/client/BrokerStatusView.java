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
package majordodo.client;

/**
 * General view of the status of the broker
 *
 * @author enrico.olivelli
 */
public class BrokerStatusView {

    private String clusterMode;
    private long currentLedgerId;
    private long currentSequenceNumber;
    private long tasks;
    private long pendingTasks;
    private long waitingTasks;
    private long errorTasks;
    private long finishedTasks;
    private long runningTasks;

    public long getWaitingTasks() {
        return waitingTasks;
    }

    public void setWaitingTasks(long waitingTasks) {
        this.waitingTasks = waitingTasks;
    }

    public long getErrorTasks() {
        return errorTasks;
    }

    public void setErrorTasks(long errorTasks) {
        this.errorTasks = errorTasks;
    }

    public long getFinishedTasks() {
        return finishedTasks;
    }

    public void setFinishedTasks(long finishedTasks) {
        this.finishedTasks = finishedTasks;
    }

    public long getRunningTasks() {
        return runningTasks;
    }

    public void setRunningTasks(long runningTasks) {
        this.runningTasks = runningTasks;
    }
    

    public long getTasks() {
        return tasks;
    }

    public void setTasks(long tasks) {
        this.tasks = tasks;
    }

    public long getPendingTasks() {
        return pendingTasks;
    }

    public void setPendingTasks(long pendingTasks) {
        this.pendingTasks = pendingTasks;
    }
    

    public long getCurrentLedgerId() {
        return currentLedgerId;
    }

    public void setCurrentLedgerId(long currentLedgerId) {
        this.currentLedgerId = currentLedgerId;
    }

    public long getCurrentSequenceNumber() {
        return currentSequenceNumber;
    }

    public void setCurrentSequenceNumber(long currentSequenceNumber) {
        this.currentSequenceNumber = currentSequenceNumber;
    }
    
    

    public String getClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(String clusterMode) {
        this.clusterMode = clusterMode;
    }

    

}

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
 * Status of the broker
 *
 * @author enrico.olivelli
 */
public class BrokerStatus {

    private String version;
    private String status;
    private String currentLedgerId;
    private String currentSequenceNumber;
    private long tasks;
    private long pendingtasks;
    private long runningtasks;
    private long errortasks;
    private long waitingtasks;
    private long finishedtasks;

    public long getRunningtasks() {
        return runningtasks;
    }

    public void setRunningtasks(long runningtasks) {
        this.runningtasks = runningtasks;
    }

    public long getErrortasks() {
        return errortasks;
    }

    public void setErrortasks(long errortasks) {
        this.errortasks = errortasks;
    }

    public long getWaitingtasks() {
        return waitingtasks;
    }

    public void setWaitingtasks(long waitingtasks) {
        this.waitingtasks = waitingtasks;
    }

    public long getFinishedtasks() {
        return finishedtasks;
    }

    public void setFinishedtasks(long finishedtasks) {
        this.finishedtasks = finishedtasks;
    }
    
    

    public long getTasks() {
        return tasks;
    }

    public void setTasks(long tasks) {
        this.tasks = tasks;
    }

    public long getPendingtasks() {
        return pendingtasks;
    }

    public void setPendingtasks(long pendingtasks) {
        this.pendingtasks = pendingtasks;
    }

    public String getCurrentLedgerId() {
        return currentLedgerId;
    }

    public void setCurrentLedgerId(String currentLedgerId) {
        this.currentLedgerId = currentLedgerId;
    }

    public String getCurrentSequenceNumber() {
        return currentSequenceNumber;
    }

    public void setCurrentSequenceNumber(String currentSequenceNumber) {
        this.currentSequenceNumber = currentSequenceNumber;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "BrokerStatus{" + "version=" + version + ", status=" + status + ", currentLedgerId=" + currentLedgerId + ", currentSequenceNumber=" + currentSequenceNumber + ", tasks=" + tasks + ", pendingtasks=" + pendingtasks + ", runningtasks=" + runningtasks + ", errortasks=" + errortasks + ", waitingtasks=" + waitingtasks + ", finishedtasks=" + finishedtasks + '}';
    }

  
}

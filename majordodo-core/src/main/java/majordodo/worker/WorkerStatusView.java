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
package majordodo.worker;

/**
 * View of the actual status of the worker
 *
 * @author enrico.olivelli
 */
public class WorkerStatusView {

    private int runningTasks;
    private int finishedTasksPendingNotification;
    private String status;
    private String connectionInfo;

    public int getRunningTasks() {
        return runningTasks;
    }

    public void setRunningTasks(int runningTasks) {
        this.runningTasks = runningTasks;
    }

    public int getFinishedTasksPendingNotification() {
        return finishedTasksPendingNotification;
    }

    public void setFinishedTasksPendingNotification(int finishedTasksPendingNotification) {
        this.finishedTasksPendingNotification = finishedTasksPendingNotification;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getConnectionInfo() {
        return connectionInfo;
    }

    public void setConnectionInfo(String connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    
    
}

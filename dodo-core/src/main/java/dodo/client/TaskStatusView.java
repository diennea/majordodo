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
package dodo.client;

import java.util.Map;

/**
 * Visible status of a task for clients
 *
 * @author enrico.olivelli
 */
public class TaskStatusView {

    private long taskId;
    private int status;
    private String userid;
    private String workerId;
    private long createdTimestamp;
    private String parameter;
    private String result;
    private int type;    

    @Override
    public String toString() {
        return "TaskStatusView{" + "taskId=" + taskId + ", status=" + status + ", userid=" + userid + ", workerId=" + workerId + ", createdTimestamp=" + createdTimestamp + ", parameter=" + parameter + ", result=" + result + ", type=" + type + '}';
    }
    

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
    
    

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

}

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

import dodo.executors.TaskExecutorStatus;
import java.util.Map;

/**
 * A task
 *
 * @author enrico.olivelli
 */
public class Task {

    public static final int STATUS_WAITING = 0;
    public static final int STATUS_RUNNING = 1;
    public static final int STATUS_FINISHED = 2;
    public static final int STATUS_ERROR = 4;

    public static final int GROUP_ANY = 0;
    public static final int TASKTYPE_ANY = 0;

    public static int taskExecutorStatusToTaskStatus(String status) {
        switch (status) {
            case TaskExecutorStatus.ERROR:
                return STATUS_ERROR;
            case TaskExecutorStatus.FINISHED:
                return STATUS_FINISHED;
            default:
                throw new IllegalArgumentException(status + "");
        }
    }

    @Override
    public String toString() {
        return "Task{" + "type=" + type + ", parameter=" + parameter + ", result=" + result + ", createdTimestamp=" + createdTimestamp + ", status=" + status + ", taskId=" + taskId + ", tenantInfo=" + userId + ", workerId=" + workerId + '}';
    }

    private int type;
    private String parameter;
    private String result;
    private long createdTimestamp;
    private int status;
    private long taskId;
    private String userId;
    private String workerId;
    private int maxattempts;
    private int attempts;
    private long executionDeadline;

    public int getMaxattempts() {
        return maxattempts;
    }

    public void setMaxattempts(int maxattempts) {
        this.maxattempts = maxattempts;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getExecutionDeadline() {
        return executionDeadline;
    }

    public void setExecutionDeadline(long executionDeadline) {
        this.executionDeadline = executionDeadline;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String tenantInfo) {
        this.userId = tenantInfo;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
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

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(long createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    Task cloneForSnapshot() {
        Task copy = new Task();
        copy.createdTimestamp = this.createdTimestamp;
        copy.parameter = this.parameter;
        copy.result = this.result;
        copy.status = this.status;
        copy.taskId = this.taskId;
        copy.workerId = this.workerId;
        copy.userId = this.userId;
        copy.type = this.type;
        copy.attempts = this.attempts;
        copy.maxattempts = this.maxattempts;
        copy.executionDeadline = this.executionDeadline;
        return copy;
    }

}

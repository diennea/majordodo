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
    public static final int STATUS_NEEDS_RECOVERY = 3;
    public static final int STATUS_ERROR = 4;

    @Override
    public String toString() {
        return "Task{" + "type=" + type + ", parameters=" + parameters + ", status=" + status + ", taskId=" + taskId + ", queueName=" + queueName + ", workerId=" + workerId + '}';
    }

    public static int taskExecutorStatusToTaskStatus(String status) {
        switch (status) {
            case TaskExecutorStatus.ERROR:
                return STATUS_ERROR;
            case TaskExecutorStatus.FINISHED:
                return STATUS_FINISHED;
            case TaskExecutorStatus.NEEDS_RECOVERY:
                return STATUS_NEEDS_RECOVERY;
            default:
                throw new IllegalArgumentException(status + "");
        }
    }

    private String type;
    private Map<String, Object> parameters;
    private long createdTimestamp;
    private int status;
    private long taskId;
    private String queueName;
    private String workerId;

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
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

}

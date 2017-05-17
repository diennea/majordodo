
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
package majordodo.task;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import majordodo.worker.TaskExecutorStatus;

/**
 * A task
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS")
public class Task implements Delayed {

    public static final int STATUS_WAITING = 0;
    public static final int STATUS_RUNNING = 1;
    public static final int STATUS_FINISHED = 2;
    public static final int STATUS_ERROR = 4;
    public static final int STATUS_DELAYED = 5;

    public static final int GROUP_ANY = 0;
    public static final String TASKTYPE_ANY = "any";

    /**
     * On the worker a TaskExecutionFactory will be used in order to execute
     * task code
     */
    public static final String MODE_EXECUTE_FACTORY = "factory";

    /**
     * The 'data' field of the task contains a stream with Java serialized
     * object, to be deserialized in the context of a CodePool if provided
     */
    public static final String MODE_EXECUTE_OBJECT = "object";

    /**
     * Default mode (as of Majordodo 1.1.xx)
     */
    public static final String MODE_DEFAULT = MODE_EXECUTE_FACTORY;

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

    public static String statusToString(int status) {
        switch (status) {
            case STATUS_ERROR:
                return "ERROR";
            case STATUS_FINISHED:
                return "FINISHED";
            case STATUS_RUNNING:
                return "RUNNING";
            case STATUS_WAITING:
                return "WAITING";
            case STATUS_DELAYED:
                return "DELAYED";
            default:
                return "?" + status;
        }
    }

    @Override
    public String toString() {
        return "Task{" + "type=" + type + ", parameter=" + parameter + ", result=" + result + ", createdTimestamp=" + createdTimestamp + ", delay=" + getDelay(TimeUnit.SECONDS) + "s, status=" + status + " " + statusToString(status) + ", taskId=" + taskId + ", userId=" + userId + ", workerId=" + workerId + '}';
    }

    private String type;
    private String parameter;
    private String result;
    private long createdTimestamp;
    private int status;
    private long taskId;
    private String userId;
    private String workerId;
    private int maxattempts;
    private int attempts;
    private long requestedStartTime;
    private long executionDeadline;
    private String slot;
    private String codepool;
    private String mode;
    private String resources;

    public long getRequestedStartTime() {
        return requestedStartTime;
    }

    public void setRequestedStartTime(long requestedStartTime) {
        this.requestedStartTime = requestedStartTime;
    }

    public String getResources() {
        return resources;
    }

    public void setResources(String resources) {
        this.resources = resources;
    }

    public String getCodepool() {
        return codepool;
    }

    public void setCodepool(String codepool) {
        this.codepool = codepool;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getSlot() {
        return slot;
    }

    public void setSlot(String slot) {
        this.slot = slot;
    }

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

    public void setUserId(String userId) {
        this.userId = userId;
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
        copy.requestedStartTime = this.requestedStartTime;
        copy.executionDeadline = this.executionDeadline;
        copy.slot = this.slot;
        copy.resources = this.resources;
        return copy;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(requestedStartTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }
    
}

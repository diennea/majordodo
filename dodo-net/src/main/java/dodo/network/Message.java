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
package dodo.network;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A message (from broker to worker or from worker to broker)
 *
 * @author enrico.olivelli
 */
public final class Message {

    public static Message WORKER_SHUTDOWN(String workerProcessId) {
        return new Message(workerProcessId, TYPE_WORKER_SHUTDOWN, null);
    }

    public static Message TYPE_TASK_ASSIGNED(String workerProcessId, Map<String, Object> taskParameters) {
        return new Message(workerProcessId, TYPE_TASK_ASSIGNED, taskParameters);
    }

    public static Message KILL_WORKER(String workerProcessId) {
        return new Message(workerProcessId, TYPE_KILL_WORKER, null);
    }

    public static Message ERROR(String workerProcessId, Throwable error) {
        Map<String, Object> params = new HashMap<>();
        params.put("error", error + "");
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        params.put("stackTrace", writer.toString());
        return new Message(workerProcessId, TYPE_ERROR, params);
    }

    public static Message ACK(String workerProcessId) {
        return new Message(workerProcessId, TYPE_ACK, null);
    }

    public static Message WORKER_CONNECTION_REQUEST(String workerId, String processId, String location, Set<Long> actualRunningTasks) {
        Map<String, Object> params = new HashMap<>();
        params.put("workerId", workerId);
        params.put("processId", processId);
        params.put("actualRunningTasks", actualRunningTasks);
        params.put("location", location);
        return new Message(processId, TYPE_WORKER_CONNECTION_REQUEST, params);
    }

    public static Message TASK_FINISHED(String processId, long taskId, String finalStatus, String results, Throwable error) {
        Map<String, Object> params = new HashMap<>();
        params.put("taskid", taskId);
        params.put("processId", processId);
        params.put("status", finalStatus);
        params.put("result", results);
        if (error != null) {
            StringWriter r = new StringWriter();
            PrintWriter t = new PrintWriter(r);
            error.printStackTrace(t);
            params.put("error", r.toString());
        }
        return new Message(processId, TYPE_TASK_FINISHED, params);
    }

    public static Message WORKER_TASKS_REQUEST(String processId, List<Integer> groups, Map<String, Integer> availableSpace, int max) {
        Map<String, Object> params = new HashMap<>();

        params.put("processId", processId);
        params.put("groups", groups);
        params.put("availableSpace", availableSpace);
        params.put("max", max);
        return new Message(processId, TYPE_WORKER_TASKS_REQUEST, params);
    }

    public final String workerProcessId;
    public final int type;
    public final Map<String, Object> parameters;
    public String messageId;
    public String replyMessageId;

    @Override
    public String toString() {
        if (replyMessageId != null) {
            return typeToString(type) + ", parameters=" + parameters + ", id=" + messageId + ", replyMessageId=" + replyMessageId;
        } else {
            return typeToString(type) + ", parameters=" + parameters + ", id=" + messageId;
        }
    }

    public static final int TYPE_TASK_FINISHED = 1;
    public static final int TYPE_KILL_WORKER = 2;
    public static final int TYPE_ERROR = 3;
    public static final int TYPE_ACK = 4;
    public static final int TYPE_WORKER_SHUTDOWN = 5;
    public static final int TYPE_WORKER_CONNECTION_REQUEST = 6;
    public static final int TYPE_TASK_ASSIGNED = 7;
    public static final int TYPE_WORKER_TASKS_REQUEST = 8;

    public static String typeToString(int type) {
        switch (type) {
            case TYPE_TASK_FINISHED:
                return "TYPE_TASK_FINISHED";
            case TYPE_KILL_WORKER:
                return "TYPE_KILL_WORKER";
            case TYPE_ERROR:
                return "TYPE_ERROR";
            case TYPE_ACK:
                return "TYPE_ACK";
            case TYPE_WORKER_SHUTDOWN:
                return "TYPE_WORKER_SHUTDOWN";
            case TYPE_WORKER_CONNECTION_REQUEST:
                return "TYPE_WORKER_CONNECTION_REQUEST";
            case TYPE_TASK_ASSIGNED:
                return "TYPE_TASK_ASSIGNED";
            default:
                return "?" + type;
        }
    }

    public Message(String workerProcessId, int type, Map<String, Object> parameters) {
        this.workerProcessId = workerProcessId;
        this.type = type;
        this.parameters = parameters;
    }

    public String getMessageId() {
        return messageId;
    }

    public Message setMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public String getReplyMessageId() {
        return replyMessageId;
    }

    public Message setReplyMessageId(String replyMessageId) {
        this.replyMessageId = replyMessageId;
        return this;
    }

}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An action for the log
 *
 * @author enrico.olivelli
 */
public final class StatusEdit {

    public static final short TYPE_ADD_TASK = 1;
    public static final short TYPE_WORKER_CONNECTED = 2;
    public static final short TYPE_ASSIGN_TASK_TO_WORKER = 3;
    public static final short TYPE_TASK_FINISHED = 4;

    public static String typeToString(short type) {
        switch (type) {
            case TYPE_ADD_TASK:
                return "ADD_TASK";
            case TYPE_WORKER_CONNECTED:
                return "WORKER_CONNECTED";
            case TYPE_ASSIGN_TASK_TO_WORKER:
                return "ASSIGN_TASK_TO_WORKER";
            case TYPE_TASK_FINISHED:
                return "TASK_FINISHED";
            default:
                return "?" + type;
        }
    }

    public short editType;
    public int taskType;
    public long taskId;
    public int taskStatus;
    public long timestamp;
    public String parameter;
    public String userid;
    public String workerId;
    public String workerLocation;
    public String workerProcessId;
    public String result;
    public Set<Long> actualRunningTasks;

    @Override
    public String toString() {
        return "StatusEdit{" + "editType=" + typeToString(editType) + ", taskType=" + taskType + ", taskId=" + taskId + ", taskStatus=" + taskStatus + ", timestamp=" + timestamp + ", parameter=" + parameter + ", userid=" + userid + ", workerId=" + workerId + ", workerLocation=" + workerLocation + ", workerProcessId=" + workerProcessId + ", result=" + result + ", actualRunningTasks=" + actualRunningTasks + '}';
    }

    public static final StatusEdit ASSIGN_TASK_TO_WORKER(long taskId, String nodeId) {
        StatusEdit action = new StatusEdit();
        action.editType = TYPE_ASSIGN_TASK_TO_WORKER;
        action.workerId = nodeId;
        action.taskId = taskId;
        return action;
    }

    public static final StatusEdit TASK_FINISHED(long taskId, String workerId, int finalStatus, String result) {
        StatusEdit action = new StatusEdit();
        action.editType = TYPE_TASK_FINISHED;
        action.workerId = workerId;
        action.taskId = taskId;
        action.taskStatus = finalStatus;
        action.result = result;
        return action;
    }

    public static final StatusEdit ADD_TASK(long taskId, int taskType, String taskParameter, String userid) {
        StatusEdit action = new StatusEdit();
        action.editType = TYPE_ADD_TASK;
        action.parameter = taskParameter;
        action.taskType = taskType;
        action.taskId = taskId;
        action.userid = userid;
        return action;
    }

    public static final StatusEdit WORKER_CONNECTED(String workerId, String processid, String nodeLocation, Set<Long> actualRunningTasks, long timestamp) {
        StatusEdit action = new StatusEdit();
        action.editType = TYPE_WORKER_CONNECTED;
        action.timestamp = timestamp;
        action.workerId = workerId;
        action.workerLocation = nodeLocation;
        action.workerProcessId = processid;
        action.actualRunningTasks = actualRunningTasks;
        return action;
    }

    byte[] serialize() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream doo = new DataOutputStream(out);
            doo.writeShort(this.editType);
            switch (this.editType) {
                case TYPE_ADD_TASK:
                    doo.writeLong(taskId);
                    doo.writeUTF(userid);
                    doo.writeInt(taskType);
                    if (parameter != null) {
                        doo.writeUTF(parameter);
                    } else {
                        doo.writeUTF("");
                    }
                    break;
                case TYPE_WORKER_CONNECTED:
                    doo.writeUTF(workerId);
                    doo.writeUTF(workerLocation);
                    doo.writeUTF(workerProcessId);
                    doo.writeLong(timestamp);
                    doo.writeUTF(actualRunningTasks.stream().map(l -> l.toString()).collect(Collectors.joining(",")));
                    break;
                case TYPE_ASSIGN_TASK_TO_WORKER:
                    doo.writeUTF(workerId);
                    doo.writeLong(taskId);
                    break;
                case TYPE_TASK_FINISHED:
                    doo.writeLong(taskId);
                    doo.writeInt(taskStatus);
                    doo.writeUTF(workerId);
                    if (result != null) {
                        doo.writeUTF(result);
                    } else {
                        doo.writeUTF("");
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();

            }
            doo.close();
            return out.toByteArray();
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    static StatusEdit read(byte[] data) throws IOException {
        StatusEdit res = new StatusEdit();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DataInputStream doo = new DataInputStream(in);
        res.editType = doo.readShort();
        switch (res.editType) {
            case TYPE_ADD_TASK:
                res.taskId = doo.readLong();
                res.userid = doo.readUTF();
                res.taskStatus = doo.readInt();
                res.parameter = doo.readUTF();
                break;
            case TYPE_WORKER_CONNECTED:
                res.workerId = doo.readUTF();
                res.workerLocation = doo.readUTF();
                res.workerProcessId = doo.readUTF();
                res.timestamp = doo.readLong();
                res.actualRunningTasks = new HashSet<>();
                String rt = doo.readUTF();

                Stream.of(rt.split(",")).filter(s->!s.isEmpty()).map(s -> Long.parseLong(s)).forEach(res.actualRunningTasks::add);

                break;
            case TYPE_ASSIGN_TASK_TO_WORKER:
                res.workerId = doo.readUTF();
                res.taskId = doo.readLong();
                break;
            case TYPE_TASK_FINISHED:
                res.taskId = doo.readLong();
                res.taskStatus = doo.readInt();
                res.workerId = doo.readUTF();
                res.result = doo.readUTF();
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return res;

    }

}

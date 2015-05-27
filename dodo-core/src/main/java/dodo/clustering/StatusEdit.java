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

import java.util.Map;
import java.util.Set;

/**
 * An action for the log
 *
 * @author enrico.olivelli
 */
public final class StatusEdit {

    public static final short ACTION_TYPE_ADD_TASK = 1;
    public static final short ACTION_TYPE_WORKER_CONNECTED = 2;
    public static final short TYPE_ASSIGN_TASK_TO_WORKER = 3;
    public static final short TYPE_TASK_FINISHED = 4;

    public static String typeToString(short type) {
        switch (type) {
            case ACTION_TYPE_ADD_TASK:
                return "ADD_TASK";
            case ACTION_TYPE_WORKER_CONNECTED:
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
        return "StatusEdit{" + "editType=" + editType + ", taskType=" + taskType + ", taskId=" + taskId + ", taskStatus=" + taskStatus + ", timestamp=" + timestamp + ", parameter=" + parameter + ", userid=" + userid + ", workerId=" + workerId + ", workerLocation=" + workerLocation + ", workerProcessId=" + workerProcessId + ", result=" + result + ", actualRunningTasks=" + actualRunningTasks + '}';
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
        action.editType = ACTION_TYPE_ADD_TASK;
        action.parameter = taskParameter;
        action.taskType = taskType;
        action.taskId = taskId;
        action.userid = userid;
        return action;
    }

    public static final StatusEdit WORKER_CONNECTED(String workerId, String processid, String nodeLocation, Set<Long> actualRunningTasks, long timestamp) {
        StatusEdit action = new StatusEdit();
        action.editType = ACTION_TYPE_WORKER_CONNECTED;
        action.timestamp = timestamp;
        action.workerId = workerId;
        action.workerLocation = nodeLocation;
        action.workerProcessId = processid;
        action.actualRunningTasks = actualRunningTasks;
        return action;
    }

}

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
public final class Action {

    public static final short ACTION_TYPE_ADD_TASK = 1;
    public static final short ACTION_TYPE_WORKER_REGISTERED = 2;
    public static final short TYPE_ASSIGN_TASK_TO_WORKER = 3;
    public static final short TYPE_TASK_FINISHED = 4;

    public short actionType;
    public String queueName;
    public String taskType;
    public long taskId;
    public String queueTag;
    public String taskParameter;
    public String workerId;
    public String workerLocation;
    public Map<String, Integer> maximumNumberOfTasksPerTag;

    public static final Action ASSIGN_TASK_TO_WORKER(long taskId, String nodeId) {
        Action action = new Action();
        action.actionType = TYPE_ASSIGN_TASK_TO_WORKER;
        action.workerId = nodeId;
        action.taskId = taskId;
        return action;
    }

    public static final Action TASK_FINISHED(long taskId, String nodeId) {
        Action action = new Action();
        action.actionType = TYPE_TASK_FINISHED;
        action.workerId = nodeId;
        action.taskId = taskId;
        return action;
    }

    public static final Action ADD_TASK(String queueName, String taskType, String taskParameter, String queueTag) {
        Action action = new Action();
        action.actionType = ACTION_TYPE_ADD_TASK;
        action.queueName = queueName;
        action.taskType = taskType;
        action.taskParameter = taskParameter;
        action.queueTag = queueTag;
        return action;
    }

    public static final Action NODE_REGISTERED(String nodeId, String nodeLocation, Map<String, Integer> maximumNumberOfTasksPerTag) {
        Action action = new Action();
        action.actionType = ACTION_TYPE_WORKER_REGISTERED;
        action.workerId = nodeId;
        action.workerLocation = nodeLocation;
        action.maximumNumberOfTasksPerTag = maximumNumberOfTasksPerTag;
        return action;
    }

}

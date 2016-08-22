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
package majordodo.clientfacade;

import java.util.ArrayList;
import java.util.List;

/**
 * View of the actual tasksheap
 *
 * @author enrico.olivelli
 */
public class HeapStatusView {

    private List<TaskStatus> tasks = new ArrayList<>();

    public List<TaskStatus> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskStatus> tasks) {
        this.tasks = tasks;
    }

    public static class TaskStatus {

        private long taskId;
        private String taskType;
        private int group;        

        @Override
        public String toString() {
            return "{" + "taskId=" + taskId + ", taskType=" + taskType + ", group=" + group + '}';
        }        

        public long getTaskId() {
            return taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public String getTaskType() {
            return taskType;
        }

        public void setTaskType(String taskType) {
            this.taskType = taskType;
        }

        public int getGroup() {
            return group;
        }

        public void setGroup(int group) {
            this.group = group;
        }

    }
}

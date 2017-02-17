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

import java.util.Objects;

/**
 * Aggregates the actual number of tasks running for a given user on a worker
 *
 * @author enrico.olivelli
 */
public final class TaskTypeUser {

    public final String taskType;
    public final String userId;
    public final int hashCode;

    public TaskTypeUser(String taskType, String userId) {
        this.taskType = taskType;
        this.userId = userId;
        int hash = 3;
        hash = 79 * hash + Objects.hashCode(this.taskType);
        hash = 79 * hash + Objects.hashCode(this.userId);
        this.hashCode = hash;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TaskTypeUser other = (TaskTypeUser) obj;
        if (this.hashCode != other.hashCode) {
            return false;
        }
        if (!Objects.equals(this.taskType, other.taskType)) {
            return false;
        }
        if (!Objects.equals(this.userId, other.userId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TaskTypeUser{" + "taskType=" + taskType + ", userId=" + userId + ", hashCode=" + hashCode + '}';
    }

}

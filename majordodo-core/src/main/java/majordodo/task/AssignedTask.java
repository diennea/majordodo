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

/**
 * Information about a Task which is running (assigned)
 *
 * @author enrico.olivelli
 */
public final class AssignedTask {

    public final long taskid;
    public final String[] resourceIds;
    public final String resources;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2")
    public AssignedTask(long taskid, String[] resourceIds, String resources) {
        this.taskid = taskid;
        this.resourceIds = resourceIds;
        this.resources = resources;
    }

    @Override
    public String toString() {
        return "{" + "taskid=" + taskid + ", resources=" + resources + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (int) (this.taskid ^ (this.taskid >>> 32));
        return hash;
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
        final AssignedTask other = (AssignedTask) obj;
        if (this.taskid != other.taskid) {
            return false;
        }
        return true;
    }

}

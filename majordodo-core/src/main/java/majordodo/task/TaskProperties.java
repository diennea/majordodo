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

import java.util.Arrays;

/**
 * Describes runtime properties of a Task, such as "group" and the usage of resources
 *
 * @author enrico.olivelli
 */
public final class TaskProperties {

    public final int groupId;
    public final String[] resources;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2")
    public TaskProperties(int groupId, String[] resources) {
        this.groupId = groupId;
        this.resources = resources;
    }

    @Override
    public String toString() {
        return "TaskProperties{" + "groupId=" + groupId + ", resources=" + Arrays.toString(resources) + '}';
    }

}

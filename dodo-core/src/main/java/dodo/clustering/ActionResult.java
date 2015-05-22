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

import dodo.scheduler.WorkerManager;

/**
 * Result of an action
 *
 * @author enrico.olivelli
 */
public class ActionResult {

    public final long taskId;
    public final Throwable error;

    @Override
    public String toString() {
        return "{" + "taskId=" + taskId + ", error=" + error + '}';
    }

    private ActionResult(long taskId, Throwable error) {
        this.taskId = taskId;
        this.error = error;
    }

    public static ActionResult ERROR(Throwable error) {
        return new ActionResult(0, error);
    }

    public static ActionResult TASKID(long taskId) {
        return new ActionResult(taskId, null);
    }

    public static ActionResult OK() {
        return new ActionResult(0, null);
    }

}

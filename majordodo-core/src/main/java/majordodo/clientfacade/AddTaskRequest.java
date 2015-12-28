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

/**
 * Task submission
 *
 * @author enrico.olivelli
 */
public class AddTaskRequest {

    public final long transaction;
    public final String taskType;
    public final String userId;
    public final String data;
    public final int maxattempts;
    public final int attempt;
    public final long deadline;
    public final String slot;
    public final String codepool;
    public final String mode;

    public AddTaskRequest(long transaction, String taskType, String userId, String parameter, int maxattemps, long deadline, String slot, int attempts,String codepool,String mode) {
        this.transaction = transaction;
        this.attempt = attempts;
        this.taskType = taskType;
        this.userId = userId;
        this.data = parameter;
        this.maxattempts = maxattemps;
        this.deadline = deadline;
        this.slot = slot;
        this.codepool=codepool;
        this.mode=mode;
    }

}

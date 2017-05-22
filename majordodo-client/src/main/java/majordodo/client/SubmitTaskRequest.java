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
package majordodo.client;

/**
 * Submit Task Request
 *
 * @author enrico.olivelli
 */
public class SubmitTaskRequest {

    private String userid;
    private String tasktype;
    private String data;
    private int maxattempts;
    private int attempt;
    private String slot;
    private long requestedStartTime;
    private long timeToLive;
    private String codePoolId;
    private String mode;

    /**
     * Task will be executed using a TaskExecutorFactory which will use the "data" as a value with arbitrary meaning
     */
    public static final String MODE_FACTORY = "factory";
    /**
     * Task will be executed using a TaskExecutor which is obtained deserializing the "data" field, which is to be serialized using the provided methods
     */
    public static final String MODE_OBJECT = "object";

    public String getCodePoolId() {
        return codePoolId;
    }

    public void setCodePoolId(String codePoolId) {
        this.codePoolId = codePoolId;
    }

    public String getMode() {
        return mode;
    }

    /**
     * @see #MODE_FACTORY
     * @see #MODE_OBJECT
     * @param mode
     */
    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getTasktype() {
        return tasktype;
    }

    public void setTasktype(String tasktype) {
        this.tasktype = tasktype;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getMaxattempts() {
        return maxattempts;
    }

    public void setMaxattempts(int maxattempts) {
        this.maxattempts = maxattempts;
    }

    public String getSlot() {
        return slot;
    }

    public void setSlot(String slot) {
        this.slot = slot;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
    }

    public long getRequestedStartTime() {
        return requestedStartTime;
    }

    public void setRequestedStartTime(long requestedStartTime) {
        this.requestedStartTime = requestedStartTime;
    }

}

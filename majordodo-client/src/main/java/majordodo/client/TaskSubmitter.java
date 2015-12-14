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

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Utility for Tasks Submission
 *
 * @author enrico.olivelli
 */
public class TaskSubmitter {

    private String codePoolId;
    private String userId = "user";
    private String tasktype = "task";
    private ClientConnection connection;
    private int maxAttempts = 1;
    private long timeToLive = 0;
    private long codePoolTimeToLive = 60000; // one minute, just for demos
    private final Map<String, String> codePoolIdByClass = new HashMap<>();

    public TaskSubmitter(ClientConnection connection) {
        this.connection = connection;
    }

    public TaskSubmitter timeToLive(int timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    public TaskSubmitter maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
    }

    public TaskSubmitter tasktype(String tasktype) {
        this.tasktype = tasktype;
        return this;
    }

    public TaskSubmitter userId(String userId) {
        this.userId = userId;
        return this;
    }

    public TaskSubmitter codePoolId(String codePoolId) {
        this.codePoolId = codePoolId;
        return this;
    }

    public SubmitTaskResponse submitTask(Object executor) throws ClientException {
        return submitTask(null, executor);
    }

    public SubmitTaskResponse submitTask(String slot, Object executor) throws ClientException {
        boolean classMode = false;
        if (executor instanceof Class) {
            classMode = true;
        }
        if (codePoolId == null) {
            try {
                if (classMode) {
                    ensureCodePoolFromExecutorClass((Class) executor);
                } else {
                    ensureCodePoolFromExecutorClass(executor.getClass());
                }
            } catch (Exception err) {
                throw new ClientException(err);
            }
        }
        SubmitTaskRequest request = new SubmitTaskRequest();
        request.setMode(SubmitTaskRequest.MODE_OBJECT);
        request.setUserid(userId);
        request.setCodePoolId(codePoolId);
        try {
            if (classMode) {
                request.setData(CodePoolUtils.serializeExecutor(executor));
            } else {
                request.setData(CodePoolUtils.serializeExecutor(executor));
            }
        } catch (Exception err) {
            throw new ClientException(err);
        }
        request.setMaxattempts(maxAttempts);
        request.setSlot(slot);
        request.setTasktype(tasktype);
        request.setTimeToLive(timeToLive);
        return connection.submitTask(request);
    }

    private void ensureCodePoolFromExecutorClass(Class<? extends Object> aClass) throws Exception {
        String id = codePoolIdByClass.get(aClass.getName());
        if (id != null) {
            this.codePoolId = id;
        }
        byte[] codePoolData = CodePoolUtils.createCodePoolDataFromClass(aClass);
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] digested = digest.digest(codePoolData);
        id = Base64.getEncoder().encodeToString(digested).replace("=", "");
        if (connection.getCodePoolStatus(id) != null) {
            codePoolIdByClass.put(aClass.getName(), id);
            codePoolId = id;
        } else {
            CreateCodePoolRequest createCodePoolRequest = new CreateCodePoolRequest();
            createCodePoolRequest.setCodePoolData(CodePoolUtils.encodeCodePoolData(new ByteArrayInputStream(codePoolData)));
            createCodePoolRequest.setTtl(codePoolTimeToLive);
            createCodePoolRequest.setCodePoolID(id);
            connection.createCodePool(createCodePoolRequest);
            codePoolIdByClass.put(aClass.getName(), id);
            codePoolId = id;
        }
    }

    public String getCodePoolId() {
        return codePoolId;
    }

    public String getUserId() {
        return userId;
    }

    public String getTasktype() {
        return tasktype;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    public long getCodePoolTimeToLive() {
        return codePoolTimeToLive;
    }

}

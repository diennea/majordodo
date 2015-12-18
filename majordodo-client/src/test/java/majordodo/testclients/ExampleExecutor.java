package majordodo.testclients;

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
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import majordodo.executors.TaskExecutor;

/**
 * Example executor
 *
 * @author enrico.olivelli
 */
public class ExampleExecutor extends TaskExecutor implements Serializable {

    @Override
    public String executeTask(Map<String, Object> parameters) throws Exception {
        String userId = (String) parameters.get(PARAMETER_USERID);
        long taskId = (Long) parameters.get(PARAMETER_TASKID);
        int attempt = (Integer) parameters.get(PARAMETER_ATTEMPT);
        String tasktype = (String) parameters.get(PARAMETER_TASKTYPE);
        String here = InetAddress.getLocalHost().getHostName();
        return "Code executed on " + here + " for user " + userId + " (parameters=" + parameters + ")";
    }

}

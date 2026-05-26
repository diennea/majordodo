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
package majordodo.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import majordodo.task.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the HTTP API, both for embedded and for standalone installation
 *
 * @author enrico.olivelli
 */
public class WorkerHttpAPIImplementation {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerHttpAPIImplementation.class);

    public static void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        WorkerCore worker = (WorkerCore) JVMWorkersRegistry.getLocalWorker();

        String view = req.getParameter("view");
        if (view == null) {
            view = "status";
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("ok", "true");
        switch (view) {

            case "tasks":
                if (worker != null) {
                    majordodo.worker.WorkerStatusView status = worker.createWorkerStatusView();
                    resultMap.put("status", status.getStatus());
                    resultMap.put("processId", worker.getProcessId());
                    resultMap.put("location", worker.getLocation());
                    resultMap.put("workerId", worker.getWorkerId());
                    resultMap.put("runningTasks", status.getRunningTasks());
                    Map<Long, String> tasks = new HashMap<>(worker.getRunningTasks());
                    resultMap.put("tasks", tasks);
                    resultMap.put("pendingNotifications", worker.getPendingFinishedTaskNotifications());
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "status":
            default: {
                if (worker != null) {
                    majordodo.worker.WorkerStatusView status = worker.createWorkerStatusView();
                    resultMap.put("status", status.getStatus());
                    resultMap.put("processId", worker.getProcessId());
                    resultMap.put("location", worker.getLocation());
                    resultMap.put("workerId", worker.getWorkerId());
                    resultMap.put("runningTasks", status.getRunningTasks());
                    resultMap.put("finishedTasksPendingNotification", status.getFinishedTasksPendingNotification());
                    resultMap.put("connectionInfo", status.getConnectionInfo());
                    resultMap.put("version", Broker.VERSION());
                } else {
                    resultMap.put("status", "not_started");
                    resultMap.put("version", Broker.VERSION());
                }
                break;
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        LOGGER.debug("GET  -> " + resultMap);
        String s = mapper.writeValueAsString(resultMap);
        byte[] res = s.getBytes(StandardCharsets.UTF_8);

        resp.setContentLength(res.length);

        resp.setContentType("application/json;charset=utf-8");
        resp.getOutputStream().write(res);
        resp.getOutputStream().close();
    }

}

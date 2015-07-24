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
package majordodo.embedded;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import majordodo.client.SubmitTaskResult;
import majordodo.client.TaskStatusView;
import majordodo.network.jvm.JVMBrokersRegistry;
import majordodo.task.Broker;
import majordodo.task.Task;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Servlet per API Client Majordodo Broker
 *
 * @author enrico.olivelli
 */
public class EmbeddedBrokerAPIServlet extends HttpServlet {

    private static final Logger LOGGER = Logger.getLogger(EmbeddedBrokerAPIServlet.class.getName());

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Broker broker = (Broker) JVMBrokersRegistry.lookupBroker("embedded");

        String view = req.getParameter("view");
        if (view == null) {
            view = "overview";
        }
        Map<String, Object> resultMap = new HashMap<>();
        switch (view) {
            case "overview":
                if (broker != null) {
                    resultMap.put("workers", broker.getClient().getAllWorkers());
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "workers":
                if (broker != null) {
                    resultMap.put("workers", broker.getClient().getAllWorkers());
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "tasks":
                if (broker != null) {

                    String filter = req.getParameter("filter");
                    if (filter == null) {
                        filter = "all";
                    }
                    List<Map<String, Object>> tt;
                    Predicate<TaskStatusView> filterPred;
                    switch (filter) {
                        case "all":
                            filterPred = (t) -> true;

                            break;
                        case "waiting":
                            filterPred = (t) -> t.getStatus() == Task.STATUS_WAITING;
                            break;
                        case "running":
                            filterPred = (t)
                                    -> t.getStatus() == Task.STATUS_RUNNING;
                            break;
                        case "error":
                            filterPred = (t) -> t.getStatus() == Task.STATUS_ERROR;
                            break;
                        case "finished":
                            filterPred = (t) -> t.getStatus() == Task.STATUS_FINISHED;
                            break;
                        default:
                            filterPred = (t) -> false;
                    }
                    tt = broker.getClient().getAllTasks().stream().filter(filterPred).map(t -> {
                        Map<String, Object> map = new HashMap<>();
                        map.put("taskId", t.getTaskId());
                        map.put("userId", t.getUser());
                        map.put("deadline", t.getExecutionDeadline());
                        map.put("createdTimestamp", t.getCreatedTimestamp());
                        map.put("maxattempts", t.getMaxattempts());
                        map.put("attempts", t.getAttempts());
                        map.put("tasktype", t.getType());
                        map.put("workerId", t.getWorkerId());
                        map.put("result", t.getResult());
                        map.put("data", t.getData());
                        String status;
                        switch (t.getStatus()) {
                            case Task.STATUS_ERROR:
                                status = "error";
                                break;
                            case Task.STATUS_FINISHED:
                                status = "finished";
                                break;
                            case Task.STATUS_RUNNING:
                                status = "running";
                                break;
                            case Task.STATUS_WAITING:
                                status = "waiting";
                                break;
                            default:
                                status = "?" + t.getStatus();
                        }
                        map.put("status", status);
                        return map;
                    }).collect(Collectors.toList());

                    resultMap.put("tasks", tt);
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                } else {
                    resultMap.put("status", "not_started");
                }
                break;

        }

        ObjectMapper mapper = new ObjectMapper();
        LOGGER.log(Level.FINE, "GET  -> " + resultMap);
        String s = mapper.writeValueAsString(resultMap);
        byte[] res = s.getBytes(StandardCharsets.UTF_8);

        resp.setContentLength(res.length);

        resp.setContentType("application/json;charset=utf-8");
        resp.getOutputStream().write(res);
        resp.getOutputStream().close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Broker broker = (Broker) JVMBrokersRegistry.lookupBroker("embedded");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> data = mapper.readValue(req.getInputStream(), Map.class);
        LOGGER.log(Level.FINE, "POST " + data + " broker=" + broker);
        String action = data.get("action") + "";
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("action", action);
        if (data.containsKey("transaction")) {
            resultMap.put("transaction", data.get("transaction"));
        }
        if (broker == null) {
            resultMap.put("error", "broker_not_started");
            resultMap.put("ok", false);
        } else {
            switch (action) {
                case "submitTask": {
                    String type = (String) data.get("tasktype");
                    String user = (String) data.get("userid");
                    String parameters = (String) data.get("data");
                    String _maxattempts = (String) data.get("maxattempts");
                    long transaction = 0;
                    if (data.containsKey("transaction")) {
                        transaction = Long.parseLong(data.get("transaction") + "");
                    }
                    int maxattempts = 1;
                    if (_maxattempts != null) {
                        maxattempts = Integer.parseInt(_maxattempts);
                    }
                    String _deadline = (String) data.get("deadline");
                    long deadline = 0;
                    if (_deadline != null) {
                        deadline = Long.parseLong(_deadline);
                    }
                    String slot = (String) data.get("slot");
                    if (slot != null && slot.trim().isEmpty()) {
                        slot = null;
                    }

                    SubmitTaskResult result;
                    try {
                        result = broker.getClient().submitTask(transaction, type, user, parameters, maxattempts, deadline, slot);
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        throw new ServletException("error " + err);
                    }
                    long taskId = result.getTaskId();
                    String error = result.getError();

                    resultMap.put("taskId", taskId);
                    resultMap.put("error", error);
                    resultMap.put("ok", error != null && taskId > 0);
                    break;
                }
                case "beginTransaction": {
                    String error = null;
                    long transactionId = 0;
                    try {
                        transactionId = broker.getClient().beginTransaction();
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        error = err + "";
                    }
                    resultMap.put("transaction", transactionId);
                    resultMap.put("ok", error != null && transactionId > 0);
                    resultMap.put("error", error);
                    break;
                }
                case "commitTransaction": {
                    long transactionId = Long.parseLong(data.get("transaction") + "");
                    String error = null;
                    try {
                        broker.getClient().commitTransaction(transactionId);
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        error = err + "";
                    }
                    resultMap.put("ok", true);
                    resultMap.put("transaction", transactionId);
                    resultMap.put("ok", error != null && transactionId > 0);
                    break;
                }
                case "rollbackTransaction": {
                    long transactionId = Long.parseLong(data.get("transaction") + "");
                    String error = null;
                    try {
                        broker.getClient().rollbackTransaction(transactionId);
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        error = err + "";
                    }
                    resultMap.put("ok", true);
                    resultMap.put("transaction", transactionId);
                    resultMap.put("ok", error != null && transactionId > 0);
                    break;
                }
                default: {
                    resultMap.put("ok", false);
                    resultMap.put("error", "bad action " + action);
                }
            }
        }

        LOGGER.log(Level.FINE, "POST " + data + " -> " + resultMap);
        String s = mapper.writeValueAsString(resultMap);
        byte[] res = s.getBytes(StandardCharsets.UTF_8);

        resp.setContentLength(res.length);

        resp.setContentType("application/json;charset=utf-8");
        resp.getOutputStream().write(res);
        resp.getOutputStream().close();

    }

}

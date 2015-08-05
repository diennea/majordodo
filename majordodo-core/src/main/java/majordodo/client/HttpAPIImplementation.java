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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import majordodo.network.jvm.JVMBrokersRegistry;
import majordodo.task.Broker;
import majordodo.task.Task;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Implementation of the HTTP API, both for embedded and for standalone
 * installation
 *
 * @author enrico.olivelli
 */
public class HttpAPIImplementation {

    private static final Logger LOGGER = Logger.getLogger(HttpAPIImplementation.class.getName());

    public static void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Broker broker = (Broker) JVMBrokersRegistry.getDefaultBroker();

        String view = req.getParameter("view");
        if (view == null) {
            view = "overview";
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("ok", "true");
        switch (view) {
            case "status": {
                if (broker != null) {
                    BrokerStatusView status = broker.getClient().getBrokerStatus();
                    resultMap.put("status", status.getClusterMode());
                    resultMap.put("currentLedgerId", status.getCurrentLedgerId() + "");
                    resultMap.put("currentSequenceNumber", status.getCurrentSequenceNumber() + "");
                    resultMap.put("version", Broker.VERSION());
                    resultMap.put("tasks", status.getTasks());
                    resultMap.put("pendingtasks", status.getPendingTasks());
                    resultMap.put("runningtasks", status.getRunningTasks());
                    resultMap.put("errortasks", status.getErrorTasks());
                    resultMap.put("waitingtasks", status.getWaitingTasks());
                    resultMap.put("finishedtasks", status.getFinishedTasks());
                } else {
                    resultMap.put("status", "not_started");
                    resultMap.put("version", Broker.VERSION());
                }
                break;
            }
            case "overview":
                if (broker != null) {
                    resultMap.put("workers", broker.getClient().getAllWorkers());
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "tasksheap":
                if (broker != null) {
                    resultMap.put("tasksheap", broker.getClient().getHeapStatus());
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
            case "task":
                if (broker != null) {
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                    try {
                        long id = Long.parseLong(req.getParameter("taskId") + "");
                        TaskStatusView task = broker.getClient().getTask(id);
                        Map<String, Object> map = serializeTaskForClient(task);
                        resultMap.put("task", map);
                    } catch (NumberFormatException err) {
                        resultMap.put("ok", false);
                        resultMap.put("error", "bad task id " + err);
                    }
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "tasks":
                if (broker != null) {
                    int max = 100;
                    if (req.getParameter("max") != null) {
                        max = Integer.parseInt(req.getParameter("max"));
                    }
                    String filter = req.getParameter("filter");
                    if (filter == null) {
                        filter = "all";
                    }
                    resultMap.put("max", max);
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
                        Map<String, Object> map = serializeTaskForClient(t);
                        return map;
                    }).collect(Collectors.toList());

                    resultMap.put("tasks", tt);
                    resultMap.put("count", tt.size());
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

    private static Map<String, Object> serializeTaskForClient(TaskStatusView t) {
        Map<String, Object> map = new HashMap<>();
        if (t == null) {
            return map;
        }
        map.put("taskId", t.getTaskId());
        map.put("userId", t.getUser());
        map.put("deadline", t.getExecutionDeadline());
        map.put("createdTimestamp", t.getCreatedTimestamp());
        map.put("maxattempts", t.getMaxattempts());
        map.put("attempts", t.getAttempts());
        map.put("tasktype", t.getType());
        map.put("workerId", t.getWorkerId());
        map.put("result", t.getResult());
        map.put("slot", t.getSlot());
        map.put("data", t.getData());
        int taskStatus = t.getStatus();
        String status = TaskStatusView.convertTaskStatusForClient(taskStatus);
        map.put("status", status);
        return map;
    }

    public static void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Broker broker = (Broker) JVMBrokersRegistry.getDefaultBroker();
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
                    if (error == null) {
                        error = "";
                    }

                    resultMap.put("taskId", taskId);
                    resultMap.put("error", error);
                    resultMap.put("ok", error.isEmpty() && taskId > 0);
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
                    resultMap.put("ok", error == null && transactionId > 0);
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
                    resultMap.put("ok", error == null && transactionId > 0);
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
                    resultMap.put("ok", error == null && transactionId > 0);
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

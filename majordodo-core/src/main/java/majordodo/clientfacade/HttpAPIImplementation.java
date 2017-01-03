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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Function;
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
 * Implementation of the HTTP API, both for embedded and for standalone installation
 *
 * @author enrico.olivelli
 */
public class HttpAPIImplementation {

    private static final Logger LOGGER = Logger.getLogger(HttpAPIImplementation.class.getName());

    private static AuthenticatedUser login(HttpServletRequest req) {
        Broker broker = (Broker) JVMBrokersRegistry.getDefaultBroker();
        if (broker == null) {
            return null;
        }
        String authHeader = req.getHeader("Authorization");
        if (authHeader != null) {
            StringTokenizer st = new StringTokenizer(authHeader);
            if (st.hasMoreTokens()) {
                String basic = st.nextToken();
                if (basic.equalsIgnoreCase("Basic")) {
                    try {
                        String credentials = new String(java.util.Base64.getDecoder().decode(st.nextToken()), StandardCharsets.UTF_8);
                        int p = credentials.indexOf(":");
                        if (p != -1) {
                            String login = credentials.substring(0, p).trim();
                            String password = credentials.substring(p + 1).trim();
                            return broker.getAuthenticationManager().login(login, password);
                        }
                    } catch (IllegalArgumentException a) {
                    }

                }
            }
        }
        return null;
    }

    public static void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Broker broker = (Broker) JVMBrokersRegistry.getDefaultBroker();

        String view = req.getParameter("view");
        if (view == null) {
            view = "overview";
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("ok", "true");
        if (broker == null) {
            resultMap.put("error", "broker_not_started");
            resultMap.put("ok", "false");
        } else {
            if (broker.getConfiguration().isApiCorsEnabled()) {
                resp.setHeader("Access-Control-Allow-Origin", "*");
            }
        }
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
            case "resources":
                if (broker != null) {
                    String workerId = req.getParameter("workerId");
                    if (workerId == null || workerId.isEmpty()) {
                        resultMap.put("resources", broker.getClient().getAllResources());
                    } else {
                        resultMap.put("workerId", workerId);
                        resultMap.put("resources", broker.getClient().getAllResourcesForWorker(workerId));
                    }

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
            case "slots":
                if (broker != null) {
                    resultMap.put("slots", broker.getClient().getSlotsStatusView());
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "transactions":
                if (broker != null) {
                    resultMap.put("transactions", broker.getClient().getTransactionsStatusView());
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
            case "codePool":
                if (broker != null) {
                    resultMap.put("status", broker.getClient().getBrokerStatus());
                    try {
                        String id = req.getParameter("codePoolId") + "";
                        CodePoolView codePool = broker.getClient().getCodePool(id);
                        Map<String, Object> map = serializeCodePoolForClient(codePool);
                        resultMap.put("codePool", codePool);
                    } catch (Exception err) {
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
                    String worker = req.getParameter("workerId");

                    String tasktype = req.getParameter("tasktype");

                    String status = req.getParameter("status");
                    if (status == null) {
                        status = "all";
                    }
                    String slot = req.getParameter("slot");

                    String user = req.getParameter("userId");

                    resultMap.put("max", max);
                    List<Map<String, Object>> tt;
                    Predicate<TaskStatusView> filterPred;
                    Predicate<TaskStatusView> filterWorker;
                    Predicate<TaskStatusView> filterTasktype;
                    Predicate<TaskStatusView> filterSlots;
                    Predicate<TaskStatusView> filterUser;
                    switch (status) {
                        case "all":
                            filterPred = (t) -> true;
                            break;
                        case "waiting":
                            filterPred = (t) -> t.getStatus() == Task.STATUS_WAITING;
                            break;
                        case "running":
                            filterPred = (t) -> t.getStatus() == Task.STATUS_RUNNING;
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

                    if (worker != null && !worker.isEmpty()) {
                        filterWorker = (t) -> {
                            return worker.equalsIgnoreCase(t.getWorkerId());
                        };
                    } else {
                        filterWorker = (t) -> true;
                    }
                    if (slot != null && !slot.isEmpty()) {
                        filterSlots = (t) -> {
                            return slot.equalsIgnoreCase(t.getSlot());
                        };
                    } else {
                        filterSlots = (t) -> true;
                    }

                    if (user != null && !user.isEmpty()) {
                        filterUser = (t) -> {
                            return user.equalsIgnoreCase(t.getUser());
                        };
                    } else {
                        filterUser = (t) -> true;
                    }

                    if (tasktype != null && !tasktype.isEmpty()) {
                        filterTasktype = (t) -> {
                            return tasktype.equalsIgnoreCase(t.getType());
                        };
                    } else {
                        filterTasktype = (t) -> true;
                    }

                    tt = broker.getClient().getAllTasks().stream().filter(filterPred).filter(filterUser).filter(filterSlots).filter(filterWorker).filter(filterTasktype).limit(max).map(t -> {
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

            case "tasksoverview":
                if (broker != null) {
                    String worker = req.getParameter("workerId");
                    String tasktype = req.getParameter("tasktype");
                    String filter = req.getParameter("filter");
                    if (filter == null) {
                        filter = "all";
                    }
                    List<Map<String, Object>> tt;
                    Predicate<TaskStatusView> filterPred;
                    Predicate<TaskStatusView> filterWorker;
                    Predicate<TaskStatusView> filterTasktype;
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

                    if (worker != null && !worker.isEmpty()) {
                        filterWorker = (t) -> {
                            return worker.equalsIgnoreCase(t.getWorkerId());
                        };
                    } else {
                        filterWorker = (t) -> true;
                    }

                    if (tasktype != null && !tasktype.isEmpty()) {
                        filterTasktype = (t) -> {
                            return tasktype.equalsIgnoreCase(t.getType());
                        };
                    } else {
                        filterTasktype = (t) -> true;
                    }

                    Map<String, Long> groupByTaskType = broker.getClient().getAllTasks().stream()
                        .filter(filterPred).filter(filterWorker).filter(filterTasktype)
                        .collect(
                            Collectors.groupingBy(TaskStatusView::getType, Collectors.counting())
                        );

                    resultMap.put("tasks", groupByTaskType);
                    resultMap.put("count", groupByTaskType.values().stream().collect(Collectors.summingLong((l) -> l)));
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

    private static Map<String, Object> serializeCodePoolForClient(CodePoolView t) {
        Map<String, Object> map = new HashMap<>();
        if (t == null) {
            return map;
        }
        map.put("codePoolId", t.getCodePoolId());
        map.put("creationTimestamp", t.getCreationTimestamp() + "");
        map.put("ttl", t.getTtl() + "");
        return map;
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
        if (t.getMode() != null) {
            map.put("mode", t.getMode());
        }
        if (t.getCodePoolId() != null) {
            map.put("codePoolId", t.getCodePoolId());
        }
        if (t.getResources() != null) {
            map.put("resources", t.getResources());
        }
        int taskStatus = t.getStatus();
        String status = TaskStatusView.convertTaskStatusForClient(taskStatus);
        map.put("status", status);
        return map;
    }

    public static void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Broker broker = (Broker) JVMBrokersRegistry.getDefaultBroker();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> data = mapper.readValue(req.getInputStream(), Map.class);
        AuthenticatedUser auth_user = login(req);
        LOGGER.log(Level.FINE, "POST " + data + " broker=" + broker + ", user: " + auth_user);
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
            if (broker.getConfiguration().isApiCorsEnabled()) {
                resp.setHeader("Access-Control-Allow-Origin", "*");
            }
            if (auth_user == null) {
                resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Majordodo broker API");
                return;
            }
            switch (action) {
                case "submitTask": {
                    String error = "";
                    String type = (String) data.get("tasktype");
                    String user = (String) data.get("userid");
                    String parameters = (String) data.get("data");
                    String _maxattempts = (String) data.get("maxattempts");
                    String _attempt = (String) data.get("attempt");
                    long transaction = 0;
                    if (data.containsKey("transaction")) {
                        transaction = Long.parseLong(data.get("transaction") + "");
                    }
                    int attempt = 0;
                    if (_attempt != null) {
                        attempt = Integer.parseInt(_attempt);
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
                    String codepool = (String) data.get("codePoolId");
                    if (codepool != null && codepool.trim().isEmpty()) {
                        codepool = null;
                    }
                    String mode = (String) data.get("mode");
                    if (mode != null && mode.trim().isEmpty()) {
                        mode = null;
                    }

                    if (auth_user.getRole() != UserRole.ADMINISTRATOR
                        && !auth_user.getUserId().equals(user)) {
                        resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Majordodo broker API");
                        return;
                    }

                    SubmitTaskResult result;
                    try {
                        result = broker.getClient().submitTask(new AddTaskRequest(transaction, type, user, parameters, maxattempts, deadline, slot, attempt, codepool, mode));
                        long taskId = result.getTaskId();
                        resultMap.put("taskId", taskId);
                        resultMap.put("result", result.getOutcome());
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        error = err + "";
                    }
                    resultMap.put("error", error);
                    resultMap.put("ok", error.isEmpty());
                    break;
                }
                case "submitTasks": {
                    String error = "";
                    List<Map<String, Object>> results = new ArrayList<>();
                    List<Map<String, Object>> tasks = (List<Map<String, Object>>) data.get("tasks");
                    if (tasks == null) {
                        error = "tasks element not present in json";
                    } else {
                        List<AddTaskRequest> requests = new ArrayList<>();
                        for (Map<String, Object> task : tasks) {
                            String type = (String) task.get("tasktype");
                            String user = (String) task.get("userid");
                            if (auth_user.getRole() != UserRole.ADMINISTRATOR
                                && !auth_user.getUserId().equals(user)) {
                                resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Majordodo broker API");
                                return;
                            }
                            String parameters = (String) task.get("data");
                            String _maxattempts = (String) task.get("maxattempts");
                            String _attempt = (String) task.get("attempt");
                            long transaction = 0;
                            if (task.containsKey("transaction")) {
                                transaction = Long.parseLong(task.get("transaction") + "");
                            }
                            int maxattempts = 1;
                            if (_maxattempts != null) {
                                maxattempts = Integer.parseInt(_maxattempts);
                            }
                            int attempt = 0;
                            if (_attempt != null) {
                                attempt = Integer.parseInt(_attempt);
                            }
                            String _deadline = (String) task.get("deadline");
                            long deadline = 0;
                            if (_deadline != null) {
                                deadline = Long.parseLong(_deadline);
                            }
                            String slot = (String) task.get("slot");
                            if (slot != null && slot.trim().isEmpty()) {
                                slot = null;
                            }
                            String codepool = (String) data.get("codePoolId");
                            if (codepool != null && codepool.trim().isEmpty()) {
                                codepool = null;
                            }
                            String mode = (String) data.get("mode");
                            if (mode != null && mode.trim().isEmpty()) {
                                mode = null;
                            }

                            requests.add(new AddTaskRequest(transaction, type, user, parameters, maxattempts, deadline, slot, attempt, codepool, mode));
                        }
                        try {
                            List<SubmitTaskResult> addresults = broker.getClient().submitTasks(requests);
                            int i = 0;
                            for (AddTaskRequest addreq : requests) {
                                SubmitTaskResult result;

                                result = addresults.get(i++);

                                long taskId = result.getTaskId();
                                Map<String, Object> resForTask = new HashMap<>();
                                resForTask.put("taskId", taskId);
                                if (addreq.transaction > 0) {
                                    resForTask.put("transaction", addreq.transaction);
                                }
                                resForTask.put("result", result.getOutcome());
                                results.add(resForTask);
                            }
                        } catch (Exception err) {
                            // very bad error                            
                            LOGGER.log(Level.SEVERE, "error for " + data, err);
                            error = err + "";
                        }

                    }
                    resultMap.put("results", results);
                    resultMap.put("ok", error.isEmpty());
                    resultMap.put("error", error);
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
                case "deleteCodePool": {
                    String error = "";
                    String id = (String) data.get("id");
                    try {
                        broker.getClient().deleteCodePool(id);
                        resultMap.put("ok", true);
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        error = err + "";
                    }

                    if (!error.isEmpty()) {
                        resultMap.put("ok", false);
                        resultMap.put("error", error);
                    }
                    break;
                }
                case "createCodePool": {
                    String error = "";
                    String id = (String) data.get("id");
                    long ttl = 0;
                    String codepooldata = (String) data.get("data");

                    if (data.containsKey("ttl")) {
                        ttl = Long.parseLong(data.get("ttl") + "");
                    }

                    if (auth_user.getRole() != UserRole.ADMINISTRATOR) {
                        resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Majordodo broker API");
                        return;
                    }

                    CreateCodePoolResult result;
                    try {
                        result = broker.getClient().createCodePool(new CreateCodePoolRequest(id, System.currentTimeMillis(), ttl, Base64.getDecoder().decode(codepooldata)));
                        resultMap.put("ok", result.ok);
                        resultMap.put("result", result.outcome);
                    } catch (Exception err) {
                        LOGGER.log(Level.SEVERE, "error for " + data, err);
                        error = err + "";
                        result = null;
                    }

                    if (!error.isEmpty()) {
                        resultMap.put("ok", false);
                        resultMap.put("error", error);
                    }
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

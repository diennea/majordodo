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
package majordodo.broker.http;

import majordodo.client.SubmitTaskResult;
import majordodo.task.Task;
import majordodo.client.TaskStatusView;
import majordodo.client.WorkerStatusView;
import majordodo.task.Broker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import majordodo.network.jvm.JVMBrokersRegistry;

/**
 * Client API
 *
 * @author enrico.olivelli
 */
public class ClientAPI {

    @GET
    @Path("/tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ClientTask> getAllTasks() {
        Broker broker = JVMBrokersRegistry.getDefaultBroker();
        List<TaskStatusView> tasks = broker.getClient().getAllTasks();
        List<ClientTask> result = new ArrayList<>();
        for (TaskStatusView t : tasks) {
            result.add(createClientTask(t));
        }
        return result;
    }

    @GET
    @Path("/workers")
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkerStatus> getAllWorkers() {
        Broker broker = JVMBrokersRegistry.getDefaultBroker();
        List<WorkerStatusView> tasks = broker.getClient().getAllWorkers();
        List<WorkerStatus> result = new ArrayList<>();
        for (WorkerStatusView t : tasks) {
            result.add(createWorkerStatus(t));
        }
        return result;
    }

    @POST
    @Path("/tasks")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ClientSubmitTaskResult submitTask(Map<String, Object> data) {
        System.out.println("submitTask:" + data);
        String type = (String) data.get("type");
        String user = (String) data.get("user");
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

        SubmitTaskResult result;
        try {
            Broker broker = JVMBrokersRegistry.getDefaultBroker();
            result = broker.getClient().submitTask(transaction, type, user, parameters, maxattempts, deadline, slot);
        } catch (Exception err) {
            throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
        }
        long taskId = result.getTaskId();
        String error = result.getError();
        if (error != null) {
            return new ClientSubmitTaskResult(null, false, error);
        } else {
            return new ClientSubmitTaskResult(getTask(taskId), true, error);
        }
    }

    @GET
    @Path("/tasks/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ClientTask getTask(@PathParam("id") long id) {
        Broker broker = JVMBrokersRegistry.getDefaultBroker();
        TaskStatusView task = broker.getClient().getTask(id);
        return createClientTask(task);
    }

    @GET
    @Path("/version")
    public String getVersion() {
        return Broker.VERSION();
    }

    private static ClientTask createClientTask(TaskStatusView t) {
        if (t == null) {
            return null;
        }
        ClientTask tt = new ClientTask();
        tt.setId(t.getTaskId());
        switch (t.getStatus()) {
            case Task.STATUS_ERROR:
                tt.setStatus("error");
                break;
            case Task.STATUS_FINISHED:
                tt.setStatus("finished");
                break;
            case Task.STATUS_RUNNING:
                tt.setStatus("running");
                break;
            case Task.STATUS_WAITING:
                tt.setStatus("waiting");
                break;
            default:
                tt.setStatus("unknown(" + t.getStatus() + ")");
        }
        tt.setCreationTimestamp(t.getCreatedTimestamp());
        tt.setType(t.getType());
        tt.setWorkerId(t.getWorkerId());
        tt.setData(t.getData());
        tt.setResult(t.getResult());
        tt.setUser(t.getUser());

        return tt;
    }

    private WorkerStatus createWorkerStatus(WorkerStatusView t) {
        WorkerStatus res = new WorkerStatus();
        res.setId(t.getId());
        res.setLocation(t.getLocation());
        res.setProcessId(t.getProcessId());
        res.setStatus(t.getStatus());
        return res;
    }

}

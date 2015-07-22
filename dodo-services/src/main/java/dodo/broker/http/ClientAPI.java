/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.broker.http;

import dodo.broker.BrokerMain;
import dodo.client.SubmitTaskResult;
import dodo.clustering.Task;
import dodo.client.TaskStatusView;
import dodo.client.WorkerStatusView;
import dodo.task.Broker;
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
        List<TaskStatusView> tasks = BrokerMain.runningInstance.getBroker().getClient().getAllTasks();
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
        List<WorkerStatusView> tasks = BrokerMain.runningInstance.getBroker().getClient().getAllWorkers();
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

        if (BrokerMain.runningInstance == null) {
            throw new WebApplicationException("broker not yet started", Status.PRECONDITION_FAILED);
        }
        SubmitTaskResult result;
        try {
            result = BrokerMain.runningInstance.getBroker().getClient().submitTask(transaction, type, user, parameters, maxattempts, deadline, slot);
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
        TaskStatusView task = BrokerMain.runningInstance.getBroker().getClient().getTask(id);
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

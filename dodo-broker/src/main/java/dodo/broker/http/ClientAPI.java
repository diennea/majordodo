/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.broker.http;

import dodo.broker.BrokerMain;
import dodo.clustering.Task;
import dodo.task.TaskStatusView;
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

    @POST
    @Path("/tasks")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ClientTask submitTask(Map<String, Object> data) {
        System.out.println("submitTask:" + data);
        String type = (String) data.get("type");
        String queueName = (String) data.get("queueName");
        String tag = (String) data.get("tag");
        if (type == null || type.isEmpty()) {
            throw new WebApplicationException(Status.BAD_REQUEST);
        }
        Map<String, Object> parameters = null;
        try {
            parameters = (Map< String, Object>) data.get("parameters");
        } catch (Throwable t) {
            throw new WebApplicationException(Status.BAD_REQUEST);
        }
        try {
            long taskId = BrokerMain.runningInstance.getBroker().getClient().submitTask(type, queueName, queueName, parameters);
            return getTask(taskId);
        } catch (Exception err) {
            err.printStackTrace();
            throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("/tasks/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public ClientTask getTask(@PathParam("id") long id) {
        TaskStatusView task = BrokerMain.runningInstance.getBroker().getClient().getTask(id);
        return createClientTask(task);
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
            case Task.STATUS_NEEDS_RECOVERY:
                tt.setStatus("needs_recovery");
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
        tt.setQueueName(t.getQueueName());
        tt.setWorkerId(t.getWorkerId());
        tt.setParameters(t.getParameters());
        tt.setResults(t.getResults());

        return tt;
    }

}

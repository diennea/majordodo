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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Client API
 *
 * @author enrico.olivelli
 */
public class ClientAPI {

    @GET
    @Path("/all")
    public List<ClientTask> getAllTasks() {
        List<TaskStatusView> tasks = BrokerMain.broker.getClient().getAllTasks();
        List<ClientTask> result = new ArrayList<>();
        for (TaskStatusView t : tasks) {
            result.add(createClientTask(t));
        }
        return result;
    }

    @GET
    @Path("/{id}")
    public ClientTask getTasks(@PathParam("id") long id) {
        TaskStatusView task = BrokerMain.broker.getClient().getTask(id);

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

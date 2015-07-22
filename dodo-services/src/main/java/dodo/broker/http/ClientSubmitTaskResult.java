/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.broker.http;

/**
 * Result of the submission
 *
 * @author enrico.olivelli
 */
public class ClientSubmitTaskResult {

    private ClientTask task;
    private boolean ok;
    private String error;

    public ClientSubmitTaskResult(ClientTask task, boolean ok, String error) {
        this.task = task;
        this.ok = ok;
        this.error = error;
    }

    public ClientTask getTask() {
        return task;
    }

    public void setTask(ClientTask task) {
        this.task = task;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

}

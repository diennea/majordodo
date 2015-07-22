/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.worker;

import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import java.util.Map;

/**
 * default exefutor factory, executes goorvy scripts
 *
 * @author enrico.olivelli
 */
public class DefaultExecutorFactory implements TaskExecutorFactory {

    public static final String TASKTYPE_GROOVYSCRIPT = "script";

    @Override
    public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
        if (taskType.equals(TASKTYPE_GROOVYSCRIPT)) {
            return new GroovyScriptTaskExecutor(parameters);
        } else {
            return new TaskExecutor();
        }
    }

}

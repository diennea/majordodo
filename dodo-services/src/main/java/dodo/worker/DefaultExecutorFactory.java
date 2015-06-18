/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.worker;

import dodo.executors.TaskExecutor;
import dodo.executors.TaskExecutorFactory;
import java.util.Map;

/**
 * default exefutor factory, executes goorvy scripts
 *
 * @author enrico.olivelli
 */
public class DefaultExecutorFactory implements TaskExecutorFactory {

    public static final int TASKTYPE_GROOVYSCRIPT = 0;

    @Override
    public TaskExecutor createTaskExecutor(int taskType, Map<String, Object> parameters) {
        if (taskType == TASKTYPE_GROOVYSCRIPT) {
            return new GroovyScriptTaskExecutor(parameters);
        } else {
            return new TaskExecutor();
        }
    }

}

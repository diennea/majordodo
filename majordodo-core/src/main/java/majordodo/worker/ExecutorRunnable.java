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
package majordodo.worker;

import majordodo.executors.TaskExecutor;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Real execution of the task
 *
 * @author enrico.olivelli
 */
public class ExecutorRunnable implements Runnable {

    private WorkerCore core;
    private Long taskId;
    private Map<String, Object> parameters;
    private TaskExecutionCallback callback;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorRunnable.class);

    public ExecutorRunnable(WorkerCore core, Long taskId, Map<String, Object> parameters, TaskExecutionCallback callback) {
        this.core = core;
        this.taskId = taskId;
        this.parameters = parameters;
        this.callback = callback;
    }

    public static interface TaskExecutionCallback {

        public void taskStatusChanged(long taskId, Map<String, Object> parameters, String finalStatus, String results, Throwable error);
    }

    @Override
    public void run() {
        long _start = System.nanoTime();
        try {
            String taskType = (String) parameters.get("tasktype");
            callback.taskStatusChanged(taskId, parameters, TaskExecutorStatus.RUNNING, null, null);
            TaskExecutor executor = core.createTaskExecutor(taskType, parameters);
            String result = executor.executeTask(parameters);
            callback.taskStatusChanged(taskId, parameters, TaskExecutorStatus.FINISHED, result, null);
        } catch (Throwable t) {
            LOGGER.error("error while executing task " + parameters, t);
            callback.taskStatusChanged(taskId, parameters, TaskExecutorStatus.ERROR, null, t);
        } finally {
            if (LOGGER.isEnabledForLevel(Level.DEBUG)) {
                long _end = System.nanoTime();
                LOGGER.debug("task time " + parameters + " " + (_end - _start) + " ns");
            }
        }
    }
}

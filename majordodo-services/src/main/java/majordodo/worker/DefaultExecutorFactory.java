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
import majordodo.executors.TaskExecutorFactory;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * default exefutor factory, executes goorvy scripts
 *
 * @author enrico.olivelli
 */
public class DefaultExecutorFactory implements TaskExecutorFactory {

    public static final String TASKTYPE_GROOVYSCRIPT = "script";
    private static final Logger LOGGER = Logger.getLogger(DefaultExecutorFactory.class.getName());

    @Override
    public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
        LOGGER.log(Level.FINE, "createTaskExecutor {0}", parameters);
        if (taskType.equals(TASKTYPE_GROOVYSCRIPT)) {
            return new GroovyScriptTaskExecutor(parameters);
        } else {
            return new TaskExecutor();
        }
    }

}

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

import java.util.Map;
import majordodo.codepools.CodePoolClassloadersManager;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;

/**
 * TaskExecutorFactory with handles CodePools
 *
 * @author enrico.olivelli
 */
public class CodePoolAwareExecutorFactory implements TaskExecutorFactory {

    private final TaskExecutorFactory inner;
    private final CodePoolClassloadersManager classloadersManager;

    public CodePoolAwareExecutorFactory(TaskExecutorFactory inner, CodePoolClassloadersManager classloadersManager) {
        this.inner = inner;
        this.classloadersManager = classloadersManager;
    }

    @Override
    public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
        String codePoolId = (String) parameters.get("codepool");
        if (codePoolId == null) {
            return inner.createTaskExecutor(taskType, parameters);
        }
        try {            
            ClassLoader cl = classloadersManager.getCodePoolClassloader(codePoolId);
            ClassLoader tccl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(cl);
            try {
                return inner.createTaskExecutor(taskType, parameters);
            } finally {
                Thread.currentThread().setContextClassLoader(tccl);
            }
        } catch (Exception error) {
            return new TaskExecutor() {
                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {
                    throw error;
                }
            };
        }

    }

}

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.net.URLClassLoader;
import java.util.Base64;
import java.util.Map;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import majordodo.task.Task;

/**
 * TaskExecutorFactory with handles CodePools
 *
 * @author enrico.olivelli
 */
public class TaskModeAwareExecutorFactory implements TaskExecutorFactory {

    private final TaskExecutorFactory inner;

    public TaskModeAwareExecutorFactory(TaskExecutorFactory inner) {
        this.inner = inner;
    }

    @Override
    public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
        String mode = (String) parameters.getOrDefault("mode", Task.MODE_DEFAULT);
        if (mode.equals(Task.MODE_EXECUTE_FACTORY)) {
            return inner.createTaskExecutor(taskType, parameters);
        }
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            String parameter = (String) parameters.getOrDefault("parameter", "");
            if (parameter.startsWith("base64:")) {
                parameter = parameter.substring("base64:".length());
                byte[] serializedObjectData = Base64.getDecoder().decode(parameter);
                ByteArrayInputStream ii = new ByteArrayInputStream(serializedObjectData);
                ObjectInputStream is = new ObjectInputStream(ii) {
                    @Override
                    public Class resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                        try {
                            return tccl.loadClass(desc.getName());
                        } catch (Exception e) {
                        }
                        return super.resolveClass(desc);
                    }

                };
                TaskExecutor res = (TaskExecutor) is.readUnshared();
                return res;
            } else if (parameter.startsWith("newinstance:")) {
                parameter = parameter.substring("newinstance:".length());
                Class clazz = Class.forName(parameter, true, tccl);
                TaskExecutor res = (TaskExecutor) clazz.newInstance();
                return res;
            } else {
                throw new RuntimeException("bad parameter: " + parameter);
            }
        } catch (Exception err) {
            return new TaskExecutor() {
                @Override
                public String executeTask(Map<String, Object> parameters) throws Exception {
                    throw err;
                }

            };
        }

    }

}

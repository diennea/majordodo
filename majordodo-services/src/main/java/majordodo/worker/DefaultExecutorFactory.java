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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import majordodo.executors.TaskExecutorFactoryImplementation;
import org.reflections.Reflections;

/**
 * Task Executor Factory which scans the cclasspath in order to find
 * implementations of tasks
 *
 * @author enrico.olivelli
 */
public class DefaultExecutorFactory implements TaskExecutorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultExecutorFactory.class);
    private final List<TaskExecutorFactory> factories = new ArrayList<>();

    public DefaultExecutorFactory() {
        discoverFromClasspath();
    }

    @Override
    public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
        for (TaskExecutorFactory factory : factories) {
            TaskExecutor e = factory.createTaskExecutor(taskType, parameters);
            if (e != null) {
                return e;
            }
        }
        return new TaskExecutor();
    }

    private void discoverFromClasspath() {
        try {
            Reflections reflections = new Reflections();
            LOGGER.error("looking on classpath for classes annotated with @TaskExecutorFactoryImplementation");
            Set<Class<?>> classes = reflections.getTypesAnnotatedWith(TaskExecutorFactoryImplementation.class);

            for (Class c : classes) {
                LOGGER.error("found class {}", c);
                TaskExecutorFactory factory = (TaskExecutorFactory) c.newInstance();
                factories.add(factory);
            }
            if (factories.isEmpty()) {
                LOGGER.error("no class annotated with @TaskExecutorFactoryImplementation was found on the class path. This worker does not implement any tasktype!");
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

}

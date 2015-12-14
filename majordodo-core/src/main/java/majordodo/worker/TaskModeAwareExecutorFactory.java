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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import majordodo.task.Task;
import org.apache.commons.io.IOUtils;

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
                URLClassLoader cc = (URLClassLoader) tccl;                
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

    /**
     * Serializes an Executor for execution with
     * {@link  TaskModeAwareExecutorFactory}
     *
     * @param executor
     * @return
     * @throws Exception
     */
    public static String serializeExecutor(TaskExecutor executor) throws Exception {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(oo);
        os.writeUnshared(executor);
        os.close();
        return Base64.getEncoder().encodeToString(oo.toByteArray());
    }

    public static byte[] createZipWithOneEntry(String entryfilename, byte[] filedata) throws IOException {
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        try (ZipOutputStream zipper = new ZipOutputStream(oo, StandardCharsets.UTF_8);) {
            int posslash = entryfilename.indexOf('/');
            if (posslash >= 0) { // simple case for directory
                String dire = entryfilename.substring(0, posslash);
                ZipEntry entry = new ZipEntry(dire);
                zipper.putNextEntry(entry);
                zipper.closeEntry();
            }
            ZipEntry entry = new ZipEntry(entryfilename);
            zipper.putNextEntry(entry);
            zipper.write(filedata);
            zipper.closeEntry();
        }
        return oo.toByteArray();
    }

}

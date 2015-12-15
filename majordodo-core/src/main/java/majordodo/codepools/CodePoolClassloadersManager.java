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
package majordodo.codepools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;
import majordodo.worker.WorkerCore;

/**
 * Manager for classloaders
 *
 * @author enrico.olivelli
 */
public class CodePoolClassloadersManager {

    private final Path codeTemporaryDirectory;
    private final WeakHashMap<String, CodePoolClassloader> classloaders = new WeakHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final WorkerCore parent;

    public CodePoolClassloadersManager(Path codeTemporaryDirectory, WorkerCore parent) throws IOException {
        this.codeTemporaryDirectory = codeTemporaryDirectory;
        Files.createDirectories(codeTemporaryDirectory);
        this.parent = parent;
    }

    public ClassLoader getCodePoolClassloader(String codePoolId) throws Exception {
        lock.lock();
        try {
            CodePoolClassloader cl = classloaders.get(codePoolId);
            if (cl != null) {
                return cl;
            }
            byte[] data = parent.downloadCodePool(codePoolId);
            cl = new CodePoolClassloader(Thread.currentThread().getContextClassLoader(), codePoolId, data, codeTemporaryDirectory);            
            classloaders.put(codePoolId, cl);
            return cl;
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        List<CodePoolClassloader> loaders;
        lock.lock();
        try {
            loaders = new ArrayList<>(classloaders.values());
            classloaders.clear();
        } finally {
            lock.unlock();
        }

        for (CodePoolClassloader cl : loaders) {
            try {
                cl.close();
            } catch (IOException err) {
            }
        }
    }

}

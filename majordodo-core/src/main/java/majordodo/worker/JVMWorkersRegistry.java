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

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM classloader based registry for Workers
 *
 * @author enrico.olivelli
 */
public class JVMWorkersRegistry {

    private static final ConcurrentHashMap<String, WorkerCore> workers = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(JVMWorkersRegistry.class);
    private static String lastRegisteredWorker = "";

    public static void registerWorker(String id, WorkerCore worker) {
        workers.put(id, worker);
        lastRegisteredWorker = id;
    }

    public static <T extends WorkerCore> T lookupBroker(String id) {
        return (T) workers.get(id);
    }

    public static <T extends WorkerCore> T getLocalWorker() {
        return (T) workers.get(lastRegisteredWorker);
    }

    public static void clear() {
        workers.clear();
    }

    public static void unregisterWorker(String brokerId) {
        workers.remove(brokerId);
    }
}

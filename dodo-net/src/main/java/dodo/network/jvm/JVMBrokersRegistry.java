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
package dodo.network.jvm;

import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM classloader based registry for Brokers
 *
 * @author enrico.olivelli
 */
public class JVMBrokersRegistry {

    private static final ConcurrentHashMap<String, JVMBrokerSupportInterface> brokers = new ConcurrentHashMap<>();

    public static void registerBroker(String id, JVMBrokerSupportInterface broker) {
        brokers.put(id, broker);
    }

    public static <T extends JVMBrokerSupportInterface> T lookupBroker(String id) {
        return (T) brokers.get(id);
    }

    public static void clear() {
        brokers.clear();
    }

    public static void unregisterBroker(String brokerId) {
        brokers.remove(brokerId);
    }
}

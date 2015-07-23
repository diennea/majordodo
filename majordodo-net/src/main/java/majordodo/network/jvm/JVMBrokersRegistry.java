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
package majordodo.network.jvm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JVM classloader based registry for Brokers
 *
 * @author enrico.olivelli
 */
public class JVMBrokersRegistry {

    private static final ConcurrentHashMap<String, JVMBrokerSupportInterface> brokers = new ConcurrentHashMap<>();
    private static final Logger LOGGER = Logger.getLogger(JVMBrokersRegistry.class.getName());

    public static void registerBroker(String id, JVMBrokerSupportInterface broker) {
        LOGGER.log(Level.SEVERE, "registerBroker {0}", id);
        brokers.put(id, broker);
    }

    public static <T extends JVMBrokerSupportInterface> T lookupBroker(String id) {
        return (T) brokers.get(id);
    }

    public static void clear() {
        LOGGER.log(Level.SEVERE, "clear");
        brokers.clear();
    }

    public static void unregisterBroker(String brokerId) {
        LOGGER.log(Level.SEVERE, "unregisterBroker {0}", brokerId);
        brokers.remove(brokerId);
    }
}

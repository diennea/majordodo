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
package majordodo.task;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Managers execution slots
 *
 * @author enrico.olivelli
 */
class SlotsManager {

    private final ConcurrentHashMap<String, Long> actualSlots = new ConcurrentHashMap<>();

    public Set<String> getBusySlots() {
        return new HashSet<>(actualSlots.keySet());
    }

    public ConcurrentHashMap<String, Long> getActualSlots() {
        return actualSlots;
    }

    public boolean assignSlot(String slot, Long taskId) {
        return actualSlots.putIfAbsent(slot, taskId) == null;
    }

    public void releaseSlot(String slot) {
        actualSlots.remove(slot);
    }

    void loadBusySlots(Map<String, Long> busySlots) {
        actualSlots.clear();
        actualSlots.putAll(busySlots);

    }

}

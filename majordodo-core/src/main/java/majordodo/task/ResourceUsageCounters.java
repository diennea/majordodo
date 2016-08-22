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

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import majordodo.utils.IntCounter;

/**
 * Counters on resources usage
 *
 * @author enrico.olivelli
 */
public class ResourceUsageCounters {

    Map<String, IntCounter> counters = new HashMap<>();
    ConcurrentLinkedQueue<String> releasedResources = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<String> usedResources = new ConcurrentLinkedQueue<>();
    AtomicBoolean clearRequested = new AtomicBoolean();

    public Map<String, Integer> getCountersView() {
        while (true) {
            try {
                Map<String, Integer> copy = new HashMap<>();
                for (Map.Entry<String, IntCounter> counter : counters.entrySet()) {
                    copy.put(counter.getKey(), counter.getValue().count);
                }
                return copy;
            } catch (ConcurrentModificationException ok) {
            }
        }
    }

    void updateResourceCounters() {
        // write access to "counters" is done only here, inside the writeLock of "TasksHeap"        
        if (clearRequested.compareAndSet(true, false)) {
            for (IntCounter c : counters.values()) {
                c.count = 0;
            }
        }
        for (String id = releasedResources.poll(); id != null; id = releasedResources.poll()) {
            IntCounter count = counters.get(id);
            if (count != null) {
                count.count--;
            } else {
                count = new IntCounter();
                count.count = -1;
                counters.put(id, count);
            }
        }
        for (String id = usedResources.poll(); id != null; id = usedResources.poll()) {
            IntCounter count = counters.get(id);
            if (count == null) {
                count = new IntCounter();
                counters.put(id, count);
            }
            count.count++;
        }
    }

    void clear() {
        clearRequested.set(true);
    }

    void useResources(String[] resourceIds) {
        // this method can be called by any thread, but real write access to "counters" is to be done only inside the writeLock of "TasksHeap"
        if (resourceIds != null) {
            usedResources.addAll(Arrays.asList(resourceIds));
        }
    }

    void releaseResources(String[] resourceIds) {
        // this method can be called by any thread, but real write access to "counters" is to be done only inside the writeLock of "TasksHeap"
        if (resourceIds != null) {
            releasedResources.addAll(Arrays.asList(resourceIds));
        }
    }

}

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
package dodo.scheduler;

import dodo.worker.BrokerSideConnection;
import dodo.task.Task;
import dodo.task.TaskQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runtime status manager for a Node
 *
 * @author enrico.olivelli
 */
public class WorkerManager {

    private final WorkerStatus node;
    private final Scheduler scheduler;
    private BrokerSideConnection connection;

    public WorkerManager(WorkerStatus node, Scheduler scheduler) {
        this.node = node;
        this.scheduler = scheduler;
    }

    public void nodeTaskFinished(Task task, TaskQueue queue) {
        scheduler.nodeSlotIsAvailable(node, queue.getTag());
    }

    public void nodeConnected() {
        node.getMaximumNumberOfTasks().forEach((tag, max) -> {
            int remaining;
            AtomicInteger actualCount = node.getActualNumberOfTasks().get(tag);
            if (actualCount != null) {
                remaining = max - actualCount.get();
            } else {
                remaining = max;
            }
            for (int i = 0; i < remaining; i++) {
                scheduler.nodeSlotIsAvailable(node, tag);
            }
        });
    }

    public void taskAssigned(Task task) {
        System.out.println("taskAssigned " + task.getTaskId());
    }

}

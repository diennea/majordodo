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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runtime status manager for a Node
 *
 * @author enrico.olivelli
 */
public class NodeManager {

    private final Node node;
    private final Scheduler scheduler;
    private final AtomicInteger actualNumberOfTasks = new AtomicInteger();
    private final int maximumNumberOfTasks;

    public NodeManager(Node node, Scheduler scheduler) {
        this.node = node;
        this.maximumNumberOfTasks = node.getMaximumNumberOfTasks();
        this.scheduler = scheduler;
    }

    public void nodeTaskFinished(long taskId) {
        scheduler.nodeSlotIsAvailable(node);

    }

    public void nodeConnected(int numberOfTasks) {
        actualNumberOfTasks.set(numberOfTasks);
        int remaining = maximumNumberOfTasks - numberOfTasks;
        for (int i = 0; i < remaining; i++) {
            scheduler.nodeSlotIsAvailable(node);
        }
    }

}

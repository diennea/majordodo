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

import dodo.clustering.Action;
import dodo.clustering.LogNotAvailableException;
import dodo.task.Organizer;
import dodo.task.Task;
import dodo.task.TaskQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default simple scheduler
 *
 * @author enrico.olivelli
 */
public class DefaultScheduler extends Scheduler {

    private static final Logger LOGGER = Logger.getLogger(DefaultScheduler.class.getName());

    Organizer organizer;

    List<PendingNode> pendingNodes = new ArrayList<>();

    private static final class PendingNode {

        String nodeId;
        String tag;

        public PendingNode(String nodeId, String tag) {
            this.nodeId = nodeId;
            this.tag = tag;
        }

    }

    public DefaultScheduler(Organizer organizer) {
        this.organizer = organizer;
    }

    @Override
    public void nodeSlotIsAvailable(Node node, String tag) {
        Task task = getNewTask(tag);
        boolean done = false;
        if (task != null) {
            try {
                Action action = Action.ASSIGN_TASK_TO_NODE(task.getTaskId(), node.getNodeId());
                organizer.executeAction(action);
                done = true;
            } catch (LogNotAvailableException | InterruptedException notAvailable) {
                LOGGER.log(Level.SEVERE, "cannot assign new task", notAvailable);
            }
        }
        if (!done) {
            pendingNodes.add(new PendingNode(node.getNodeId(), tag));
        }

    }

    private Task getNewTask(String tag) {
        try {
            return organizer.readonlyAccess(() -> {
                for (TaskQueue q : organizer.queues.values()) {
                    if (q.getTag().equals(tag)) {
                        Task peek = q.peekNext();
                        if (peek != null) {
                            return peek;
                        }
                    }
                }
                return null;
            });
        } catch (Exception err) {
            LOGGER.log(Level.SEVERE, "cannot get a new task", err);
            return null;
        }
    }

}

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
package dodo.worker;

import dodo.clustering.Action;
import dodo.network.Channel;
import dodo.network.Message;
import dodo.network.ReplyCallback;
import dodo.scheduler.WorkerManager;
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection to a node from the broker side
 *
 * @author enrico.olivelli
 */
public class BrokerSideConnection {

    private final String workerId;
    private final String workerProcessId;
    private final long connectionId;
    private final Map<String, Integer> maximumNumberOfTasks;
    private final String location;
    private WorkerManager manager;
    private final Channel channel;

    private static final AtomicLong sessionId = new AtomicLong();

    public BrokerSideConnection(String workerId, String workerProcessId, Map<String, Integer> maximumNumberOfTasks, String location, Channel channel) {
        this.workerId = workerId;
        this.workerProcessId = workerProcessId;
        this.connectionId = sessionId.incrementAndGet();
        this.maximumNumberOfTasks = maximumNumberOfTasks;
        this.location = location;
        this.channel = channel;
    }

    public String getLocation() {
        return location;
    }

    public Map<String, Integer> getMaximumNumberOfTasks() {
        return maximumNumberOfTasks;
    }

    public String getWorkerProcessId() {
        return workerProcessId;
    }

    public long getConnectionId() {
        return connectionId;
    }

    String getWorkerId() {
        return workerId;
    }

    void activate(WorkerManager manager) {
        manager.activateConnection(this);
        this.manager = manager;
    }

    void receivedMessageFromWorker(Message message) {
        String brokerToken = manager.getBroker().getLeaderToken();
        if (!message.workerProcessId.equals(workerProcessId)) {
            // worker process is not the same as the one we expect, send a "die" message and invalidate the channel
            Message killWorkerMessage = Message.KILL_WORKER(brokerToken, workerProcessId);
            channel.sendMessageWithAsyncReply(killWorkerMessage, new ReplyCallback() {

                @Override
                public void replyReceived(Message originalMessage, Message message, Throwable error) {
                    // any way we are closing the channel
                    channel.invalidate();
                }
            });
            return;
        }

        switch (message.type) {
            case Message.TYPE_TASK_FINISHED:
                long taskid = (Long) message.parameters.get("taskid");
                Action action = Action.TASK_FINISHED(taskid, this.workerId);
                try {
                    manager.getBroker().executeAction(action, (a, result) -> {
                        if (result.error != null) {
                            channel.sendReplyMessage(message, Message.ERROR(brokerToken, workerProcessId, result.error));
                        } else {
                            channel.sendReplyMessage(message, Message.ACK(brokerToken, workerProcessId));
                        }
                    });
                } catch (InterruptedException | InvalidActionException err) {
                    channel.sendReplyMessage(message, Message.ERROR(brokerToken, workerProcessId, err));
                }

        }

    }

    void answerConnectionNotAcceptedAndClose(Message connectionRequestMessage, Throwable ex) {
        channel.sendReplyMessage(connectionRequestMessage, Message.ERROR(workerId, workerProcessId, ex));
    }

}

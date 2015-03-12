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

import dodo.clustering.Event;
import dodo.network.Channel;
import dodo.network.InboundMessagesReceiver;
import dodo.network.Message;
import dodo.scheduler.WorkerManager;
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import dodo.task.Task;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Connection to a node from the broker side
 *
 * @author enrico.olivelli
 */
public class BrokerSideConnection implements InboundMessagesReceiver {

    private String workerId;
    private String workerProcessId;
    private long connectionId;
    private Map<String, Integer> maximumNumberOfTasks;
    private String location;
    private WorkerManager manager;
    private Channel channel;
    private Broker broker;

    private static final AtomicLong sessionId = new AtomicLong();

    public BrokerSideConnection() {
        connectionId = sessionId.incrementAndGet();
    }

    public Broker getBroker() {
        return broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public void setWorkerProcessId(String workerProcessId) {
        this.workerProcessId = workerProcessId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }

    public void setMaximumNumberOfTasks(Map<String, Integer> maximumNumberOfTasks) {
        this.maximumNumberOfTasks = maximumNumberOfTasks;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
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

    @Override
    public void messageReceived(Message message) {
        System.out.println("[BROKER] receivedMessageFromWorker " + message);
        if (workerProcessId != null && !message.workerProcessId.equals(workerProcessId)) {
            // worker process is not the same as the one we expect, send a "die" message and close the channel
            Message killWorkerMessage = Message.KILL_WORKER(workerProcessId);
            channel.sendMessageWithAsyncReply(killWorkerMessage, (Message originalMessage, Message message1, Throwable error) -> {
                // any way we are closing the channel
                channel.close();
            });
            return;
        }

        switch (message.type) {
            case Message.TYPE_WORKER_CONNECTION_REQUEST: {
                try {
                    this.workerId = (String) message.parameters.get("workerId");
                    if (this.workerId == null) {
                        answerConnectionNotAcceptedAndClose(message, new Exception("invalid workerid " + workerId));
                        return;
                    }
                    this.workerProcessId = (String) message.parameters.get("processId");
                    this.location = (String) message.parameters.get("location");
                    this.maximumNumberOfTasks = (Map<String, Integer>) message.parameters.get("maximumThreadPerTag");
                    Set<Long> actualRunningTasks = (Set<Long>) message.parameters.get("actualRunningTasks");
                    BrokerSideConnection actual = this.broker.getAcceptor().getWorkersConnections().get(workerId);
                    if (actual != null) {
                        answerConnectionNotAcceptedAndClose(message, new Exception("already connected from " + workerId));
                        return;
                    }
                    Event action = Event.NODE_REGISTERED(workerId, location, maximumNumberOfTasks, actualRunningTasks);
                    this.broker.logEvent(action, (a, result) -> {
                        if (result.error != null) {
                            answerConnectionNotAcceptedAndClose(message, result.error);
                            return;
                        }
                        broker.getAcceptor().getWorkersConnections().put(workerId, this);
                        this.manager = broker.getWorkerManager(workerId);
                        answerConnectionAccepted(message);
                        broker.getScheduler().workerConnected(workerId, maximumNumberOfTasks, actualRunningTasks);
                    });
                } catch (InterruptedException ex) {
                    answerConnectionNotAcceptedAndClose(message, ex);
                } catch (InvalidActionException ex) {
                    answerConnectionNotAcceptedAndClose(message, ex);
                }
                break;
            }
            case Message.TYPE_TASK_FINISHED:
                long taskid = (Long) message.parameters.get("taskid");
                Event action = Event.TASK_FINISHED(taskid, this.workerId);
                try {
                    manager.getBroker().logEvent(action, (a, result) -> {
                        if (result.error != null) {
                            channel.sendReplyMessage(message, Message.ERROR(workerProcessId, result.error));
                        } else {
                            channel.sendReplyMessage(message, Message.ACK(workerProcessId));
                        }
                    });
                } catch (InterruptedException | InvalidActionException err) {
                    channel.sendReplyMessage(message, Message.ERROR(workerProcessId, err));
                }

        }

    }

    void answerConnectionNotAcceptedAndClose(Message connectionRequestMessage, Throwable ex) {
        channel.sendReplyMessage(connectionRequestMessage, Message.ERROR(workerProcessId, ex));
    }

    void answerConnectionAccepted(Message connectionRequestMessage) {
        channel.sendReplyMessage(connectionRequestMessage, Message.ACK(workerProcessId));
    }

    public void sendTaskAssigned(Task task) {
        Map<String, Object> params = new HashMap<>();
        params.put("taskid", task.getTaskId());
        params.put("tasktype", task.getType());
        params.putAll(task.getParameters());
        channel.sendOneWayMessage(Message.TYPE_TASK_ASSIGNED(workerProcessId, params));
    }

}

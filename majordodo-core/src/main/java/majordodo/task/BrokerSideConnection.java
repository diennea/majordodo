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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.Message;
import majordodo.network.SendResultCallback;
import majordodo.network.ServerSideConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import majordodo.codepools.CodePool;
import majordodo.network.ConnectionRequestInfo;
import majordodo.security.sasl.SaslNettyServer;

/**
 * Connection to a node from the broker side
 *
 * @author enrico.olivelli
 */
public class BrokerSideConnection implements ChannelEventListener, ServerSideConnection {

    private static final Logger LOGGER = Logger.getLogger(BrokerSideConnection.class.getName());

    private String clientId;
    private String workerProcessId;
    private long connectionId;
    private String location;
    private WorkerManager manager;
    private Channel channel;
    private Broker broker;
    private long lastReceivedMessageTs;
    private volatile SaslNettyServer saslNettyServer;
    private volatile boolean authenticated;
    private boolean requireAuthentication;
    private volatile boolean isWorker = false;
    private volatile boolean isBroker = false;
    private volatile String username;
    private static final AtomicLong SESSIONID = new AtomicLong();

    public BrokerSideConnection() {
        connectionId = SESSIONID.incrementAndGet();
    }

    public Broker getBroker() {
        return broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public boolean isRequireAuthentication() {
        return requireAuthentication;
    }

    public void setRequireAuthentication(boolean requireAuthentication) {
        this.requireAuthentication = requireAuthentication;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setWorkerProcessId(String workerProcessId) {
        this.workerProcessId = workerProcessId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
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

    public String getWorkerProcessId() {
        return workerProcessId;
    }

    public long getConnectionId() {
        return connectionId;
    }

    String getClientId() {
        return clientId;
    }

    public long getLastReceivedMessageTs() {
        return lastReceivedMessageTs;
    }

    public boolean validate() {
        return channel != null && channel.isValid();
    }

    @Override
    public void messageReceived(Message message) {
        lastReceivedMessageTs = System.currentTimeMillis();
        Channel _channel = channel;
        if (_channel == null) {
            LOGGER.log(Level.SEVERE, "receivedMessageFromWorker {0}, but channel is closed", message);
            return;
        }
        LOGGER.log(Level.FINE, "receivedMessageFromWorker {0}", message);
        switch (message.type) {
            case Message.TYPE_SASL_TOKEN_MESSAGE_REQUEST: {
                try {
                    byte[] token = (byte[]) message.parameters.get("token");
                    if (token == null) {
                        token = new byte[0];
                    }
                    String mech = (String) message.parameters.get("mech");
                    if (saslNettyServer == null) {
                        saslNettyServer = new SaslNettyServer(broker.getConfiguration().getSharedSecret(), mech);
                    }
                    byte[] responseToken = saslNettyServer.response(token);
                    Message tokenChallenge = Message.SASL_TOKEN_SERVER_RESPONSE(responseToken);
                    _channel.sendReplyMessage(message, tokenChallenge);
                } catch (Exception err) {
                    Message error = Message.ERROR(null, err);
                    _channel.sendReplyMessage(message, error);
                }
                break;
            }
            case Message.TYPE_SASL_TOKEN_MESSAGE_TOKEN: {
                try {

                    if (saslNettyServer == null) {
                        Message error = Message.ERROR(null, new Exception("Authentication failed (SASL protocol error)"));
                        _channel.sendReplyMessage(message, error);
                        return;
                    }
                    byte[] token = (byte[]) message.parameters.get("token");
                    byte[] responseToken = saslNettyServer.response(token);
                    Message tokenChallenge = Message.SASL_TOKEN_SERVER_RESPONSE(responseToken);
                    if (saslNettyServer.isComplete()) {
                        username = saslNettyServer.getUserName();
                        authenticated = true;
                        LOGGER.severe("client " + channel + " completed SASL authentication as " + username);
                        saslNettyServer = null;
                    }
                    _channel.sendReplyMessage(message, tokenChallenge);
                } catch (Exception err) {
                    if (err instanceof javax.security.sasl.SaslException) {
                        LOGGER.log(Level.SEVERE, "SASL error " + err, err);
                        Message error = Message.ERROR(null, new Exception("Authentication failed (SASL error)"));
                        _channel.sendReplyMessage(message, error);
                    } else {
                        Message error = Message.ERROR(null, err);
                        _channel.sendReplyMessage(message, error);
                    }
                }
                break;
            }
            case Message.TYPE_CONNECTION_REQUEST: {
                LOGGER.log(Level.INFO, "connection request {0}", message);
                String clientType = (String) message.parameters.get("clientType");
                if (clientType == null) {
                    // legacy workers
                    clientType = ConnectionRequestInfo.CLIENT_TYPE_WORKER;
                }

                switch (clientType) {
                    case ConnectionRequestInfo.CLIENT_TYPE_WORKER:
                        isWorker = true;
                        break;
                    case ConnectionRequestInfo.CLIENT_TYPE_BROKER:
                        isBroker = true;
                        break;
                    default:
                        answerConnectionNotAcceptedAndClose(message, new Exception("invalid clientType " + clientType));
                        return;
                }

                String sharedSecret = (String) message.parameters.get("secret");
                if (sharedSecret == null || !sharedSecret.equals(broker.getConfiguration().getSharedSecret())) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("invalid network secret"));
                    return;
                }
                if (isWorker && workerProcessId != null && !message.workerProcessId.equals(workerProcessId)) {
                    // worker process is not the same as the one we expect, send a "die" message and close the channel
                    Message killWorkerMessage = Message.KILL_WORKER(workerProcessId);
                    channel.sendMessageWithAsyncReply(killWorkerMessage, broker.getConfiguration().getNetworkTimeout(), (Message originalMessage, Message message1, Throwable error) -> {
                        // any way we are closing the channel
                        channel.close();
                    });
                    return;
                }
                String _clientId = (String) message.parameters.get("workerId");
                if (_clientId == null) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("invalid clientId " + clientId));
                    return;

                }
                if (!broker.isWritable()) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("this broker is not yet writable/leader"));
                    return;
                }

                Set<Long> actualRunningTasks = (Set<Long>) message.parameters.getOrDefault("actualRunningTasks", Collections.emptySet());
                int maxThreads = (Integer) message.parameters.getOrDefault("maxThreads", 0);
                Map<String, Integer> maxThreadsByTaskType = (Map<String, Integer>) message.parameters.getOrDefault("maxThreadsByTaskType", Collections.emptyMap());
                List<Integer> groups = (List<Integer>) message.parameters.getOrDefault("groups", Collections.emptyList());
                Set<Integer> excludedGroups = (Set<Integer>) message.parameters.getOrDefault("excludedGroups", Collections.emptySet());
                Map<String, Integer> resourceLimits = (Map<String, Integer>) message.parameters.getOrDefault("resources", Collections.emptyMap());
                int maxThreadPerUserPerTaskTypePercent = (Integer) message.parameters.getOrDefault("maxThreadPerUserPerTaskTypePercent", 0);

                this.clientId = _clientId;
                this.location = (String) message.parameters.get("location");
                this.workerProcessId = (String) message.parameters.get("processId");
                if (isWorker) {
                    LOGGER.log(Level.SEVERE, "registering worker connection " + connectionId + ", workerId:" + _clientId + ", processId=" + message.parameters.get("processId") + ", location=" + message.parameters.get("location"));
                    BrokerSideConnection actual = this.broker.getAcceptor().getActualConnectionFromWorker(_clientId);
                    if (actual != null) {
                        LOGGER.log(Level.SEVERE, "there is already a connection id: {0}, workerId:{1}, {2}", new Object[]{actual.getConnectionId(), _clientId, actual});
                        if (!actual.validate()) {
                            LOGGER.log(Level.SEVERE, "connection id: {0}, is no more valid", actual.getConnectionId());
                            actual.close();
                        } else {
                            answerConnectionNotAcceptedAndClose(message, new Exception("already connected from " + _clientId + ", processId " + actual.workerProcessId + ", location:" + actual.location + ", connectionId " + actual.connectionId + " channel " + actual.channel));
                            return;
                        }
                    }
                    try {
                        this.broker.workerConnected(clientId, workerProcessId, location, actualRunningTasks, System.currentTimeMillis());
                    } catch (LogNotAvailableException error) {
                        answerConnectionNotAcceptedAndClose(message, error);
                        return;
                    }
                } else {
                    LOGGER.log(Level.SEVERE, "registering broker peer connection " + connectionId + ", processId=" + message.parameters.get("processId") + ", location=" + message.parameters.get("location"));
                }
                channel.setName(clientId);
                channel.setRemoteHost(location);
                broker.getAcceptor().connectionAccepted(this);
                if (isWorker) {
                    this.manager = broker.getWorkers().getWorkerManager(clientId);
                    manager.applyConfiguration(maxThreads, maxThreadsByTaskType, groups, excludedGroups, resourceLimits, maxThreadPerUserPerTaskTypePercent);
                    manager.activateConnection(this);
                }
                answerConnectionAccepted(message);
                break;
            }

            case Message.TYPE_TASK_FINISHED:
                if (!authenticated && requireAuthentication) {
                    Message error = Message.ERROR(null, new Exception("autentication required (client " + channel + ")"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                if (!isWorker) {
                    Message error = Message.ERROR(null, new Exception("request type " + message.type + " is only for workers"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                List<Map<String, Object>> tasksData = (List<Map<String, Object>>) message.parameters.get("tasksData");
                LOGGER.log(Level.FINEST, "tasksFinished {0} {1}", new Object[]{tasksData, message.parameters});
                List<TaskFinishedData> finishedTasksInfo = new ArrayList<>(tasksData.size());
                for (Map<String, Object> task : tasksData) {
                    long taskid = (Long) task.get("taskid");
                    String status = (String) task.get("status");
                    String result = (String) task.get("result");
                    int finalStatus = Task.taskExecutorStatusToTaskStatus(status);
                    TaskFinishedData dd = new TaskFinishedData(taskid, result, finalStatus);
                    finishedTasksInfo.add(dd);
                }

                try {
                    broker.tasksFinished(clientId, finishedTasksInfo);
                    _channel.sendReplyMessage(message, Message.ACK(workerProcessId));
                } catch (LogNotAvailableException error) {
                    _channel.sendReplyMessage(message, Message.ERROR(workerProcessId, error));
                    LOGGER.log(Level.SEVERE, "error", error);
                }
                break;
            case Message.TYPE_WORKER_PING:
                if (!authenticated && requireAuthentication) {
                    Message error = Message.ERROR(null, new Exception("autentication required (client " + channel + ")"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                if (!isWorker) {
                    Message error = Message.ERROR(null, new Exception("request type " + message.type + " is only for workers"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                String processId = (String) message.parameters.getOrDefault("processId", "");
                Integer maxThreads = (Integer) message.parameters.getOrDefault("maxThreads", 0);
                int maxThreadPerUserPerTaskTypePercent = (Integer) message.parameters.getOrDefault("maxThreadPerUserPerTaskTypePercent", 0);
                Map<String, Integer> maxThreadsByTaskType = (Map<String, Integer>) message.parameters.getOrDefault("maxThreadsByTaskType", Collections.emptyMap());
                List<Integer> groups = (List<Integer>) message.parameters.getOrDefault("groups", Collections.emptyList());
                Set<Integer> excludedGroups = (Set<Integer>) message.parameters.getOrDefault("excludedGroups", Collections.emptySet());
                Map<String, Integer> resourceLimits = (Map<String, Integer>) message.parameters.getOrDefault("resources", Collections.emptyMap());
                LOGGER.log(Level.SEVERE, "ping connection " + connectionId + ", workerId:" + clientId + ", processId=" + message.parameters.get("processId") + ", location=" + message.parameters.get("location"));
                if (workerProcessId != null && !message.workerProcessId.equals(processId)) {
                    // worker process is not the same as the one we expect, send a "die" message and close the channel
                    Message killWorkerMessage = Message.KILL_WORKER(workerProcessId);
                    channel.sendMessageWithAsyncReply(killWorkerMessage, broker.getConfiguration().getNetworkTimeout(), (Message originalMessage, Message message1, Throwable error) -> {
                        // any way we are closing the channel
                        channel.close();
                    });
                    return;
                }
                this.manager = broker.getWorkers().getWorkerManager(clientId);
                manager.applyConfiguration(maxThreads, maxThreadsByTaskType, groups, excludedGroups, resourceLimits, maxThreadPerUserPerTaskTypePercent);
                break;
            case Message.TYPE_WORKER_SHUTDOWN:
                if (!authenticated && requireAuthentication) {
                    Message error = Message.ERROR(null, new Exception("autentication required (client " + channel + ")"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                if (!isWorker) {
                    Message error = Message.ERROR(null, new Exception("request type " + message.type + " is only for workers"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                LOGGER.log(Level.SEVERE, "worker " + clientId + " at " + location + ", processid " + workerProcessId + " sent shutdown message");
                /// ignore
                break;
            case Message.TYPE_SNAPSHOT_DOWNLOAD_REQUEST:
                if (!authenticated && requireAuthentication) {
                    Message error = Message.ERROR(null, new Exception("autentication required (client " + channel + ")"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                if (!isBroker) {
                    Message error = Message.ERROR(null, new Exception("request type " + message.type + " is only for brokers"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                LOGGER.log(Level.SEVERE, "creating snapshot in reponse to a SNAPSHOT_DOWNLOAD_REQUEST from " + this.channel);
                try {
                    BrokerStatusSnapshot snapshot = broker.getBrokerStatus().createSnapshot();

                    byte[] data;
                    try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                        GZIPOutputStream zout = new GZIPOutputStream(out)) {
                        BrokerStatusSnapshot.serializeSnapshot(snapshot, zout);
                        zout.close();
                        data = out.toByteArray();
                    } catch (IOException err) {
                        throw new LogNotAvailableException(err);
                    }
                    LOGGER.log(Level.SEVERE, "sending snapshot data..." + data.length + " bytes");
                    channel.sendReplyMessage(message, Message.SNAPSHOT_DOWNLOAD_RESPONSE(data));
                } catch (Exception error) {
                    LOGGER.log(Level.SEVERE, "Error", error);
                    channel.sendReplyMessage(message, Message.ERROR(workerProcessId, error));
                }
                break;
            case Message.TYPE_DOWNLOAD_CODEPOOL:
                if (!authenticated && requireAuthentication) {
                    Message error = Message.ERROR(null, new Exception("autentication required (client " + channel + ")"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                if (!isWorker) {
                    Message error = Message.ERROR(null, new Exception("request type " + message.type + " is only for workers"));
                    _channel.sendReplyMessage(message, error);
                    break;
                }
                String codePoolId = (String) message.parameters.get("codePoolId");
                LOGGER.log(Level.SEVERE, "serving codepool " + codePoolId);
                try {
                    CodePool codePool = broker.getBrokerStatus().getCodePool(codePoolId);
                    if (codePool == null) {
                        throw new Exception("codepool " + codePoolId + " does not exist");
                    } else {
                        channel.sendReplyMessage(message, Message.DOWNLOAD_CODEPOOL_RESPONSE(codePool.getCodePoolData()));
                    }
                } catch (Exception error) {
                    LOGGER.log(Level.SEVERE, "Error", error);
                    channel.sendReplyMessage(message, Message.ERROR(workerProcessId, error));
                }
                break;

            default:
                LOGGER.log(Level.SEVERE, "worker " + clientId + " at " + location + ", processid " + workerProcessId + " sent unknown message " + message);
                channel.sendReplyMessage(message, Message.ERROR(workerProcessId, new Exception("invalid message type:" + message.type)));

        }

    }

    @Override
    public void channelClosed() {
        LOGGER.log(Level.SEVERE, "client " + clientId + " connection " + this + " closed");
        channel = null;
        if (clientId != null) {
            WorkerManager workerManager = broker.getWorkers().getWorkerManagerNoCreate(clientId);
            if (workerManager != null) {
                workerManager.deactivateConnection(this);
            }
        }
        broker.getAcceptor().connectionClosed(this);
        if (clientId != null) {
            broker.getWorkers().wakeUp();
        }
    }

    void answerConnectionNotAcceptedAndClose(Message connectionRequestMessage, Throwable ex
    ) {
        if (channel != null) {
            channel.sendReplyMessage(connectionRequestMessage, Message.ERROR(workerProcessId, ex));
        }
        close();
    }

    public void close() {
        if (channel != null) {
            channel.close();
        } else {
            channelClosed();
        }
    }

    void answerConnectionAccepted(Message connectionRequestMessage
    ) {
        channel.sendReplyMessage(connectionRequestMessage, Message.ACK(workerProcessId));
    }

    public void sendTaskAssigned(Task task, SimpleCallback<Void> callback) {
        Map<String, Object> params = new HashMap<>();
        params.put("taskid", task.getTaskId());
        params.put("tasktype", task.getType());
        params.put("parameter", task.getParameter());
        params.put("attempt", task.getAttempts());
        params.put("userid", task.getUserId());
        params.put("resources", task.getResources());
        if (task.getMode() != null) {
            params.put("mode", task.getMode());
        }
        if (task.getCodepool() != null) {
            params.put("codepool", task.getCodepool());
        }
        channel.sendOneWayMessage(Message.TYPE_TASK_ASSIGNED(workerProcessId, params), new SendResultCallback() {

            @Override
            public void messageSent(Message originalMessage, Throwable error) {
                callback.onResult(null, error);
            }
        });
    }

    public void workerDied() {
        LOGGER.log(Level.SEVERE, "worker " + clientId + " connection " + this + ": workerDied");
        if (channel != null) {
            channel.sendOneWayMessage(Message.KILL_WORKER(workerProcessId), new SendResultCallback() {
                @Override
                public void messageSent(Message originalMessage, Throwable error) {
                    // any way we are closing the channel
                    channel.close();
                    broker.getAcceptor().connectionClosed(BrokerSideConnection.this);
                }
            });
        }

    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 43 * hash + (int) (this.connectionId ^ (this.connectionId >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BrokerSideConnection other = (BrokerSideConnection) obj;
        if (this.connectionId != other.connectionId) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "BrokerSideConnection{" + "workerId=" + clientId + ", workerProcessId=" + workerProcessId + ", connectionId=" + connectionId + ", location=" + location + ", channel=" + channel + ", lastReceivedMessageTs=" + lastReceivedMessageTs + '}';
    }

}

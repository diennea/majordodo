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
package majordodo.worker;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import majordodo.codepools.CodePoolClassloadersManager;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import majordodo.network.BrokerLocator;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.ConnectionRequestInfo;
import majordodo.network.Message;
import majordodo.network.ReplyCallback;
import majordodo.network.SendResultCallback;
import majordodo.utils.ErrorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core of the worker inside the JVM
 *
 * @author enrico.olivelli
 */
public class WorkerCore implements ChannelEventListener, ConnectionRequestInfo, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerCore.class);

    private final ExecutorService threadpool;
    private final String processId;
    private final String workerId;
    private final String location;
    private final Map<Long, String> runningTasks = new HashMap<>();
    private final ReentrantReadWriteLock runningTasksLock = new ReentrantReadWriteLock(true);
    private final BrokerLocator brokerLocator;
    private final Thread coreThread;
    private final Path codePoolsDirectory;
    private final CodePoolClassloadersManager classloadersManager;
    private volatile boolean stopped = false;
    private Channel channel;
    private WorkerStatusListener listener;
    private KillWorkerHandler killWorkerHandler = KillWorkerHandler.GRACEFULL_STOP;
    private Callable<Void> externalProcessChecker; // PIDFILECHECKER

    public CodePoolClassloadersManager getClassloadersManager() {
        return classloadersManager;
    }

    public KillWorkerHandler getKillWorkerHandler() {
        return killWorkerHandler;
    }

    public void setKillWorkerHandler(KillWorkerHandler killWorkerHandler) {
        this.killWorkerHandler = killWorkerHandler;
    }

    public Callable<Void> getExternalProcessChecker() {
        return externalProcessChecker;
    }

    public void setExternalProcessChecker(Callable<Void> externalProcessChecker) {
        this.externalProcessChecker = externalProcessChecker;
    }

    public void die() {
        LOGGER.error("Die!");
        if (coreThread != null) {
            coreThread.interrupt();
        }
        this.stop();
    }

    public byte[] downloadCodePool(String codePoolId) throws Exception {
        Channel _channel = channel;
        if (_channel == null) {
            throw new Exception("not connected");
        }
        Message reply = _channel.sendMessageWithReply(Message.DOWNLOAD_CODEPOOL(workerId, codePoolId), 10000);
        if (reply.type == Message.TYPE_DOWNLOAD_CODEPOOL_RESPONSE) {
            return (byte[]) reply.parameters.get("data");
        } else {
            throw new Exception("error from broker while downloading codepool " + codePoolId + " data:" + reply);
        }
    }

    private static final class NotImplementedTaskExecutorFactory implements TaskExecutorFactory {

        @Override
        public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
            return new TaskExecutor();
        }

    }

    ;
    private TaskExecutorFactory executorFactory;

    public Map<Long, String> getRunningTasks() {
        runningTasksLock.readLock().lock();
        try {
            // clone only the structure, not the task status
            return new HashMap<>(runningTasks);
        } finally {
            runningTasksLock.readLock().unlock();
        }
    }

    @Override
    public Set<Long> getRunningTaskIds() {
        Set<Long> res = new HashSet<>();
        runningTasksLock.readLock().lock();
        try {
            res.addAll(runningTasks.keySet());
        } finally {
            runningTasksLock.readLock().unlock();
        }
        for (FinishedTaskNotification f : this.pendingFinishedTaskNotifications) {
            res.add(f.taskId);
        }
        return res;
    }

    public List<FinishedTaskNotification> getPendingFinishedTaskNotifications() {
        return new ArrayList<>(pendingFinishedTaskNotifications);
    }

    public TaskExecutorFactory getExecutorFactory() {
        return executorFactory;
    }

    public void setExecutorFactory(TaskExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
    }

    private void ping() {
        long now = System.currentTimeMillis();
        long delta = now - lastPingSent;
        if (delta < config.getMaxKeepAliveTime()) {
            return;
        }
        lastPingSent = now;
        Channel _channel = channel;
        if (_channel != null) {
            _channel.sendOneWayMessage(Message.WORKER_PING(
                            processId, config.getGroups(), config.getExcludedGroups(), config.getMaxThreadsByTaskType(), config.getMaxThreads(), config.getResourcesLimits(),
                            config.getMaxThreadPerUserPerTaskTypePercent()),
                    (Message originalMessage, Throwable error) -> {
                        if (error != null) {
                            if (!stopped) {
                                LOGGER.error("ping error ", error);
                                disconnect();
                            }
                        }
                    });
        } else {
            LOGGER.debug("ping not connected");
        }
    }

    private WorkerCoreConfiguration config;

    public WorkerCore(
            WorkerCoreConfiguration config,
            String processId,
            BrokerLocator brokerLocator,
            WorkerStatusListener listener) {
        this.config = config;

        if (listener == null) {
            listener = new WorkerStatusListener() {
            };
        }
        this.listener = listener;
        this.threadpool = Executors.newCachedThreadPool(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "dodo-worker-thread-" + workerId);
            }
        });
        this.processId = processId;
        this.workerId = config.getWorkerId();
        this.location = config.getLocation();
        this.brokerLocator = brokerLocator;
        this.coreThread = new Thread(new ConnectionManager(), "dodo-worker-connection-manager-" + workerId);
        if (config.isEnableCodePools()) {
            if (config.getCodePoolsDirectory() == null || config.getCodePoolsDirectory().isEmpty()) {
                codePoolsDirectory = Paths.get("codepools").toAbsolutePath();
            } else {
                codePoolsDirectory = Paths.get(config.getCodePoolsDirectory()).toAbsolutePath();
            }
            LOGGER.info("CodePools Working directory {}", codePoolsDirectory);
            try {
                this.classloadersManager = new CodePoolClassloadersManager(codePoolsDirectory, this);
            } catch (IOException err) {
                throw new RuntimeException(err);
            }
        } else {
            this.classloadersManager = null;
            this.codePoolsDirectory = null;
        }
    }

    public void start() {
        if (executorFactory == null) {
            if (codePoolsDirectory == null) {
                executorFactory = new TaskModeAwareExecutorFactory(
                        new NotImplementedTaskExecutorFactory()
                );
            } else {
                executorFactory = new CodePoolAwareExecutorFactory(
                        new TaskModeAwareExecutorFactory(
                                new NotImplementedTaskExecutorFactory()
                        ), classloadersManager);
            }
        }
        this.coreThread.start();
        JVMWorkersRegistry.registerWorker(workerId, this);
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.debug("received {}", message);
        if (message.type == Message.TYPE_KILL_WORKER) {
            killWorkerHandler.killWorker(this);
            return;
        }
        if (message.type == Message.TYPE_TASK_ASSIGNED) {
            startTask(message);
        }
    }

    @Override
    public void channelClosed() {
        LOGGER.debug("channel closed");
        disconnect();
        brokerLocator.brokerDisconnected();
    }

    BlockingQueue<FinishedTaskNotification> pendingFinishedTaskNotifications = new LinkedBlockingQueue<>();
    private long lastFinishedTaskNotificationSent;
    private long lastPingSent;

    ExecutorRunnable.TaskExecutionCallback executionCallback = new ExecutorRunnable.TaskExecutionCallback() {
        @Override
        public void taskStatusChanged(long taskId, Map<String, Object> parameters, String finalStatus, String results, Throwable error) {
            LOGGER.debug("taskStatusChanged {} {} {} {} {}", taskId, parameters, finalStatus, results, error);
            switch (finalStatus) {
                case TaskExecutorStatus.ERROR:
                case TaskExecutorStatus.FINISHED:
                    runningTasksLock.writeLock().lock();
                    try {
                        runningTasks.remove(taskId);
                    } finally {
                        runningTasksLock.writeLock().unlock();
                    }
                    pendingFinishedTaskNotifications.add(new FinishedTaskNotification(taskId, finalStatus, results, error));
                    break;
                case TaskExecutorStatus.RUNNING:
                    break;
            }
        }

    };

    private void notifyTasksFinished(List<FinishedTaskNotification> notifications) {
        listener.beforeNotifyTasksFinished(notifications, this);
        LOGGER.debug("notifyTasksFinished {}", notifications);
        Channel _channel = channel;
        if (_channel != null) {
            List<Map<String, Object>> tasksData = new ArrayList<>();
            notifications.stream().forEach((not) -> {
                Map<String, Object> params = new HashMap<>();
                params.put("taskid", not.taskId);
                params.put("status", not.finalStatus);
                params.put("result", not.results);
                if (not.error != null) {
                    params.put("error", ErrorUtils.stacktrace(not.error));
                    LOGGER.error("notifyTaskFinished " + params, not.error);
                }
                tasksData.add(params);
            });
            Message msg = Message.TASK_FINISHED(processId, tasksData);
            _channel.sendMessageWithAsyncReply(msg, config.getNetworkTimeout(), new ReplyCallback() {

                @Override
                public void replyReceived(Message originalMessage, Message msg, Throwable error) {
                    if (error != null) {
                        LOGGER.error("re-enqueing notification of task finish, due to broker comunication failure", error);
                        pendingFinishedTaskNotifications.addAll(notifications);
                    } else if (msg.type != Message.TYPE_ACK) {
                        LOGGER.error("re-enqueing notification of task finish, due to broker error anwser {}", msg);
                        pendingFinishedTaskNotifications.addAll(notifications);
                    }
                }
            });
        } else {
            LOGGER.info("re-enqueing notification of task finish, due to broker connection failure");
            pendingFinishedTaskNotifications.addAll(notifications);
        }
    }

    private void startTask(Message message) {
        Long taskid = (Long) message.parameters.get("taskid");
        String tasktype = (String) message.parameters.get("tasktype");
        runningTasksLock.writeLock().lock();
        try {
            runningTasks.put(taskid, tasktype);
        } finally {
            runningTasksLock.writeLock().unlock();
        }
        ExecutorRunnable runnable = new ExecutorRunnable(this, taskid, message.parameters, executionCallback);
        threadpool.submit(runnable);
    }

    public void stop() {
        stopped = true;
        try {
            coreThread.join();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        if (classloadersManager != null) {
            classloadersManager.close();
        }
        JVMWorkersRegistry.unregisterWorker(workerId);
    }

    @Override
    public void close() {
        stop();
    }

    TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
        return executorFactory.createTaskExecutor(taskType, parameters);
    }

    private class ConnectionManager implements Runnable {

        @Override
        public void run() {
            while (!stopped) {
                try {
                    try {
                        if (channel == null) {
                            connect();
                        }
                    } catch (InterruptedException exit) {
                        LOGGER.error("exit loop " + exit);
                        break;
                    } catch (BrokerNotAvailableException | BrokerRejectedConnectionException retry) {
                        LOGGER.error("no broker available:" + retry);
                    }

                    if (channel == null) {
                        try {
                            LOGGER.debug("not connected, waiting 5000 ms");
                            Thread.sleep(5000);
                        } catch (InterruptedException exit) {
                            LOGGER.error("exit loop " + exit);
                            break;
                        }
                        continue;
                    }

                    try {
                        sendPendingNotifications(false);
                    } catch (InterruptedException exit) {
                        LOGGER.error("exit loop " + exit);
                        break;
                    }

                    ping();

                    if (externalProcessChecker != null) {
                        try {
                            externalProcessChecker.call();
                        } catch (Exception err) {
                            err.printStackTrace();
                            killWorkerHandler.killWorker(WorkerCore.this);
                        }
                    }
                } catch (Throwable error) {
                    LOGGER.error("error on main WorkerCore loop:" + error, error);
                }
            }
            LOGGER.debug("shutting down {}", processId);

            Channel _channel = channel;
            if (_channel != null) {
                try {
                    sendPendingNotifications(true);
                } catch (InterruptedException ignore) {
                }
                _channel.sendOneWayMessage(Message.WORKER_SHUTDOWN(processId), new SendResultCallback() {

                    @Override
                    public void messageSent(Message originalMessage, Throwable error) {
                        // ignore
                    }
                });
                disconnect();
            }

        }

        private void sendPendingNotifications(boolean force) throws InterruptedException {
            long now = System.currentTimeMillis();
            long delta = now - lastFinishedTaskNotificationSent;
            int count = pendingFinishedTaskNotifications.size();
            if (!force && (count < config.getMaxPendingFinishedTaskNotifications() && delta < config.getMaxWaitPendingFinishedTaskNotifications())) {
                LOGGER.debug("sendPendingNotifications count {} elapsed {}", count, delta + " ms");
                Thread.sleep(100);
                return;
            }
            lastFinishedTaskNotificationSent = now;
            int max = 1000;
            List<FinishedTaskNotification> batch = new ArrayList<>();
            FinishedTaskNotification notification = pendingFinishedTaskNotifications.poll();
            while (notification != null) {
                batch.add(notification);
                if (max-- <= 0) {
                    break;
                }
                notification = pendingFinishedTaskNotifications.poll();
            }
            if (batch.isEmpty()) {
                return;
            }
            long _start = System.currentTimeMillis();
            notifyTasksFinished(batch);
            long _stop = System.currentTimeMillis();
            LOGGER.debug("pending notifications sent {} remaining {}, {} ms", batch.size(), pendingFinishedTaskNotifications.size(), _stop - _start);
        }
    }

    private void connect() throws InterruptedException, BrokerNotAvailableException, BrokerRejectedConnectionException {
        if (channel != null) {
            try {
                channel.close();
            } finally {
                channel = null;
            }
        }
        LOGGER.info("connecting, location={} processId={} workerid={}", this.location, this.processId, this.workerId);
        disconnect();
        try {
            channel = brokerLocator.connect(this, this);
            LOGGER.debug("connected, channel:{}", channel);
            listener.connectionEvent(WorkerStatusListener.EVENT_CONNECTED, this);
        } catch (BrokerRejectedConnectionException | BrokerNotAvailableException error) {
            listener.connectionEvent(WorkerStatusListener.EVENT_CONNECTION_ERROR, this);
            throw error;
        }
    }

    public void disconnect() {
        try {
            Channel c = channel;
            if (c != null) {
                channel = null;
                c.close();
                listener.connectionEvent(WorkerStatusListener.EVENT_DISCONNECTED, this);
            }
        } finally {
            channel = null;
        }

    }

    @Override
    public String getProcessId() {
        return processId;
    }

    @Override
    public String getWorkerId() {
        return workerId;
    }

    @Override
    public String getLocation() {
        return location;
    }

    @Override
    public String getClientType() {
        return CLIENT_TYPE_WORKER;
    }

    @Override
    public String getSharedSecret() {
        return config.getSharedSecret();
    }

    @Override
    public int getMaxThreads() {
        return config.getMaxThreads();
    }

    @Override
    public int getMaxThreadPerUserPerTaskTypePercent() {
        return config.getMaxThreadPerUserPerTaskTypePercent();
    }

    @Override
    public Map<String, Integer> getMaxThreadsByTaskType() {
        return config.getMaxThreadsByTaskType();
    }

    @Override
    public List<Integer> getGroups() {
        return config.getGroups();
    }

    @Override
    public Set<Integer> getExcludedGroups() {
        return config.getExcludedGroups();
    }

    @Override
    public Map<String, Integer> getResourceLimits() {
        return config.getResourcesLimits();
    }

    public WorkerStatusView createWorkerStatusView() {
        WorkerStatusView res = new WorkerStatusView();
        if (stopped) {
            res.setStatus("STOPPED");
        } else if (channel != null) {
            res.setStatus("CONNECTED");
            res.setConnectionInfo(channel + "");
        } else {
            res.setStatus("DISCONNECTED");
        }
        runningTasksLock.readLock().lock();
        try {
            res.setRunningTasks(this.runningTasks.size());
        } finally {
            runningTasksLock.readLock().unlock();
        }
        res.setFinishedTasksPendingNotification(this.pendingFinishedTaskNotifications.size());
        return res;
    }

}

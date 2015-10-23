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

import java.util.ArrayList;
import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerLocator;
import majordodo.network.ConnectionRequestInfo;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.Message;
import majordodo.network.SendResultCallback;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import majordodo.network.ReplyCallback;
import majordodo.utils.ErrorUtils;

/**
 * Core of the worker inside the JVM
 *
 * @author enrico.olivelli
 */
public class WorkerCore implements ChannelEventListener, ConnectionRequestInfo, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(WorkerCore.class.getName());

    private final ExecutorService threadpool;
    private final String processId;
    private final String workerId;
    private final String location;
    private final Map<Long, String> runningTasks = new ConcurrentHashMap<>();
    private final BrokerLocator brokerLocator;
    private final Thread coreThread;
    private volatile boolean stopped = false;
    private Channel channel;
    private WorkerStatusListener listener;
    private KillWorkerHandler killWorkerHandler = KillWorkerHandler.GRACEFULL_STOP;
    private Callable<Void> externalProcessChecker; // PIDFILECHECKER

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
        LOGGER.log(Level.SEVERE, "Die!");
        if (coreThread != null) {
            coreThread.interrupt();
        }
        this.stop();
    }

    private static final class NotImplementedTaskExecutorFactory implements TaskExecutorFactory {

        @Override
        public TaskExecutor createTaskExecutor(String taskType, Map<String, Object> parameters) {
            return new TaskExecutor();
        }

    };
    private TaskExecutorFactory executorFactory;

    public Map<Long, String> getRunningTasks() {
        return runningTasks;
    }

    @Override
    public Set<Long> getRunningTaskIds() {
        Set<Long> res = new HashSet<>();
        res.addAll(runningTasks.keySet());
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

    private volatile boolean requestNewTasksPending = false;

    private void requestNewTasks() {
        if (requestNewTasksPending) {
            return;
        }
        Channel _channel = channel;
        if (_channel != null) {
            Map<String, Integer> availableSpace = new HashMap<>(config.getMaxThreadsByTaskType());
            runningTasks.values().forEach(tasktype -> {
                Integer count = availableSpace.get(tasktype);
                if (count != null && count > 1) {
                    availableSpace.put(tasktype, count - 1);
                } else {
                    availableSpace.remove(tasktype);
                }
            });

            int maxnewthreads = config.getMaxThreads() - runningTasks.size();
            long _start = System.currentTimeMillis();
            LOGGER.log(Level.FINER, "requestNewTasks maxnewthreads:" + maxnewthreads + ", availableSpace:" + availableSpace + " groups:" + config.getGroups() + " excludedGroups" + config.getExcludedGroups());
            if (availableSpace.isEmpty() || maxnewthreads <= 0) {
                return;
            }
            requestNewTasksPending = true;
            _channel.sendMessageWithAsyncReply(Message.WORKER_TASKS_REQUEST(processId, config.getGroups(), config.getExcludedGroups(), availableSpace, maxnewthreads),
                    (Message originalMessage, Message message, Throwable error) -> {
                        requestNewTasksPending = false;
                        if (error != null) {
                            if (!stopped) {
                                LOGGER.log(Level.SEVERE, "requestNewTasks error ", error);
                                disconnect();
                            }
                        } else {
                            Integer count = (Integer) message.parameters.get("countAssigned");
                            LOGGER.log(Level.FINE, "requestNewTasks finished {0} ms got {1} tasks", new Object[]{System.currentTimeMillis() - _start, count});
                        }
                    });
        } else {
            LOGGER.log(Level.FINER, "requestNewTasks not connected");
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
    }

    public void start() {
        if (executorFactory == null) {
            executorFactory = new NotImplementedTaskExecutorFactory();
        }
        this.coreThread.start();
        JVMWorkersRegistry.registerWorker(workerId, this);
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.log(Level.FINEST, "received {0}", new Object[]{message});
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
        LOGGER.log(Level.SEVERE, "channel closed");
        disconnect();
        brokerLocator.brokerDisconnected();
    }

    BlockingQueue<FinishedTaskNotification> pendingFinishedTaskNotifications = new LinkedBlockingQueue<>();
    private long lastFinishedTaskNotificationSent;

    ExecutorRunnable.TaskExecutionCallback executionCallback = new ExecutorRunnable.TaskExecutionCallback() {
        @Override
        public void taskStatusChanged(long taskId, Map<String, Object> parameters, String finalStatus, String results, Throwable error) {
            LOGGER.log(Level.FINEST, "taskStatusChanged {0} {1} {2} {3} {4}", new Object[]{taskId, parameters, finalStatus, results, error});
            switch (finalStatus) {
                case TaskExecutorStatus.ERROR:
                case TaskExecutorStatus.FINISHED:
                    runningTasks.remove(taskId);
                    pendingFinishedTaskNotifications.add(new FinishedTaskNotification(taskId, finalStatus, results, error));
                    break;
                case TaskExecutorStatus.RUNNING:
                    break;
            }
        }

    };

    private void notifyTasksFinished(List<FinishedTaskNotification> notifications) {
        LOGGER.log(Level.FINEST, "notifyTasksFinished {0}", notifications);
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
                    LOGGER.log(Level.SEVERE, "notifyTaskFinished " + params, not.error);
                }
                tasksData.add(params);
            });
            Message msg = Message.TASK_FINISHED(processId, tasksData);
            _channel.sendOneWayMessage(msg, new SendResultCallback() {
                @Override
                public void messageSent(Message originalMessage, Throwable error) {
                    if (error != null) {
                        LOGGER.log(Level.SEVERE, "re-enqueing notification of task finish, due to broker comunication failure", error);
                        pendingFinishedTaskNotifications.addAll(notifications);
                    }
                }
            });
        } else {
            LOGGER.log(Level.SEVERE, "re-enqueing notification of task finish, due to broker connection failure");
            pendingFinishedTaskNotifications.addAll(notifications);
        }
    }

    private void startTask(Message message) {
        Long taskid = (Long) message.parameters.get("taskid");
        String tasktype = (String) message.parameters.get("tasktype");
        runningTasks.put(taskid, tasktype);
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
                    if (channel == null) {
                        connect();
                    }

                } catch (InterruptedException exit) {
                    LOGGER.log(Level.SEVERE, "exit loop " + exit);
                    break;
                } catch (BrokerNotAvailableException | BrokerRejectedConnectionException retry) {
                    LOGGER.log(Level.SEVERE, "no broker available:" + retry);
                }

                if (channel == null) {
                    try {
                        LOGGER.log(Level.FINEST, "not connected, waiting 5000 ms");
                        Thread.sleep(5000);
                    } catch (InterruptedException exit) {
                        LOGGER.log(Level.SEVERE, "exit loop " + exit);
                        break;
                    }
                    continue;
                }

                try {
                    sendPendingNotifications(false);
                } catch (InterruptedException exit) {
                    LOGGER.log(Level.SEVERE, "exit loop " + exit);
                    break;
                }

                requestNewTasks();
                if (externalProcessChecker != null) {
                    try {
                        externalProcessChecker.call();
                    } catch (Exception err) {
                        err.printStackTrace();
                        killWorkerHandler.killWorker(WorkerCore.this);
                    }
                }
            }
            LOGGER.log(Level.SEVERE, "shutting down " + processId);

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
            LOGGER.log(Level.FINE, "pending notifications sent {0} remaining {1}, {2} ms", new Object[]{batch.size(), pendingFinishedTaskNotifications.size(), _stop - _start});
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
        LOGGER.log(Level.SEVERE, "connecting, location=" + this.location + " processId=" + this.processId + " workerid=" + this.workerId);
        disconnect();
        channel = brokerLocator.connect(this, this);
        LOGGER.log(Level.SEVERE, "connected, channel:" + channel);
        listener.connectionEvent("connected", this);
    }

    public void disconnect() {
        requestNewTasksPending = false;
        try {
            Channel c = channel;
            if (c != null) {
                channel = null;
                c.close();
                listener.connectionEvent("disconnected", this);
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
    public String getSharedSecret() {
        return config.getSharedSecret();
    }

    public WorkerStatusView createWorkerStatusView() {
        WorkerStatusView res = new WorkerStatusView();
        if (stopped) {
            res.setStatus("STOPPED");
        } else if (channel != null) {

            if (requestNewTasksPending) {
                res.setStatus("REQUESTNEWTASKS");
            } else {
                res.setStatus("CONNECTED");
            }
            res.setConnectionInfo(channel + "");
        } else {
            res.setStatus("DISCONNECTED");
        }
        res.setRunningTasks(this.runningTasks.size());
        res.setFinishedTasksPendingNotification(this.pendingFinishedTaskNotifications.size());
        return res;
    }

}

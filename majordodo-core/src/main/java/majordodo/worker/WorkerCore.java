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

import majordodo.network.BrokerRejectedConnectionException;
import majordodo.network.BrokerNotAvailableException;
import majordodo.network.BrokerLocator;
import majordodo.network.ConnectionRequestInfo;
import majordodo.executors.TaskExecutor;
import majordodo.executors.TaskExecutorFactory;
import majordodo.executors.TaskExecutorStatus;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.Message;
import majordodo.network.SendResultCallback;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        return runningTasks.keySet();
    }

    public TaskExecutorFactory getExecutorFactory() {
        return executorFactory;
    }

    public void setExecutorFactory(TaskExecutorFactory executorFactory) {
        this.executorFactory = executorFactory;
    }

    private void requestNewTasks() {
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

            if (availableSpace.isEmpty()) {
                return;
            }
            int maxnewthreads = config.getMaxThreads() - runningTasks.size();
            if (maxnewthreads <= 0) {
                return;
            }
            try {
                LOGGER.log(Level.FINER, "requestNewTasks maxnewthreads:" + maxnewthreads + ", availableSpace:" + availableSpace + " groups:" + config.getGroups());
                _channel.sendMessageWithReply(
                        Message.WORKER_TASKS_REQUEST(processId, config.getGroups(), availableSpace, maxnewthreads),
                        config.getTasksRequestTimeout()
                );
                LOGGER.log(Level.FINER, "requestNewTasks finished");
            } catch (InterruptedException | TimeoutException err) {
                if (!stopped) {
                    LOGGER.log(Level.SEVERE, "requestNewTasks error ", err);
                    err.printStackTrace();
                }
                return;
            }
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
        this.location = config.getWorkerId();
        this.brokerLocator = brokerLocator;
        this.coreThread = new Thread(new ConnectionManager(), "dodo-worker-connection-manager-" + workerId);
    }

    public void start() {
        if (executorFactory == null) {
            executorFactory = new NotImplementedTaskExecutorFactory();
        }
        this.coreThread.start();
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.log(Level.SEVERE, "[BROKER->WORKER] received " + message);
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
        LOGGER.log(Level.SEVERE, "[BROKER->WORKER] channel closed");
        disconnect();
    }

    BlockingQueue<FinishedTaskNotification> pendingFinishedTaskNotifications = new LinkedBlockingQueue<>();

    ExecutorRunnable.TaskExecutionCallback executionCallback = new ExecutorRunnable.TaskExecutionCallback() {
        @Override
        public void taskStatusChanged(long taskId, Map<String, Object> parameters, String finalStatus, String results, Throwable error) {
            switch (finalStatus) {
                case TaskExecutorStatus.ERROR:
                case TaskExecutorStatus.FINISHED:
                    runningTasks.remove(taskId);
                    notifyTaskFinished(taskId, finalStatus, results, error);
                    break;
                case TaskExecutorStatus.RUNNING:
                    break;
            }
        }

    };

    private void notifyTaskFinished(long taskId, String finalStatus, String results, Throwable error) {
        if (error != null) {
            error.printStackTrace();
        }
        LOGGER.log(Level.SEVERE, "notifyTaskFinished " + taskId + " " + finalStatus + " " + results + " " + error);
        Channel _channel = channel;
        if (_channel != null) {
            _channel.sendOneWayMessage(Message.TASK_FINISHED(processId, taskId, finalStatus, results, error), new SendResultCallback() {

                @Override
                public void messageSent(Message originalMessage, Throwable sendingerror) {
                    if (sendingerror != null) {
                        LOGGER.log(Level.SEVERE, "enqueing notification of task finish, due to broker connection failure");
                        pendingFinishedTaskNotifications.add(new FinishedTaskNotification(taskId, finalStatus, results, error));
                    }
                }
            });
        } else {
            LOGGER.log(Level.SEVERE, "enqueing notification of task finish, due to broker connection failure");
            pendingFinishedTaskNotifications.add(new FinishedTaskNotification(taskId, finalStatus, results, error));
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

                } catch (InterruptedException | BrokerRejectedConnectionException exit) {
                    LOGGER.log(Level.SEVERE, "[WORKER] exit loop " + exit);
                    break;
                } catch (BrokerNotAvailableException retry) {
                    LOGGER.log(Level.SEVERE, "[WORKER] no broker available:" + retry);
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException exit) {
                    LOGGER.log(Level.SEVERE, "[WORKER] exit loop " + exit);
                    break;
                }

                FinishedTaskNotification notification = pendingFinishedTaskNotifications.poll();
                if (notification != null) {
                    notifyTaskFinished(notification.taskId, notification.finalStatus, notification.results, notification.error);
                }

                requestNewTasks();
            }

            Channel _channel = channel;
            if (_channel != null) {
                _channel.sendOneWayMessage(Message.WORKER_SHUTDOWN(processId), new SendResultCallback() {

                    @Override
                    public void messageSent(Message originalMessage, Throwable error) {
                        // ignore
                    }
                });
                disconnect();
            }

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
        LOGGER.log(Level.SEVERE, "[WORKER] connecting");
        disconnect();
        channel = brokerLocator.connect(this, this);
        LOGGER.log(Level.SEVERE, "[WORKER] connected, channel:" + channel);
        listener.connectionEvent("connected", this);
    }

    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
                listener.connectionEvent("disconnected", this);
            } finally {
                channel = null;
            }
        }

    }

    public String getProcessId() {
        return processId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public String getLocation() {
        return location;
    }

}

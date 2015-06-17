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

import dodo.network.BrokerRejectedConnectionException;
import dodo.network.BrokerNotAvailableException;
import dodo.network.BrokerLocator;
import dodo.network.ConnectionRequestInfo;
import dodo.executors.TaskExecutor;
import dodo.executors.TaskExecutorFactory;
import dodo.executors.TaskExecutorStatus;
import dodo.network.Channel;
import dodo.network.ChannelEventListener;
import dodo.network.Message;
import dodo.network.SendResultCallback;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Core of the worker inside the JVM
 *
 * @author enrico.olivelli
 */
public class WorkerCore implements ChannelEventListener, ConnectionRequestInfo, AutoCloseable {

    private final ExecutorService threadpool;
    private final String processId;
    private final String workerId;
    private final String location;
    private final Map<Integer, Integer> maximumThreadPerTaskType;
    private final Map<Long, Integer> runningTasks = new ConcurrentHashMap<>();
    private final BrokerLocator brokerLocator;
    private final Thread coreThread;
    private volatile boolean stopped = false;
    private final int maxThreads;
    private final List<Integer> groups;
    private Channel channel;
    private WorkerStatusListener listener;
    private KillWorkerHandler killWorkerHandler = KillWorkerHandler.GRACEFULL_STOP;

    private static final class NotImplementedTaskExecutorFactory implements TaskExecutorFactory {

        @Override
        public TaskExecutor createTaskExecutor(int taskType, Map<String, Object> parameters) {
            return new TaskExecutor();
        }

    };
    private TaskExecutorFactory executorFactory = new NotImplementedTaskExecutorFactory();

    public Map<Long, Integer> getRunningTasks() {
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
            Map<Integer, Integer> availableSpace = new HashMap<>(maximumThreadPerTaskType);
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
            int maxnewthreads = maxThreads - runningTasks.size();
            _channel.sendOneWayMessage(Message.WORKER_TASKS_REQUEST(processId, groups, availableSpace, maxnewthreads), new SendResultCallback() {

                @Override
                public void messageSent(Message originalMessage, Throwable error) {
                    if (error != null) {
                        error.printStackTrace();
                    }
                }
            });
        }
    }

    public WorkerCore(int maxThreads, String processId, String workerId, String location, Map<Integer, Integer> maximumThreadPerTag, BrokerLocator brokerLocator, WorkerStatusListener listener, List<Integer> groups) {
        this.maxThreads = maxThreads;
        if (listener == null) {
            listener = new WorkerStatusListener() {
            };
        }
        this.groups = groups;
        this.listener = listener;
        this.threadpool = Executors.newFixedThreadPool(maxThreads, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "dodo-worker-thread-" + workerId);
            }
        });
        this.processId = processId;
        this.workerId = workerId;
        this.location = location;
        this.maximumThreadPerTaskType = maximumThreadPerTag;
        this.brokerLocator = brokerLocator;
        this.coreThread = new Thread(new ConnectionManager(), "dodo-worker-connection-manager-" + workerId);
    }

    public void start() {
        this.coreThread.start();
    }

    @Override
    public void messageReceived(Message message) {
        System.out.println("[BROKER->WORKER] received " + message);
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
        System.out.println("[BROKER->WORKER] channel closed");
        disconnect();
    }

    private void startTask(Message message) {
        Long taskid = (Long) message.parameters.get("taskid");
        int tasktype = (Integer) message.parameters.get("tasktype");
        runningTasks.put(taskid, tasktype);
        ExecutorRunnable runnable = new ExecutorRunnable(this, taskid, message.parameters, new ExecutorRunnable.TaskExecutionCallback() {
            @Override
            public void taskStatusChanged(long taskId, Map<String, Object> parameters, String finalStatus, String results, Throwable error) {
                switch (finalStatus) {
                    case TaskExecutorStatus.ERROR:
                        runningTasks.remove(taskId);
                        channel.sendOneWayMessage(Message.TASK_FINISHED(processId, taskId, finalStatus, results, error), new SendResultCallback() {

                            @Override
                            public void messageSent(Message originalMessage, Throwable error) {
                                if (error != null) {
                                    error.printStackTrace();
                                }
                            }
                        });
                        break;
                    case TaskExecutorStatus.RUNNING:
                        break;
                    case TaskExecutorStatus.NEEDS_RECOVERY:
                        throw new RuntimeException("not implemented");
                    case TaskExecutorStatus.FINISHED:
                        runningTasks.remove(taskId);
                        channel.sendOneWayMessage(Message.TASK_FINISHED(processId, taskId, finalStatus, results, null), new SendResultCallback() {

                            @Override
                            public void messageSent(Message originalMessage, Throwable error
                            ) {
                                if (error != null) {
                                    error.printStackTrace();
                                }
                            }
                        });
                        break;
                }
            }
        });
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

    TaskExecutor createTaskExecutor(int taskType, Map<String, Object> parameters) {
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
                    System.out.println("[WORKER] exit loop " + exit);
                    break;
                } catch (BrokerNotAvailableException retry) {
                    System.out.println("[WORKER] no broker available:" + retry);
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException exit) {
                    System.out.println("[WORKER] exit loop " + exit);
                    break;
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
        System.out.println("[WORKER] connecting");
        disconnect();
        channel = brokerLocator.connect(this, this);
        System.out.println("[WORKER] connected, channel:" + channel);
        listener.connectionEvent("connected", this);
    }

    private void disconnect() {
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

    public Map<Integer, Integer> getMaximumThreadPerTaskType() {
        return maximumThreadPerTaskType;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

}

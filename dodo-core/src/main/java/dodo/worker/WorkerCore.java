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

import dodo.network.Channel;
import dodo.network.InboundMessagesReceiver;
import dodo.network.Message;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Core of the worker inside the JVM
 *
 * @author enrico.olivelli
 */
public class WorkerCore implements InboundMessagesReceiver {

    private final ExecutorService threadpool;
    private final String processId;
    private final String workerId;
    private final String location;
    private final Map<String, Integer> maximumThreadPerTag;
    private final BrokerLocator brokerLocator;
    private final Thread coreThread;
    private volatile boolean stopped = false;
    private final int maxThreads;
    private Channel channel;
    private WorkerStatusListener listener;

    public WorkerCore(int maxThreads, String processId, String workerId, String location, Map<String, Integer> maximumThreadPerTag, BrokerLocator brokerLocator, WorkerStatusListener listener) {
        this.maxThreads = maxThreads;
        if (listener == null) {
            listener = new WorkerStatusListener() {
            };
        }
        this.listener = listener;
        this.threadpool = Executors.newFixedThreadPool(maxThreads, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "dodo-worker-thread");
            }
        });
        this.processId = processId;
        this.workerId = workerId;
        this.location = location;
        this.maximumThreadPerTag = maximumThreadPerTag;
        this.brokerLocator = brokerLocator;
        this.coreThread = new Thread(new ConnectionManager(), "dodo-worker-connection-manager");
    }

    public void start() {
        this.coreThread.start();
    }

    @Override
    public void messageReceived(Message message) {
        System.out.println("[BROKER->WORKER] received " + message);
    }

    public void stop() {
        stopped = true;
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
                    Thread.sleep(1000);
                } catch (InterruptedException exit) {
                    System.out.println("[WORKER] exit loop " + exit);
                    break;
                }
            }

            Channel _channel = channel;
            if (_channel != null) {
                _channel.sendOneWayMessage(Message.WORKER_SHUTDOWN(processId));
                disconnect();
            }

        }
    }

    private void connect() throws InterruptedException, BrokerNotAvailableException, BrokerRejectedConnectionException {
        System.out.println("[WORKER] connecting");
        disconnect();
        channel = brokerLocator.connect(this);
        System.out.println("[WORKER] connected, channel:" + channel);
        listener.connectionEvent("connected", this);
    }

    private void disconnect() {
        if (channel != null) {
            channel.close();
            listener.connectionEvent("disconnected", this);
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

    public Map<String, Integer> getMaximumThreadPerTag() {
        return maximumThreadPerTag;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

}

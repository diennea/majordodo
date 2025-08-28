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

import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import majordodo.clientfacade.BrokerStatusView;
import majordodo.clientfacade.DelayedTasksQueueView;
import majordodo.clientfacade.HeapStatusView;
import majordodo.clientfacade.HeapStatusView.TaskStatus;
import majordodo.clientfacade.SlotsStatusView;
import majordodo.clientfacade.TransactionsStatusView;
import majordodo.clientfacade.TransactionStatus;

/**
 * Periodically log the status of the broker on the logs
 *
 * @author enrico.olivelli
 */
public class BrokerStatusMonitor {

    private final BrokerConfiguration configuration;
    private final ScheduledExecutorService timer;
    private final Broker broker;

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerStatusMonitor.class);

    public BrokerStatusMonitor(BrokerConfiguration configuration, Broker broker) {
        this.configuration = configuration;
        this.broker = broker;
        if (this.configuration.getBrokerStatusMonitorPeriod() > 0) {
            this.timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "dodo-broker-monitor-thread");
                    t.setDaemon(true);
                    return t;
                }
            });
        } else {
            this.timer = null;
        }
    }

    private class Monitor implements Runnable {

        @Override
        public void run() {
            HeapStatusView heap = broker.getHeapStatusView();
            DelayedTasksQueueView delayedQueue = broker.getDelayedTasksQueueView();
            TransactionsStatusView transactions = broker.getTransactionsStatusView();
            SlotsStatusView slots = broker.getSlotsStatusView();
            BrokerStatusView brokerStatusView = broker.createBrokerStatusView();

            TransactionStatus oldestTransaction = transactions
                    .getTransactions()
                    .stream()
                    .sorted(Comparator.comparing(TransactionStatus::getCreationTimestamp))
                    .findFirst()
                    .orElse(null);

            int countHeap = heap.getTasks().size();
            TaskStatus first = null;
            TaskStatus last = null;
            if (countHeap > 0) {
                first = heap.getTasks().get(0);
                last = heap.getTasks().get(countHeap - 1);
            }
            
            long delaysSum = 0;
            for (DelayedTasksQueueView.TaskStatus task: delayedQueue.getTasks()) {
                delaysSum += task.getDelay();
            }
            double averageDelayInSeconds = 0;
            if (delayedQueue.getTasks().size() > 0){
                averageDelayInSeconds = (double) delaysSum / delayedQueue.getTasks().size() / 1000;
            }
            
            LOGGER.info("Broker status: " + brokerStatusView.getClusterMode()
                    + ", logpos:" + brokerStatusView.getCurrentLedgerId() + "," + brokerStatusView.getCurrentSequenceNumber() + ",Tasks:" + brokerStatusView.getTasks()
                    + ", waiting:" + brokerStatusView.getWaitingTasks()
                    + ", running:" + brokerStatusView.getRunningTasks()
                    + ", error:" + brokerStatusView.getErrorTasks()
                    + ", finished:" + brokerStatusView.getFinishedTasks() + ","
                    + "Transactions: count " + transactions.getTransactions().size() + ", oldest " + oldestTransaction + ", "
                    + "TasksHeap: size " + heap.getTasks().size() + ", first " + first + ", last " + last + ", "
                    + "DelayedTasksQueue: size " + delayedQueue.getTasks().size() + ", average delay " + averageDelayInSeconds + ", "
                    + "Slots: " + slots.getBusySlots().size());
        }

    }

    public void start() {
        if (this.timer != null) {
            this.timer.scheduleAtFixedRate(new Monitor(), 1000, configuration.getBrokerStatusMonitorPeriod(), TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (this.timer != null) {
            this.timer.shutdown();
        }
    }

}

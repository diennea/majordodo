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
package dodo.task;

import dodo.task.Broker;
import dodo.task.BrokerConfiguration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Schedules checkpoints
 *
 * @author enrico.olivelli
 */
public class CheckpointScheduler {

    private final BrokerConfiguration configuration;
    private final ScheduledExecutorService timer;
    private final Broker broker;
    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());

    public CheckpointScheduler(BrokerConfiguration configuration, Broker broker) {
        this.configuration = configuration;
        this.broker = broker;
        this.timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "dodo-broker-checkpoint-thread");
                t.setDaemon(true);
                return t;
            }
        });
    }

    private class Checkpoint implements Runnable {

        @Override
        public void run() {
            try {
                broker.checkpoint();
            } catch (LogNotAvailableException error) {
                LOGGER.log(Level.SEVERE, "fatal error during checkpoint", error);
            }
        }

    }

    public void start() {
        this.timer.scheduleAtFixedRate(new Checkpoint(), configuration.getCheckpointTime(), configuration.getCheckpointTime(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        this.timer.shutdown();
    }

}

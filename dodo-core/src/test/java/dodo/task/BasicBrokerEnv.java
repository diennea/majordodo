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

import dodo.client.ClientFacade;
import dodo.clustering.MemoryCommitLog;
import dodo.network.BrokerLocator;
import dodo.network.jvm.JVMBrokerLocator;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import org.junit.After;
import org.junit.Before;

/**
 * Basic Utils to have a broker
 *
 * @author enrico.olivelli
 */
public abstract class BasicBrokerEnv {

    protected Broker broker;
    private BrokerLocator locator;

//    @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.ALL;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter() {
//
//            @Override
//            public synchronized String format(LogRecord record) {
//                if (record.getThrown() != null) {
//                    return super.format(record);
//                } else {
//                    return record.getThreadID() + " - " + record.getLoggerName() + " - " + java.text.MessageFormat.format(record.getMessage(), record.getParameters()) + "\r\n";
//                }
//            }
//
//        };
//
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }

    public BrokerLocator getBrokerLocator() {
        if (locator == null) {
            locator = createBrokerLocator();
        }
        return locator;
    }

    public ClientFacade getClient() {
        return broker.getClient();
    }

    protected BrokerLocator createBrokerLocator() {
        return new JVMBrokerLocator(broker);
    }

    @Before
    public void startBroker() {
        broker = new Broker(new MemoryCommitLog());
        broker.start();
    }

    @After
    public void stopBroker() {
        if (locator != null) {
            locator.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }
}

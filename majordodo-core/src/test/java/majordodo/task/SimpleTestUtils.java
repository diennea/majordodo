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

import java.util.Arrays;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author francesco.caliumi
 */
public class SimpleTestUtils {
    
    public static String getThreadName(long threadid) {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getId() == threadid) {
                return t.getName();
            }
        }
        return null;
    }
    
    public static void checkResourcesUsage(Broker broker, String workerId, String[] resources, int expectedVal) {
        // a little dirty, this function should be called inside TasksHeap writeLock
        broker.getGlobalResourceUsageCounters().updateResourceCounters();
                    
        Map<String, Integer> countersGlobal = broker.getGlobalResourceUsageCounters().getCountersView();
        for (String resource : Arrays.asList(resources)) {
            System.out.println("Global counter resource=" + resource + "; counter=" + countersGlobal.get(resource));
            if (expectedVal > 0) {
                assertNotNull(countersGlobal.get(resource));
            }
            if (countersGlobal.get(resource) != null) {
                assertEquals(expectedVal, countersGlobal.get(resource).intValue());
            }
        }
        
        if (workerId != null) {
            broker.getWorkers().getWorkerManager(workerId).getResourceUsageCounters().updateResourceCounters();
            
            Map<String, Integer> countersWorker = broker.getWorkers().getWorkerManager(workerId)
                .getResourceUsageCounters().getCountersView();
            for (String resource : Arrays.asList(resources)) {
                System.out.println("Worker counter resource=" + resource + "; counter=" + countersWorker.get(resource));
                if (expectedVal > 0) {
                    assertNotNull(countersWorker.get(resource));
                }
                if (countersWorker.get(resource) != null) {
                    assertEquals(expectedVal, countersWorker.get(resource).intValue());
                }
            }
        }
    }
    
    public static void setupLogger(Level level) throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
            }
        });
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        ch.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return "" + new java.sql.Timestamp(record.getMillis()) + " " + record.getLevel() + " [" + getThreadName(record.getThreadID()) + "<" + record.getThreadID() + ">] " + record.getLoggerName() + ": " + formatMessage(record) + "\n";
            }
        });
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }
}

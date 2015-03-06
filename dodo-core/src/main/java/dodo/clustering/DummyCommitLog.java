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
package dodo.clustering;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dummy commit log
 *
 * @author enrico.olivelli
 */
public class DummyCommitLog extends CommitLog {

    private final AtomicLong sequenceNumber = new AtomicLong();
    private final ExecutorService service = Executors.newCachedThreadPool();

    @Override
    public void logAction(Action action, ActionLogCallback callback) {
        long newNumber = sequenceNumber.incrementAndGet();
        service.submit(() -> {
            callback.actionCommitted(new LogSequenceNumber(1, newNumber), null);
        });
    }

}

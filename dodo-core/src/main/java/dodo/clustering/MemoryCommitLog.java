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
 * In memory commit log
 *
 * @author enrico.olivelli
 */
public class MemoryCommitLog extends StatusChangesLog {

    private final AtomicLong sequenceNumber = new AtomicLong();
    private final ExecutorService service = Executors.newCachedThreadPool();

    @Override
    public LogSequenceNumber logStatusEdit(StatusEdit action) {
        long newNumber = sequenceNumber.incrementAndGet();
        return new LogSequenceNumber(1, newNumber);
    }

}

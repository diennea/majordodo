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

/**
 * Log of mofications, this is the base of the replication system
 *
 * @author enrico.olivelli
 */
public abstract class CommitLog {

    public static interface ActionLogCallback {

        public void actionCommitted(LogSequenceNumber number, Throwable error);
    }

    public abstract void logEvent(Event action, ActionLogCallback callback);

}

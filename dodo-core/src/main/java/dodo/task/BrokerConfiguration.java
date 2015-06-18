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

/**
 * Broker configuration
 *
 * @author enrico.olivelli
 */
public class BrokerConfiguration {

    private int maxWorkerIdleTime = 30000;

    /**
     * Maximum time to wait for a worker to demonstrate its liveness
     *
     * @return
     */
    public int getMaxWorkerIdleTime() {
        return maxWorkerIdleTime;
    }

    public void setMaxWorkerIdleTime(int maxWorkerIdleTime) {
        this.maxWorkerIdleTime = maxWorkerIdleTime;
    }

}

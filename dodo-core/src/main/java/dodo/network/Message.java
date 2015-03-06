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
package dodo.network;

import java.util.HashMap;
import java.util.Map;

/**
 * A message (from broker to worker or from worker to broker)
 *
 * @author enrico.olivelli
 */
public final class Message {

    public static Message KILL_WORKER(String brokerToken, String workerProcessId) {
        return new Message(brokerToken, workerProcessId, TYPE_KILL_WORKER, null);
    }

    public static Message ERROR(String brokerToken, String workerProcessId, Throwable error) {
        Map<String, Object> params = new HashMap<>();
        params.put("error", error);
        return new Message(brokerToken, workerProcessId, TYPE_ERROR, params);
    }

    public static Message ACK(String brokerToken, String workerProcessId) {
        return new Message(brokerToken, workerProcessId, TYPE_ACK, null);
    }

    public final String brokerToken;
    public final String workerProcessId;
    public final int type;
    public final Map<String, Object> parameters;

    public static final int TYPE_TASK_FINISHED = 1;
    public static final int TYPE_KILL_WORKER = 2;
    public static final int TYPE_ERROR = 3;
    public static final int TYPE_ACK = 4;

    public Message(String brokerToken, String workerProcessId, int type, Map<String, Object> parameters) {
        this.brokerToken = brokerToken;
        this.workerProcessId = workerProcessId;
        this.type = type;
        this.parameters = parameters;
    }

}

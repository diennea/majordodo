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
package dodo.client;

import dodo.clustering.Action;
import dodo.clustering.ActionResult;
import dodo.task.Broker;
import dodo.task.InvalidActionException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Client API
 *
 * @author enrico.olivelli
 */
public class ClientFacade {

    private Broker broker;

    public ClientFacade(Broker broker) {
        this.broker = broker;
    }

    public static interface ClientActionCallback {

        public void actionResult(long taskId, Throwable error);
    }

    /**
     * Submit a new task to the broker
     *
     * @param taskType
     * @param queueName
     * @param queueTag
     * @param parameters
     * @param callback
     */
    public void submitTaskAsync(String taskType, String queueName, String queueTag, Map<String, Object> parameters, ClientActionCallback callback) {
        Action addTask = Action.ADD_TASK(queueName, taskType, parameters, queueTag);
        try {
            broker.executeAction(addTask, new Broker.ActionCallback() {

                @Override
                public void actionExecuted(Action action, ActionResult result) {
                    if (result.error != null) {
                        callback.actionResult(0, result.error);
                    } else {
                        callback.actionResult(result.taskId, null);
                    }
                }
            });
        } catch (InterruptedException | InvalidActionException err) {
            callback.actionResult(0, err);
        }
    }

    public long submitTask(String taskType, String queueName, String queueTag, Map<String, Object> parameters) throws Exception {

        CompletableFuture<Long> result = new CompletableFuture<>();
        submitTaskAsync(taskType, queueName, queueTag, parameters, new ClientActionCallback() {

            @Override
            public void actionResult(long taskId, Throwable error) {
                if (error != null) {
                    result.complete(taskId);
                } else {
                    result.completeExceptionally(error);
                }
            }
        });

        try {
            return result.get();
        } catch (ExecutionException err) {
            throw new Exception(err.getCause());
        }

    }
}

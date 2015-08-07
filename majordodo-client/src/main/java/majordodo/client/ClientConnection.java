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
package majordodo.client;

import java.io.IOException;
import java.util.List;

/**
 * Generic Connection to the broker
 *
 * @author enrico.olivelli
 */
public interface ClientConnection extends AutoCloseable {

    void close() throws ClientException;

    void commit() throws ClientException;

    BrokerStatus getBrokerStatus() throws ClientException;

    TaskStatus getTaskStatus(String id) throws ClientException;

    boolean isTransacted();

    void rollback() throws ClientException;

    void setTransacted(boolean transacted);

    /**
     * Submit a single task
     *
     * @param request
     * @return the result of the submission. If using slots maybe the taskId
     * would be null or empty, and outcome will describe the reason
     * @throws majordodo.client.ClientException
     * @throws IOException
     */
    SubmitTaskResponse submitTask(SubmitTaskRequest request) throws ClientException;

    /**
     * Submit multiple task
     *
     * @param requests
     * @param request
     * @return the result of the submission. If using slots maybe the taskId
     * would be null or empty, and outcome will describe the reason
     * @throws majordodo.client.ClientException
     * @throws IOException
     */
    List<SubmitTaskResponse> submitTasks(List<SubmitTaskRequest> requests) throws ClientException;

}

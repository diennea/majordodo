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

    @Override
    void close() throws ClientException;

    /**
     * Commits the current transaction
     *
     * @throws ClientException
     */
    void commit() throws ClientException;

    /**
     * Gets actual broker status
     *
     * @return
     * @throws ClientException
     */
    BrokerStatus getBrokerStatus() throws ClientException;

    /**
     * Gets the status of a task
     *
     * @param id
     * @return
     * @throws ClientException
     */
    TaskStatus getTaskStatus(String id) throws ClientException;

    /**
     * Tells whether this connection is transacted, that it that it creates
     * transaction
     *
     * @return
     */
    boolean isTransacted();

    /**
     * Rollbacks current uncommitted work
     *
     * @throws ClientException
     */
    void rollback() throws ClientException;

    /**
     * Changes transaction mode. Transaction mode cannot be changed if a
     * transaction is active
     *
     * @param transacted
     */
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

    /**
     * Creates a new CodePool
     *
     * @param request
     * @return
     * @throws ClientException
     */
    CreateCodePoolResult createCodePool(CreateCodePoolRequest request) throws ClientException;

    /**
     * Deletes a CodePool if exists
     *
     * @param codePoolId
     * @throws ClientException
     */
    void deleteCodePool(String codePoolId) throws ClientException;
    
    /**
     * Get Info about a CodePool. Returns null if the CodePool does not exist
     * @param codePoolId
     * @return
     * @throws ClientException 
     */
    CodePoolStatus getCodePoolStatus(String codePoolId) throws ClientException;

}

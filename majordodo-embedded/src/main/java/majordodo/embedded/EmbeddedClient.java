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
package majordodo.embedded;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.client.BrokerStatus;
import majordodo.clientfacade.BrokerStatusView;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.clientfacade.SubmitTaskResult;
import majordodo.client.TaskStatus;
import majordodo.clientfacade.TaskStatusView;
import majordodo.client.ClientConnection;
import majordodo.client.ClientException;
import majordodo.client.CodePoolStatus;
import majordodo.client.CreateCodePoolRequest;
import majordodo.client.CreateCodePoolResult;
import majordodo.client.TaskSubmitter;
import majordodo.clientfacade.CodePoolView;
import majordodo.network.jvm.JVMBrokersRegistry;
import majordodo.task.Broker;

/**
 * Client to the embedded broker
 *
 * @author enrico.olivelli
 */
public class EmbeddedClient implements AutoCloseable {

    public ClientConnection openConnection() {
        return new EmbeddedBrokerConnection(JVMBrokersRegistry.getDefaultBroker());
    }

    @Override
    public void close() {
    }

    private static class EmbeddedBrokerConnection implements ClientConnection {

        Broker broker;
        long transactionId;
        boolean transacted;
        TaskSubmitter submitter;

        @Override
        public TaskSubmitter submitter() {
            if (submitter == null) {
                submitter = new TaskSubmitter(this);
            }
            return submitter;
        }

        public EmbeddedBrokerConnection(Broker broker) {
            if (broker == null) {
                throw new IllegalStateException("embedded broker not registered, maybe it has not started yet");
            }
            this.broker = broker;
        }

        @Override
        public void close() throws ClientException {
        }

        @Override
        public void commit() throws ClientException {
            if (transactionId > 0) {
                try {
                    this.broker.getClient().commitTransaction(transactionId);
                } catch (Exception err) {
                    throw new ClientException(err);
                }
                transactionId = 0;
            }
        }

        @Override
        public BrokerStatus getBrokerStatus() throws ClientException {
            BrokerStatus res = new BrokerStatus();
            BrokerStatusView brokerStatus = broker.getClient().getBrokerStatus();
            res.setCurrentLedgerId(brokerStatus.getCurrentLedgerId() + "");
            res.setCurrentSequenceNumber(brokerStatus.getCurrentSequenceNumber() + "");
            res.setErrortasks(brokerStatus.getErrorTasks());
            res.setFinishedtasks(brokerStatus.getFinishedTasks());
            res.setPendingtasks(brokerStatus.getPendingTasks());
            res.setRunningtasks(brokerStatus.getRunningTasks());
            res.setStatus(brokerStatus.getClusterMode());
            res.setTasks(brokerStatus.getTasks());
            res.setVersion(Broker.VERSION());
            res.setWaitingtasks(brokerStatus.getWaitingTasks());
            return res;
        }

        @Override
        public TaskStatus getTaskStatus(String id) throws ClientException {
            TaskStatusView t = broker.getClient().getTask(Long.parseLong(id));
            if (t == null) {
                return null;
            }
            TaskStatus res = new TaskStatus();
            res.setAttempts(t.getAttempts());
            res.setCreatedTimestamp(t.getCreatedTimestamp());
            res.setData(t.getData());
            res.setDeadline(t.getExecutionDeadline());
            res.setMaxattempts(t.getMaxattempts());
            res.setResult(t.getResult());
            res.setSlot(t.getSlot());
            String status = TaskStatusView.convertTaskStatusForClient(t.getStatus());
            res.setStatus(status);
            res.setTaskId(t.getTaskId() + "");
            res.setTasktype(t.getType());
            res.setUserId(t.getUser());
            res.setWorkerId(t.getWorkerId());

            return res;
        }

        protected void beginTransaction() throws ClientException {
            try {
                this.transactionId = broker.getClient().beginTransaction();
            } catch (Exception err) {
                throw new ClientException(err);
            }
        }

        @Override
        public boolean isTransacted() {
            return transacted;
        }

        @Override
        public void rollback() throws ClientException {
            if (transactionId > 0) {
                try {
                    this.broker.getClient().rollbackTransaction(transactionId);
                } catch (Exception err) {
                    throw new ClientException(err);
                }
            }
        }

        @Override
        public void setTransacted(boolean transacted) {
            if (transactionId > 0) {
                throw new IllegalStateException("cannot change transaction mode during transaction");
            }
            this.transacted = transacted;
        }

        @Override
        public SubmitTaskResponse submitTask(SubmitTaskRequest request) throws ClientException {
            ensureTransaction();
            long deadline = 0;
            if (request.getTimeToLive() > 0) {
                deadline = (System.currentTimeMillis() + request.getTimeToLive());
            }
            try {
                if (request.getTasktype() == null || request.getTasktype().isEmpty()) {
                    throw new ClientException("invalid tasktype " + request.getTasktype());
                }
                if (request.getMaxattempts() < 0) {
                    throw new ClientException("invalid Maxattempts " + request.getMaxattempts());
                }
                if (request.getAttempt() > 0 && request.getMaxattempts() > 0 && request.getAttempt() >= request.getMaxattempts()) {
                    throw new ClientException("invalid Maxattempts " + request.getMaxattempts() + " with attempt " + request.getAttempt());
                }
                SubmitTaskResult submitTask = broker.getClient().submitTask(
                    new AddTaskRequest(transactionId, request.getTasktype(), request.getUserid(), request.getData(),
                        request.getMaxattempts(), request.getRequestedStartTime(), deadline, request.getSlot(), request.getAttempt(), null, null));
                SubmitTaskResponse resp = new SubmitTaskResponse();
                resp.setTaskId(submitTask.getTaskId() + "");
                if (submitTask.getOutcome() != null) {
                    resp.setOutcome(submitTask.getOutcome());
                } else {
                    resp.setOutcome("");
                }
                return resp;
            } catch (Exception err) {
                throw new ClientException(err);
            }
        }

        @Override
        public List<SubmitTaskResponse> submitTasks(List<SubmitTaskRequest> requestlist) throws ClientException {
            ensureTransaction();
            List<AddTaskRequest> requests = new ArrayList<>(requestlist.size());
            for (SubmitTaskRequest request : requestlist) {
                long deadline = 0;
                if (request.getTimeToLive() > 0) {
                    deadline = (System.currentTimeMillis() + request.getTimeToLive());
                }
                if (request.getTasktype() == null || request.getTasktype().isEmpty()) {
                    throw new ClientException("invalid tasktype " + request.getTasktype());
                }
                if (request.getMaxattempts() < 0) {
                    throw new ClientException("invalid Maxattempts " + request.getMaxattempts());
                }
                if (request.getAttempt() > 0 && request.getMaxattempts() > 0 && request.getAttempt() >= request.getMaxattempts()) {
                    throw new ClientException("invalid Maxattempts " + request.getMaxattempts() + " with attempt " + request.getAttempt());
                }
                requests.add(new AddTaskRequest(transactionId, request.getTasktype(), request.getUserid(), request.getData(),
                        request.getMaxattempts(), request.getRequestedStartTime(), deadline, request.getSlot(), request.getAttempt(), null, null));
            }
            try {
                List<SubmitTaskResult> submitTasks = broker.getClient().submitTasks(requests);
                List<SubmitTaskResponse> results = new ArrayList<>(requestlist.size());
                for (SubmitTaskResult submitTask : submitTasks) {
                    SubmitTaskResponse resp = new SubmitTaskResponse();
                    resp.setTaskId(submitTask.getTaskId() + "");
                    if (submitTask.getOutcome() != null) {
                        resp.setOutcome(submitTask.getOutcome());
                    } else {
                        resp.setOutcome("");
                    }
                    results.add(resp);
                }
                return results;
            } catch (Exception err) {
                throw new ClientException(err);
            }
        }

        @Override
        public CreateCodePoolResult createCodePool(CreateCodePoolRequest request) throws ClientException {
            try {
                majordodo.clientfacade.CreateCodePoolResult res = broker.getClient().createCodePool(new majordodo.clientfacade.CreateCodePoolRequest(request.getCodePoolID(), System.currentTimeMillis(), request.getTtl(), request.getCodePoolData().getBytes(StandardCharsets.UTF_8)));
                CreateCodePoolResult cr = new CreateCodePoolResult();
                if (!res.ok) {
                    throw new Exception("createCodePool failed " + res.outcome);
                }
                cr.setOk(true);
                return cr;
            } catch (Exception err) {
                throw new ClientException(err);
            }
        }

        @Override
        public void deleteCodePool(String codePoolId) throws ClientException {
            try {
                broker.getClient().deleteCodePool(codePoolId);
            } catch (Exception err) {
                throw new ClientException(err);
            }
        }

        @Override
        public CodePoolStatus getCodePoolStatus(String codePoolId) throws ClientException {
            try {
                CodePoolView codePool = broker.getClient().getCodePool(codePoolId);
                if (codePool == null) {
                    return null;
                }
                CodePoolStatus s = new CodePoolStatus();
                s.setCreationTimestamp(codePool.getCreationTimestamp());
                s.setId(codePool.getCodePoolId());
                return s;
            } catch (Exception ex) {
                throw new ClientException(ex);
            }
        }

        private void ensureTransaction() throws ClientException {
            if (transacted && transactionId <= 0) {
                beginTransaction();
            }
        }

    }
}

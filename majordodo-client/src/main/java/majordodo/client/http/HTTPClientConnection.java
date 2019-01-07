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
package majordodo.client.http;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import majordodo.client.ClientConnection;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import majordodo.client.BrokerAddress;
import majordodo.client.BrokerDiscoveryService;
import majordodo.client.BrokerStatus;
import majordodo.client.ClientException;
import majordodo.client.CodePoolStatus;
import majordodo.client.CreateCodePoolRequest;
import majordodo.client.CreateCodePoolResult;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
import majordodo.client.TaskSubmitter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.codehaus.jackson.map.ObjectMapper;

@SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
        justification = "https://github.com/spotbugs/spotbugs/issues/756")
public class HTTPClientConnection implements ClientConnection {

    private static final Logger LOGGER = Logger.getLogger(HTTPClientConnection.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String transactionId;
    private boolean transacted;

    private HttpClientContext context;

    private final CloseableHttpClient httpclient;
    private final ClientConfiguration configuration;
    private static final boolean debug = Boolean.getBoolean("majordodo.client.debug");
    private BrokerAddress _broker;
    private final BrokerDiscoveryService discoveryService;
    private TaskSubmitter submitter;

    public HTTPClientConnection(CloseableHttpClient client, ClientConfiguration configuration, BrokerDiscoveryService discoveryService) {
        this.httpclient = client;
        this.configuration = configuration;
        this.discoveryService = discoveryService;
    }

    private BrokerAddress getBroker() throws IOException {
        if (_broker == null) {
            _broker = discoveryService.getLeaderBroker();
            if (_broker == null) {
                throw new IOException("not leader broker is available");
            }
        }
        return _broker;
    }

    private void brokerFailed() {
        if (_broker != null) {
            discoveryService.brokerFailed(_broker);
            _broker = null;
        }
        context = null;
    }

    private HttpClientContext getContext() throws IOException {
        if (context == null) {
            BrokerAddress broker = getBroker();
            String scheme = broker.getProtocol();
            HttpHost targetHost = new HttpHost(broker.getAddress(), broker.getPort(), scheme);

            context = HttpClientContext.create();
            if (configuration.getUsername() != null && !configuration.getUsername().isEmpty()) {
                UsernamePasswordCredentials creds = new UsernamePasswordCredentials(configuration.getUsername(), configuration.getPassword());
                CredentialsProvider credsProvider = new BasicCredentialsProvider();
                credsProvider.setCredentials(
                    new AuthScope(targetHost.getHostName(), targetHost.getPort(), AuthScope.ANY_REALM, AuthScope.ANY_SCHEME),
                    creds);
                BasicAuthCache authCache = new BasicAuthCache();
                BasicScheme basicAuth = new BasicScheme();
                authCache.put(targetHost, basicAuth);
                context.setCredentialsProvider(credsProvider);
                context.setAuthCache(authCache);
            }

        }
        return context;
    }

    public static Map<String, Object> map(Object... objects) {
        if (objects.length % 2 != 0) {
            throw new RuntimeException("bad argument list " + objects.length + ": " + Arrays.toString(objects));
        }

        HashMap<String, Object> m = new HashMap<>();
        for (int i = 0; i < objects.length; i += 2) {
            m.put((String) objects[i], objects[i + 1]);
        }

        return m;
    }

    @Override
    public final void commit() throws ClientException {
        if (transactionId == null) {
            return;
        }
        commitTransaction(transactionId);
        transactionId = null;
    }

    @Override
    public final void rollback() throws ClientException {
        if (transactionId == null) {
            return;
        }
        rollbackTransaction(transactionId);
        transactionId = null;
    }

    @Override
    public final boolean isTransacted() {
        return transacted;
    }

    @Override
    public final void setTransacted(boolean transacted) {
        if (transactionId != null) {
            throw new IllegalStateException("cannot change transaction mode during transaction");
        }
        this.transacted = transacted;
    }

    private Map<String, Object> request(String method, Map<String, Object> data) throws ClientException {

        try {
            final int MAX_RETRIES = this.configuration.getBrokerNotAvailableRetries();
            for (int i = 0; i < MAX_RETRIES; i++) {
                try {
                    String s = MAPPER.writeValueAsString(data);
                    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                    Map<String, Object> rr;
                    if (method.equals("POST")) {
                        rr = post("application/json;charset=utf-8", bytes);
                    } else if (method.equals("GET")) {
                        String path = data.entrySet().stream().map((entry) -> {
                            try {
                                return entry.getKey() + "=" + URLEncoder.encode(entry.getValue().toString(), "utf-8");
                            } catch (UnsupportedEncodingException err) {
                                return "";
                            }
                        }).collect(Collectors.joining("&"));
                        rr = get("?" + path);
                    } else {
                        throw new IllegalStateException(method);
                    }
                    if (!"true".equals(rr.get("ok") + "")) {
                        LOGGER.log(Level.SEVERE, "error from {0}: {1}", new Object[]{_broker, rr});
                        brokerFailed();
                        String error = rr.get("error") + "";
                        if (error.contains("broker_not_started") // broker does not exist on JVM
                            || error.contains("recovery_in_progress") // broker is in recovery mode, maybe it would become leader
                            || error.contains("broker_not_leader")) // broker is not leader
                        {
                            throw new RetryableError(rr + "");
                        } else {
                            throw new IOException("error from broker: " + rr);
                        }

                    }
                    return rr;
                } catch (RetryableError retry) {
                    int interval = this.configuration.getBrokerNotAvailableRetryInterval() * (i + 1);
                    LOGGER.log(Level.SEVERE, "retry on #{0}error from {1}: {2}: sleep {3} ms", new Object[]{i + 1, _broker, retry, interval});
                    Thread.sleep(interval);
                }
            }
            throw new IOException("could not issue request after " + MAX_RETRIES + " trials");
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            brokerFailed();
            throw new ClientException(err);
        } catch (Exception err) {
            brokerFailed();
            throw new ClientException(err);
        }
    }

    protected void beginTransaction() throws ClientException {
        Map<String, Object> res = request("POST", map("action", "beginTransaction"));
        this.transactionId = res.get("transaction") + "";
    }

    protected void commitTransaction(String id) throws ClientException {
        request("POST", map("action", "commitTransaction", "transaction", id));
    }

    protected void rollbackTransaction(String id) throws ClientException {
        request("POST", map("action", "rollbackTransaction", "transaction", id));
    }

    @Override
    public SubmitTaskResponse submitTask(SubmitTaskRequest request) throws ClientException {
        if (request.getUserid() == null || request.getUserid().isEmpty()) {
            throw new ClientException("invalid userid " + request.getUserid());
        }
        if (request.getTasktype() == null || request.getTasktype().isEmpty()) {
            throw new ClientException("invalid tasktype " + request.getTasktype());
        }
        if (request.getMaxattempts() < 0) {
            throw new ClientException("invalid Maxattempts " + request.getMaxattempts());
        }
        ensureTransaction();
        Map<String, Object> reqdata = new HashMap<>();
        reqdata.put("action", "submitTask");
        reqdata.put("userid", request.getUserid());
        reqdata.put("tasktype", request.getTasktype());
        reqdata.put("data", request.getData());
        if (request.getMode() != null && !SubmitTaskRequest.MODE_FACTORY.equals(request.getMode())) {
            reqdata.put("mode", request.getMode());
        }
        if (request.getCodePoolId() != null) {
            reqdata.put("codePoolId", request.getCodePoolId());
        }
        reqdata.put("maxattempts", request.getMaxattempts() + "");
        if (request.getAttempt() > 0) {
            reqdata.put("attempt", request.getAttempt() + "");
        }
        if (request.getSlot() != null && !request.getSlot().isEmpty()) {
            reqdata.put("slot", request.getSlot());
        }
        if (request.getTimeToLive() > 0) {
            reqdata.put("deadline", (System.currentTimeMillis() + request.getTimeToLive()) + "");
        }
        if (request.getRequestedStartTime() > 0) {
            reqdata.put("requestedStartTime", request.getRequestedStartTime());
        }
        if (transactionId != null) {
            reqdata.put("transaction", transactionId);
        }
        Map<String, Object> result = request("POST", reqdata);
        SubmitTaskResponse response = new SubmitTaskResponse();
        if (result.get("taskId") != null) {
            String taskId = result.get("taskId") + "";
            if (!taskId.equals("0")) {
                response.setTaskId(taskId);
            }
        }
        if (result.get("outcome") != null) {
            response.setOutcome(result.get("outcome") + "");
        } else {
            response.setOutcome("");
        }
        return response;

    }

    @Override
    public List<SubmitTaskResponse> submitTasks(List<SubmitTaskRequest> requests) throws ClientException {

        ensureTransaction();
        Map<String, Object> fullreqdata = new HashMap<>();
        fullreqdata.put("action", "submitTasks");
        List<Map<String, Object>> tasks = new ArrayList<>();
        fullreqdata.put("tasks", tasks);
        for (SubmitTaskRequest request : requests) {
            Map<String, Object> reqdata = new HashMap<>();
            tasks.add(reqdata);

            if (request.getUserid() == null || request.getUserid().isEmpty()) {
                throw new ClientException("invalid userid " + request.getUserid());
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
            if (request.getMode() != null && !SubmitTaskRequest.MODE_FACTORY.equals(request.getMode())) {
                reqdata.put("mode", request.getMode());
            }
            if (request.getCodePoolId() != null) {
                reqdata.put("codePoolId", request.getCodePoolId());
            }
            reqdata.put("userid", request.getUserid());
            reqdata.put("tasktype", request.getTasktype());
            reqdata.put("data", request.getData());
            reqdata.put("maxattempts", request.getMaxattempts() + "");
            if (request.getAttempt() > 0) {
                reqdata.put("attempt", request.getAttempt() + "");
            }
            if (request.getSlot() != null && !request.getSlot().isEmpty()) {
                reqdata.put("slot", request.getSlot());
            }
            if (request.getTimeToLive() > 0) {
                reqdata.put("deadline", (System.currentTimeMillis() + request.getTimeToLive()) + "");
            }
            if (request.getRequestedStartTime() > 0) {
                reqdata.put("requestedStartTime", request.getRequestedStartTime());
            }
            if (transactionId != null) {
                reqdata.put("transaction", transactionId);
            }
        }

        Map<String, Object> results = request("POST", fullreqdata);
        List<Map<String, Object>> resultlist = (List<Map<String, Object>>) results.get("results");
        if (resultlist == null) {
            throw new ClientException("no results (" + results + ")");
        }
        List<SubmitTaskResponse> responses = new ArrayList<>(resultlist.size());
        for (Map<String, Object> result : resultlist) {
            SubmitTaskResponse response = new SubmitTaskResponse();
            if (result.get("taskId") != null) {
                String taskId = result.get("taskId") + "";
                if (!taskId.equals("0")) {
                    response.setTaskId(taskId);
                }
            }
            if (result.get("outcome") != null) {
                response.setOutcome(result.get("outcome") + "");
            } else {
                response.setOutcome("");
            }
            responses.add(response);
        }
        return responses;

    }

    @Override
    public TaskStatus getTaskStatus(String id) throws ClientException {
        Map<String, Object> data = request("GET", map("view", "task", "taskId", id));
        Map<String, Object> task = (Map<String, Object>) data.get("task");
        return deserializeTaskStatus(task);
    }

    @Override
    public CodePoolStatus getCodePoolStatus(String codePoolId) throws ClientException {
        Map<String, Object> data = request("GET", map("view", "codePool", "codePoolId", codePoolId));
        Map<String, Object> codePool = (Map<String, Object>) data.get("codePool");
        return deserializeCodePoolStatus(codePool);
    }

    @Override
    public BrokerStatus getBrokerStatus() throws ClientException {
        Map<String, Object> data = request("GET", map("view", "status"));
        BrokerStatus res = new BrokerStatus();
        res.setVersion(data.get("version") + "");
        res.setStatus(data.get("status") + "");
        if (data.get("tasks") != null) {
            res.setTasks(Long.parseLong(data.get("tasks") + ""));
        }
        if (data.get("pendingtasks") != null) {
            res.setPendingtasks(Long.parseLong(data.get("pendingtasks") + ""));
        }
        if (data.get("runningtasks") != null) {
            res.setRunningtasks(Long.parseLong(data.get("runningtasks") + ""));
        }
        if (data.get("waitingtasks") != null) {
            res.setWaitingtasks(Long.parseLong(data.get("waitingtasks") + ""));
        }
        if (data.get("errortasks") != null) {
            res.setErrortasks(Long.parseLong(data.get("errortasks") + ""));
        }
        if (data.get("finishedtasks") != null) {
            res.setFinishedtasks(Long.parseLong(data.get("finishedtasks") + ""));
        }
        if (data.get("currentLedgerId") != null) {
            res.setCurrentLedgerId(data.get("currentLedgerId") + "");
        }
        if (data.get("currentSequenceNumber") != null) {
            res.setCurrentSequenceNumber(data.get("currentSequenceNumber") + "");
        }
        return res;
    }

    protected final void ensureTransaction() throws ClientException {
        if (transacted && transactionId == null) {
            beginTransaction();
        }
    }

    @Override
    public void close() throws ClientException {
        if (transactionId != null) {
            rollback();
        }
    }

    private String getBaseUrl() throws IOException {
        BrokerAddress broker = getBroker();
        String base = broker.getProtocol() + "://" + broker.getAddress() + ":" + broker.getPort() + "" + broker.getPath();
        return base;
    }

    public static final class RetryableError extends IOException {

        public RetryableError(String message) {
            super(message);
        }

    }

    private Map<String, Object> post(String contentType, byte[] content) throws IOException {
        HttpPost httpget = new HttpPost(getBaseUrl());
        RequestConfig requestConfig = RequestConfig.custom()
            .setSocketTimeout(configuration.getSotimeout())
            .setConnectTimeout(configuration.getConnectionTimeout())
            .build();
        httpget.setConfig(requestConfig);
        ByteArrayEntity body = new ByteArrayEntity(content);
        body.setChunked(true);
        body.setContentType(contentType);
        httpget.setEntity(body);
        try (CloseableHttpResponse response1 = httpclient.execute(httpget, getContext());) {
            if (response1.getStatusLine().getStatusCode() != 200) {
                brokerFailed();
                throw new IOException("HTTP request failed: " + response1.getStatusLine());
            }
            return MAPPER.readValue(response1.getEntity().getContent(), Map.class);
        }
    }

    private Map<String, Object> get(String url) throws IOException {
        String base = getBaseUrl();
        HttpGet httpget = new HttpGet(base + url);
        RequestConfig requestConfig = RequestConfig.custom()
            .setSocketTimeout(configuration.getSotimeout())
            .setConnectTimeout(configuration.getConnectionTimeout())
            .build();
        httpget.setConfig(requestConfig);

        try (CloseableHttpResponse response1 = httpclient.execute(httpget, getContext());) {

            if (response1.getStatusLine().getStatusCode() != 200) {
                brokerFailed();
                throw new IOException("HTTP request failed: " + response1.getStatusLine());
            }
            return MAPPER.readValue(response1.getEntity().getContent(), Map.class);
        }
    }

    private CodePoolStatus deserializeCodePoolStatus(Map<String, Object> data) {
        if (data == null || !data.containsKey("codePoolId")) {
            return null;
        }
        CodePoolStatus res = new CodePoolStatus();
        res.setId((String) data.get("codePoolId"));
        res.setCreationTimestamp(Long.parseLong(data.get("creationTimestamp") + ""));
        return res;

    }

    private TaskStatus deserializeTaskStatus(Map<String, Object> task) {

        if (task.get("taskId") == null) {
            return null;
        }
        TaskStatus t = new TaskStatus();
        t.setAttempts(Integer.parseInt(task.get("attempts") + ""));
        t.setCreatedTimestamp(Long.parseLong(task.get("createdTimestamp") + ""));
        t.setData(task.get("data") + "");
        t.setDeadline(Long.parseLong(task.get("deadline") + ""));
        t.setRequestedStartTime(Long.parseLong(task.get("requestedStartTime") + ""));
        t.setMaxattempts(Integer.parseInt(task.get("maxattempts") + ""));
        t.setResult(task.get("result") + "");
        t.setSlot(task.get("slot") + "");
        t.setStatus(task.get("status") + "");
        String mode = (String) task.get("mode");
        if (mode == null) {
            mode = SubmitTaskRequest.MODE_FACTORY;
        }
        t.setMode(mode);
        t.setCodePoolId((String) task.get("codePoolId"));
        t.setTaskId(task.get("taskId") + "");
        t.setTasktype(task.get("tasktype") + "");
        t.setUserId(task.get("userId") + "");
        if (task.get("workerId") != null) {
            t.setWorkerId(task.get("workerId") + "");
        }
        return t;
    }

    @Override
    public CreateCodePoolResult createCodePool(CreateCodePoolRequest request) throws ClientException {
        request("POST", map("action", "createCodePool", "id", request.getCodePoolID(), "ttl", request.getTtl() + "",
            "data", request.getCodePoolData()));
        CreateCodePoolResult result = new CreateCodePoolResult();
        result.setOk(true);
        return result;

    }

    @Override
    public void deleteCodePool(String codePoolId) throws ClientException {
        request("POST", map("action", "deleteCodePool", "id", codePoolId));
    }

    @Override
    public TaskSubmitter submitter() {
        if (submitter == null) {
            submitter = new TaskSubmitter(this);
        }
        return submitter;
    }

}

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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import majordodo.client.BrokerStatus;
import majordodo.client.SubmitTaskRequest;
import majordodo.client.SubmitTaskResponse;
import majordodo.client.TaskStatus;
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

public class ClientConnection implements AutoCloseable {

    private String transactionId;
    private boolean transacted;

    private HttpClientContext context;

    private CloseableHttpClient httpclient;
    private ClientConfiguration configuration;
    private static boolean debug = Boolean.getBoolean("majordodo.client.debug");
    private InetSocketAddress broker;

    public ClientConnection(CloseableHttpClient client, ClientConfiguration configuration, InetSocketAddress broker) {
        this.httpclient = client;
        this.configuration = configuration;
        this.broker = broker;
    }

    private HttpClientContext getContext() {
        if (context == null) {
            String scheme = configuration.isUsessl() ? "https" : "http";
            HttpHost targetHost = new HttpHost(broker.getAddress().getHostAddress(), broker.getPort(), scheme);

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

    public final void commit() throws IOException {
        if (transactionId == null) {
            return;
        }
        commitTransaction(transactionId);
        transactionId = null;
    }

    public final void rollback() throws IOException {
        if (transactionId == null) {
            return;
        }
        rollbackTransaction(transactionId);
        transactionId = null;
    }

    public final boolean isTransacted() {
        return transacted;
    }

    public final void setTransacted(boolean transacted) {
        if (transactionId != null) {
            throw new IllegalStateException("cannot change transaction modo during transaction");
        }
        this.transacted = transacted;
    }

    private Map<String, Object> request(String method, Map<String, Object> data) throws IOException {

        try {
            ObjectMapper mapper = new ObjectMapper();

            String s = mapper.writeValueAsString(data);
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
                throw new Exception("error from broker: " + rr);
            }
            return rr;
        } catch (Exception err) {
            throw new IOException(err);
        }
    }

    protected void beginTransaction() throws IOException {
        Map<String, Object> res = request("POST", map("action", "beginTransaction"));
        this.transactionId = res.get("transaction") + "";
    }

    protected void commitTransaction(String id) throws IOException {
        request("POST", map("action", "commitTransaction", "transaction", id));
    }

    protected void rollbackTransaction(String id) throws IOException {
        request("POST", map("action", "rollbackTransaction", "transaction", id));
    }

    /**
     *
     * @param request
     * @return
     * @throws IOException
     */
    public SubmitTaskResponse submitTask(SubmitTaskRequest request) throws IOException {
        if (request.getUserid() == null || request.getUserid().isEmpty()) {
            throw new IOException("invalid userid " + request.getUserid());
        }
        if (request.getTasktype() == null || request.getTasktype().isEmpty()) {
            throw new IOException("invalid tasktype " + request.getTasktype());
        }
        if (request.getMaxattempts() < 0) {
            throw new IOException("invalid Maxattempts " + request.getMaxattempts());
        }
        ensureTransaction();
        Map<String, Object> reqdata = new HashMap<>();
        reqdata.put("action", "submitTask");
        reqdata.put("userid", request.getUserid());
        reqdata.put("tasktype", request.getTasktype());
        reqdata.put("data", request.getData());
        reqdata.put("maxattempts", request.getMaxattempts() + "");
        if (request.getSlot() != null && !request.getSlot().isEmpty()) {
            reqdata.put("slot", request.getSlot());
        }
        if (request.getTimeToLive() > 0) {
            reqdata.put("deadline", (System.currentTimeMillis() + request.getTimeToLive()) + "");
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
        return response;

    }

    public TaskStatus getTaskStatus(String id) throws IOException {
        Map<String, Object> data = request("GET", map("view", "task","taskId",id));
        Map<String, Object> task = (Map<String, Object>) data.get("task");        
        return deserializeTaskStatus(task);
    }

    public BrokerStatus getBrokerStatus() throws IOException {
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

    protected final void ensureTransaction() throws IOException {
        if (transacted && transactionId == null) {
            beginTransaction();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (transactionId != null) {
                rollback();
            }
        } finally {
            httpclient.close();

        }
    }

    private String getBaseUrl() {
        if (configuration.isUsessl()) {
            String base = "https://" + broker.getHostName() + ":" + broker.getPort() + "" + configuration.getClientApiPath();
            return base;
        } else {
            String base = "http://" + broker.getHostName() + ":" + broker.getPort() + "" + configuration.getClientApiPath();
            return base;
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
                throw new IOException("HTTP request failed: " + response1.getStatusLine());
            }
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response1.getEntity().getContent(), Map.class);
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
                throw new IOException("HTTP request failed: " + response1.getStatusLine());
            }
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response1.getEntity().getContent(), Map.class);
        }
    }

    private TaskStatus deserializeTaskStatus(Map<String, Object> task) {
        
        if (task.get("taskId")== null) {
            return null;
        }
        TaskStatus t = new TaskStatus();
        t.setAttempts(Integer.parseInt(task.get("attempts") + ""));
        t.setCreatedTimestamp(Long.parseLong(task.get("createdTimestamp") + ""));
        t.setData(task.get("data") + "");
        t.setDeadline(Long.parseLong(task.get("deadline") + ""));
        t.setMaxattempts(Integer.parseInt(task.get("maxattempts") + ""));
        t.setResult(task.get("result") + "");
        t.setSlot(task.get("slot") + "");
        t.setStatus(task.get("status") + "");
        t.setTaskId(task.get("taskId") + "");
        t.setTasktype(task.get("tasktype") + "");
        t.setUserId(task.get("userId") + "");
        if (task.get("workerId") != null) {
            t.setWorkerId(task.get("workerId") + "");
        }
        return t;
    }

}

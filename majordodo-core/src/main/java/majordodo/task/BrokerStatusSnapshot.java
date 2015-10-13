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
package majordodo.task;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Snapshot of the status of the broker
 *
 * @author enrico.olivelli
 */
public class BrokerStatusSnapshot {

    private static void serializeTransaction(Transaction transaction, JsonGenerator g) throws IOException {
        g.writeStartObject();
        writeSimpleProperty(g, "id", transaction.getTransactionId());
        writeSimpleProperty(g, "creationTimestamp", transaction.getCreationTimestamp());

        if (transaction.getPreparedTasks() != null && !transaction.getPreparedTasks().isEmpty()) {
            g.writeFieldName("preparedTasks");
            g.writeStartArray();
            for (Task t : transaction.getPreparedTasks()) {
                serializeTask(t, g);
            }
            g.writeEndArray();
        }
        g.writeEndObject();
    }

    List<Task> tasks = new ArrayList<>();
    List<WorkerStatus> workers = new ArrayList<>();
    List<Transaction> transactions = new ArrayList<>();
    long maxTaskId;
    long maxTransactionId;
    LogSequenceNumber actualLogSequenceNumber;

    public BrokerStatusSnapshot(long maxTaskId, long maxTransactionId, LogSequenceNumber actualLogSequenceNumber) {
        this.maxTaskId = maxTaskId;
        this.maxTransactionId = maxTransactionId;
        this.actualLogSequenceNumber = actualLogSequenceNumber;
    }

    public long getMaxTransactionId() {
        return maxTransactionId;
    }

    public void setMaxTransactionId(long maxTransactionId) {
        this.maxTransactionId = maxTransactionId;
    }

    public LogSequenceNumber getActualLogSequenceNumber() {
        return actualLogSequenceNumber;
    }

    public void setActualLogSequenceNumber(LogSequenceNumber actualLogSequenceNumber) {
        this.actualLogSequenceNumber = actualLogSequenceNumber;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    public List<WorkerStatus> getWorkers() {
        return workers;
    }

    public void setWorkers(List<WorkerStatus> workers) {
        this.workers = workers;
    }

    public long getMaxTaskId() {
        return maxTaskId;
    }

    public void setMaxTaskId(long maxTaskId) {
        this.maxTaskId = maxTaskId;
    }

    private static JsonToken nextToken(JsonParser jParser) throws IOException {
        jParser.nextToken();
        JsonToken currentToken = jParser.getCurrentToken();
        return currentToken;
    }

    private static String readValue(JsonParser jParser) throws IOException {
        String result = jParser.getText();

        return result;
    }

    public static BrokerStatusSnapshot deserializeSnapshot(InputStream in) throws IOException {
        JsonFactory jfactory = new JsonFactory();
        JsonParser jParser = jfactory.createJsonParser(in);

        long ledgerId = 0;
        long sequenceNumber = 0;
        long maxTaskId = 0;
        long maxTransactionId = 0;
        List< Transaction> transactions = new ArrayList<>();
        List<Task> tasks = new ArrayList<>();
        List<WorkerStatus> workers = new ArrayList<>();
        nextToken(jParser);

        while (jParser.nextToken() != JsonToken.END_OBJECT) {
            switch (jParser.getCurrentName() + "") {
                case "ledgerid": {
                    nextToken(jParser);
                    ledgerId = Long.parseLong(readValue(jParser)); // display mkyong
                    break;
                }
                case "sequenceNumber": {
                    nextToken(jParser);
                    sequenceNumber = Long.parseLong(readValue(jParser)); // display mkyong
                    break;
                }
                case "maxTaskId": {
                    nextToken(jParser);
                    maxTaskId = Long.parseLong(readValue(jParser)); // display mkyong                                        
                    break;
                }
                case "maxTransactionId": {
                    nextToken(jParser);
                    maxTransactionId = Long.parseLong(readValue(jParser)); // display mkyong
                    break;
                }
                case "transactions": {
                    nextToken(jParser); // field name                                        
                    while (jParser.nextToken() != JsonToken.END_ARRAY) {
                        Transaction transaction = readTransaction(jParser);
                        transactions.add(transaction);
                    }
                    break;
                }
                case "tasks": {
                    nextToken(jParser); // field name                                        
                    while (jParser.nextToken() != JsonToken.END_ARRAY) {
                        Task task = readTask(jParser);
                        tasks.add(task);
                    }
                    break;
                }
                case "workers": {
                    nextToken(jParser); // field name                                        
                    while (jParser.nextToken() != JsonToken.END_ARRAY) {
                        WorkerStatus workerStatus = readWorkerStatus(jParser);
                        workers.add(workerStatus);
                    }
                    break;
                }
                default:
                    throw new IOException("Unexpected field " + jParser.getCurrentName());
            }
        }
        BrokerStatusSnapshot res = new BrokerStatusSnapshot(maxTaskId, maxTransactionId, new LogSequenceNumber(ledgerId, sequenceNumber));
        res.setTransactions(transactions);
        res.setWorkers(workers);
        res.setTasks(tasks);
        return res;
    }

    private static WorkerStatus readWorkerStatus(JsonParser jParser) throws IOException {
        WorkerStatus res = new WorkerStatus();

        while (jParser.nextToken() != JsonToken.END_OBJECT) {
            switch (jParser.getCurrentName() + "") {
                case "workerId":
                    nextToken(jParser);
                    res.setWorkerId(readValue(jParser));
                    break;
                case "location":
                    nextToken(jParser);
                    res.setWorkerLocation(readValue(jParser));
                    break;
                case "processId":
                    nextToken(jParser);
                    res.setProcessId(readValue(jParser));
                    break;
                case "status":
                    nextToken(jParser);
                    res.setStatus(Integer.parseInt(readValue(jParser)));
                    break;
                case "lastConnectionTs":
                    nextToken(jParser);
                    res.setLastConnectionTs(Long.parseLong(readValue(jParser)));
                    break;
                default:
                    throw new IOException("Unexpected field " + jParser.getCurrentName());
            }
        }

        return res;
    }

    private static Transaction readTransaction(JsonParser jParser) throws IOException {
        long creationTimestamp = 0;
        long id = 0;
        List<Task> preparedTasks = new ArrayList<>();
        while (jParser.nextToken() != JsonToken.END_OBJECT) {

            switch (jParser.getCurrentName() + "") {
                case "creationTimestamp":
                    nextToken(jParser);
                    creationTimestamp = Long.parseLong(readValue(jParser));
                    break;
                case "id":
                    nextToken(jParser);
                    id = Long.parseLong(readValue(jParser));
                    break;
                case "preparedTasks":
                    nextToken(jParser);
                    while (jParser.nextToken() != JsonToken.END_ARRAY) {
                        Task task = readTask(jParser);
                        preparedTasks.add(task);
                    }
                    break;
                default:
                    throw new IOException("Unexpected field " + jParser.getCurrentName());
            }
        }
        Transaction res = new Transaction(id, creationTimestamp);
        if (!preparedTasks.isEmpty()) {
            res.getPreparedTasks().addAll(preparedTasks);
        }
        return res;

    }

    private static Task readTask(JsonParser jParser) throws NumberFormatException, IOException {
        Task task = new Task();
        while (jParser.nextToken() != JsonToken.END_OBJECT) {
            switch (jParser.getCurrentName() + "") {
                case "id":
                    nextToken(jParser);
                    task.setTaskId(Long.parseLong(readValue(jParser)));
                    break;
                case "status":
                    nextToken(jParser);
                    task.setStatus(Integer.parseInt(readValue(jParser)));
                    break;
                case "maxattempts":
                    nextToken(jParser);
                    task.setMaxattempts(Integer.parseInt(readValue(jParser)));
                    break;
                case "attempts":
                    nextToken(jParser);
                    task.setAttempts(Integer.parseInt(readValue(jParser)));
                    break;
                case "slot":
                    nextToken(jParser);
                    task.setSlot(readValue(jParser));
                    break;
                case "parameter":
                    nextToken(jParser);
                    task.setParameter(readValue(jParser));
                    break;
                case "result":
                    nextToken(jParser);
                    task.setResult(readValue(jParser));
                    break;
                case "userId":
                    nextToken(jParser);
                    task.setUserId(readValue(jParser));
                    break;
                case "type":
                    nextToken(jParser);
                    task.setType(readValue(jParser));
                    break;
                case "workerId":
                    nextToken(jParser);
                    task.setWorkerId(readValue(jParser));
                    break;
                case "createdTimestamp":
                    nextToken(jParser);
                    task.setCreatedTimestamp(Long.parseLong(readValue(jParser)));
                    break;
                case "executionDeadline":
                    nextToken(jParser);
                    task.setExecutionDeadline(Long.parseLong(readValue(jParser)));
                    break;
                default:
                    throw new IOException("Unexpected field " + jParser.getCurrentName());
            }
        }
        return task;
    }

    private static void writeSimpleProperty(JsonGenerator g, String name, String value) throws IOException {
        if (value != null) {
            g.writeFieldName(name);
            g.writeString(value);
        }
    }

    private static void writeSimpleProperty(JsonGenerator g, String name, long value) throws IOException {

        g.writeFieldName(name);
        g.writeNumber(value);
    }

    public static void serializeSnapshot(BrokerStatusSnapshot snapshotData, OutputStream out) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(out);
        g.writeStartObject();
        LogSequenceNumber actualLogSequenceNumber = snapshotData.getActualLogSequenceNumber();

        Map<String, Object> filedata = new HashMap<>();
        writeSimpleProperty(g, "ledgerid", actualLogSequenceNumber.ledgerId);
        writeSimpleProperty(g, "sequenceNumber", actualLogSequenceNumber.sequenceNumber);
        writeSimpleProperty(g, "maxTaskId", snapshotData.maxTaskId);
        writeSimpleProperty(g, "maxTransactionId", snapshotData.maxTransactionId);
        g.writeFieldName("tasks");
        g.writeStartArray();

        for (Task task : snapshotData.getTasks()) {
            serializeTask(task, g);
        }

        g.writeEndArray();
        g.writeFieldName("workers");
        g.writeStartArray();
        for (WorkerStatus worker : snapshotData.getWorkers()) {
            serializeWorker(worker, g);
        };
        g.writeEndArray();
        g.writeFieldName("transactions");
        g.writeStartArray();
        for (Transaction t : snapshotData.getTransactions()) {
            serializeTransaction(t, g);
        }
        g.writeEndArray();

        g.writeEndObject();
        g.flush();
    }

    private static void serializeTask(Task task, JsonGenerator g) throws IOException {
        g.writeStartObject();

        writeSimpleProperty(g, "id", task.getTaskId());
        writeSimpleProperty(g, "status", task.getStatus());
        writeSimpleProperty(g, "maxattempts", task.getMaxattempts());
        writeSimpleProperty(g, "slot", task.getSlot());
        writeSimpleProperty(g, "attempts", task.getAttempts());
        writeSimpleProperty(g, "executionDeadline", task.getExecutionDeadline());
        writeSimpleProperty(g, "parameter", task.getParameter());
        writeSimpleProperty(g, "result", task.getResult());
        writeSimpleProperty(g, "userId", task.getUserId());
        writeSimpleProperty(g, "createdTimestamp", task.getCreatedTimestamp());
        writeSimpleProperty(g, "type", task.getType());
        writeSimpleProperty(g, "workerId", task.getWorkerId());
        g.writeEndObject();
    }

    private static void serializeWorker(WorkerStatus worker, JsonGenerator g) throws IOException {
        g.writeStartObject();
        writeSimpleProperty(g, "workerId", worker.getWorkerId());
        writeSimpleProperty(g, "location", worker.getWorkerLocation());
        writeSimpleProperty(g, "processId", worker.getProcessId());
        writeSimpleProperty(g, "lastConnectionTs", worker.getLastConnectionTs());
        writeSimpleProperty(g, "status", worker.getStatus());
        g.writeEndObject();
    }

}

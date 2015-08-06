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

import majordodo.client.ClientFacade;
import majordodo.network.jvm.JVMBrokerSupportInterface;
import majordodo.network.jvm.JVMBrokersRegistry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import majordodo.client.BrokerStatusView;
import majordodo.client.HeapStatusView;
import majordodo.client.HeapStatusView.TaskStatus;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Global status of the broker
 *
 * @author enrico.olivelli
 */
public class Broker implements AutoCloseable, JVMBrokerSupportInterface {

    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());
    private String brokerId = UUID.randomUUID().toString();
    private Callable<Void> externalProcessChecker; // PIDFILECHECKER

    public Callable<Void> getExternalProcessChecker() {
        return externalProcessChecker;
    }

    public void setExternalProcessChecker(Callable<Void> externalProcessChecker) {
        this.externalProcessChecker = externalProcessChecker;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }

    public static String VERSION() {
        return "0.1.2";
    }

    public static byte[] formatHostdata(String host, int port, Map<String, String> additional) {
        try {
            Map<String, String> data = new HashMap<>();
            data.put("host", host);
            data.put("port", port + "");
            data.put("version", VERSION());
            if (additional != null) {
                data.putAll(additional);
            }
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            new ObjectMapper().writeValue(oo, data);
            return oo.toByteArray();
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    public static InetSocketAddress parseHostdata(byte[] oo) {

        try {
            Map<String, String> data = new ObjectMapper().readValue(new ByteArrayInputStream(oo), Map.class);
            String host = data.get("host");
            int port = Integer.parseInt(data.get("port"));
            return new InetSocketAddress(host, port);
        } catch (IOException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    private final Workers workers;
    public final TasksHeap tasksHeap;
    private final BrokerStatus brokerStatus;
    private final StatusChangesLog log;
    private final BrokerServerEndpoint acceptor;
    private final ClientFacade client;
    private volatile boolean started;
    private volatile boolean stopped;
    private final CountDownLatch stopperLatch=new CountDownLatch(1);

    private final BrokerConfiguration configuration;
    private final CheckpointScheduler checkpointScheduler;
    private final GroupMapperScheduler groupMapperScheduler;
    private final FinishedTaskCollectorScheduler finishedTaskCollectorScheduler;
    private final Thread brokerLifeThread;

    public BrokerConfiguration getConfiguration() {
        return configuration;
    }

    public ClientFacade getClient() {
        return client;
    }

    public Workers getWorkers() {
        return workers;
    }

    public BrokerStatus getBrokerStatus() {
        return brokerStatus;
    }

    public Broker(BrokerConfiguration configuration, StatusChangesLog log, TasksHeap tasksHeap) {
        this.configuration = configuration;
        this.workers = new Workers(this);
        this.acceptor = new BrokerServerEndpoint(this);
        this.client = new ClientFacade(this);
        this.brokerStatus = new BrokerStatus(log);
        this.tasksHeap = tasksHeap;
        this.log = log;
        this.checkpointScheduler = new CheckpointScheduler(configuration, this);
        this.groupMapperScheduler = new GroupMapperScheduler(configuration, this);
        this.finishedTaskCollectorScheduler = new FinishedTaskCollectorScheduler(configuration, this);
        this.brokerLifeThread = new Thread(brokerLife, "broker-life");
    }

    public void start() {
        JVMBrokersRegistry.registerBroker(brokerId, this);
        if (configuration.isClearStatusAtBoot()) {
            try {
                this.log.clear();
            } catch (LogNotAvailableException error) {
                LOGGER.log(Level.SEVERE, "Could not clear status at boot", error);
                throw new RuntimeException(error);
            }
        }
        this.brokerStatus.recover();
        // checkpoint must startboth in leader mode and in follower mode
        this.checkpointScheduler.start();
        this.brokerLifeThread.start();
        this.groupMapperScheduler.start();
    }

    public void startAsWritable() throws InterruptedException {
        this.start();
        while (!log.isWritable() && !log.isClosed()) {
            Thread.sleep(500);
        }
    }

    private final Runnable brokerLife = new Runnable() {

        @Override
        public void run() {
            try {
                LOGGER.log(Level.SEVERE, "Waiting to become leader...");
                brokerStatus.followTheLeader();
                if (stopped) {
                    return;
                }
                LOGGER.log(Level.SEVERE, "Starting as leader");
                brokerStatus.startWriting();
                for (Task task : brokerStatus.getTasksAtBoot()) {
                    switch (task.getStatus()) {
                        case Task.STATUS_WAITING:
                            LOGGER.log(Level.SEVERE, "Task " + task.getTaskId() + ", " + task.getType() + ", user=" + task.getUserId() + " is to be scheduled");
                            tasksHeap.insertTask(task.getTaskId(), task.getType(), task.getUserId());
                            break;
                    }
                }
                workers.start(brokerStatus);
                started = true;
                finishedTaskCollectorScheduler.start();
                try {
                    while (!stopped) {
                        noop(); // write something to long, this simple action detects fencing and forces flushes to other follower brokers
                        if (externalProcessChecker != null) {
                            externalProcessChecker.call();
                        }
                        stopperLatch.await(10, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException exit) {
                }
            } catch (Throwable uncaught) {
                LOGGER.log(Level.SEVERE, "fatal error", uncaught);
                uncaught.printStackTrace();
                shutdown();
            }
        }

    };
    

    private void shutdown() {
        stopperLatch.countDown();
        stopped = true;
        JVMBrokersRegistry.unregisterBroker(brokerId);
        this.finishedTaskCollectorScheduler.stop();
        this.checkpointScheduler.stop();
        this.groupMapperScheduler.stop();
        this.workers.stop();
        this.brokerStatus.close();
    }

    public void stop() {
        shutdown();
        try {
            brokerLifeThread.join();
        } catch (InterruptedException exit) {
        }
        started = false;
    }

    @Override
    public void close() {
        stop();
    }

    public BrokerServerEndpoint getAcceptor() {
        return acceptor;
    }

    public boolean isRunning() {
        return started;
    }
    
    public boolean isWritable(){
        return log.isWritable();
    }

    public List<Long> assignTasksToWorker(int max, Map<String, Integer> availableSpace, List<Integer> groups, Set<Integer> excludedGroups, String workerId) throws LogNotAvailableException {
        List<Long> tasks = tasksHeap.takeTasks(max, groups, excludedGroups, availableSpace);
        long now = System.currentTimeMillis();
        Set<Long> expired = null;
        for (long taskId : tasks) {
            Task task = this.brokerStatus.getTask(taskId);
            if (task != null) {
                long deadline = task.getExecutionDeadline();
                if (deadline > 0 && deadline < now) {
                    if (expired == null) {
                        expired = new HashSet<>();
                    }
                    expired.add(taskId);
                    LOGGER.log(Level.INFO, "task {0} deadline expired {1}", new Object[]{taskId, new java.util.Date(deadline)});
                    StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, null, Task.STATUS_ERROR, "deadline_expired");
                    this.brokerStatus.applyModification(edit);
                } else {
                    StatusEdit edit = StatusEdit.ASSIGN_TASK_TO_WORKER(taskId, workerId, task.getAttempts() + 1);
                    this.brokerStatus.applyModification(edit);
                }
            }
        }
        if (expired != null) {
            tasks.removeAll(expired);
        }
        return tasks;
    }

    public void checkpoint() throws LogNotAvailableException {
        this.brokerStatus.checkpoint(configuration.getTransactionsTtl());
    }

    void purgeTasks() {
        List<Long> expired = this.brokerStatus.purgeFinishedTasksAndSignalExpiredTasks(configuration.getFinishedTasksRetention(), configuration.getMaxExpiredTasksPerCycle());

        expired.stream().forEach((taskId) -> {
            try {
                StatusEdit change = StatusEdit.TASK_STATUS_CHANGE(taskId, null, Task.STATUS_ERROR, "deadline_expired");
                this.brokerStatus.applyModification(change);
                this.tasksHeap.removeExpiredTask(taskId);
            } catch (LogNotAvailableException logNotAvailableException) {
                LOGGER.log(Level.SEVERE, "error while expiring task " + taskId, logNotAvailableException);
            }
        });
    }

    public void recomputeGroups() {
        try {
            tasksHeap.recomputeGroups();
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "error during group mapping recomputation", t);
        }
    }

    public void noop() throws LogNotAvailableException {
        this.brokerStatus.applyModification(StatusEdit.NOOP());
    }

    public long beginTransaction() throws LogNotAvailableException, IllegalActionException {
        long transactionId = brokerStatus.nextTransactionId();
        StatusEdit edit = StatusEdit.BEGIN_TRANSACTION(transactionId, System.currentTimeMillis());
        BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(edit);
        if (result.error != null) {
            throw new IllegalActionException(result.error);
        }
        return transactionId;
    }

    public void commitTransaction(long id) throws LogNotAvailableException, IllegalActionException {
        StatusEdit edit = StatusEdit.COMMIT_TRANSACTION(id);
        BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(edit);
        if (result.error != null) {
            throw new IllegalActionException(result.error);
        }
        List<Task> preparedtasks = (List<Task>) result.data;
        for (Task task : preparedtasks) {
            this.tasksHeap.insertTask(task.getTaskId(), task.getType(), task.getUserId());
        }

    }

    public void rollbackTransaction(long id) throws LogNotAvailableException, IllegalActionException {
        StatusEdit edit = StatusEdit.ROLLBACK_TRANSACTION(id);
        BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(edit);
        if (result.error != null) {
            throw new IllegalActionException(result.error);
        }
    }

    public BrokerStatusView createBrokerStatusView() {
        BrokerStatusView res = new BrokerStatusView();
        if (log.isLeader()) {
            res.setClusterMode("LEADER");
        } else {
            res.setClusterMode("FOLLOWER");
        }
        res.setCurrentLedgerId(log.getCurrentLedgerId());
        res.setCurrentSequenceNumber(log.getCurrentSequenceNumber());
        res.setTasks(brokerStatus.getStats().getTasks());
        res.setPendingTasks(brokerStatus.getStats().getPendingTasks());
        res.setRunningTasks(brokerStatus.getStats().getRunningTasks());
        res.setWaitingTasks(brokerStatus.getStats().getWaitingTasks());
        res.setErrorTasks(brokerStatus.getStats().getErrorTasks());
        res.setFinishedTasks(brokerStatus.getStats().getFinishedTasks());
        return res;
    }

    public HeapStatusView getHeapStatusView() {
        HeapStatusView res = new HeapStatusView();
        tasksHeap.scan((task) -> {
            TaskStatus status = new TaskStatus();
            status.setGroup(task.groupid);
            status.setTaskId(task.taskid);
            status.setTaskType(tasksHeap.resolveTaskType(task.tasktype));
            res.getTasks().add(status);
        });
        return res;
    }

    public static interface ActionCallback {

        public void actionExecuted(StatusEdit action, ActionResult result);
    }

    public AddTaskResult addTask(
            long transaction,
            String taskType,
            String userId,
            String parameter,
            int maxattempts,
            long deadline,
            String slot) throws LogNotAvailableException {
        Long taskId = brokerStatus.nextTaskId();
        if (transaction > 0) {
            StatusEdit addTask = StatusEdit.PREPARE_ADD_TASK(transaction, taskId, taskType, parameter, userId, maxattempts, deadline, slot);
            BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(addTask);
            return new AddTaskResult((Long) result.data, result.error);
        } else {
            StatusEdit addTask = StatusEdit.ADD_TASK(taskId, taskType, parameter, userId, maxattempts, deadline, slot);
            BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(addTask);
            taskId = (Long) result.data;
            if (taskId > 0 && result.error == null) {
                this.tasksHeap.insertTask(taskId, taskType, userId);
            }
            return new AddTaskResult((Long) result.data, result.error);
        }
    }

    public void taskNeedsRecoveryDueToWorkerDeath(long taskId, String workerId) throws LogNotAvailableException {
        taskFinished(workerId, taskId, Task.STATUS_ERROR, "worker " + workerId + " died");
    }

    public void taskFinished(String workerId, long taskId, int finalstatus, String result) throws LogNotAvailableException {
        Task task = this.brokerStatus.getTask(taskId);
        if (task == null) {
            LOGGER.log(Level.SEVERE, "taskFinished {0}, task does not exist", taskId);
            return;
        }
        switch (finalstatus) {
            case Task.STATUS_FINISHED: {
                StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, finalstatus, result);
                this.brokerStatus.applyModification(edit);
                return;
            }
            case Task.STATUS_ERROR: {
                int maxAttepts = task.getMaxattempts();
                int attempt = task.getAttempts();
                if (maxAttepts > 0 && attempt >= maxAttepts) {
                    // too many attempts
                    LOGGER.log(Level.SEVERE, "taskFinished {0}, too many attempts {1}/{2}", new Object[]{taskId, attempt, maxAttepts});
                    StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_ERROR, result);
                    this.brokerStatus.applyModification(edit);
                    return;
                }
                long deadline = task.getExecutionDeadline();
                if (deadline > 0 && deadline < System.currentTimeMillis()) {
                    // deadline expired
                    LOGGER.log(Level.SEVERE, "taskFinished {0}, deadline expired {1}", new Object[]{taskId, new java.util.Date(deadline)});
                    StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_ERROR, result);
                    this.brokerStatus.applyModification(edit);
                    return;
                }

                // submit for new execution
                LOGGER.log(Level.FINER, "taskFinished {0}, attempts {1}/{2}, scheduling for retry", new Object[]{taskId, attempt, maxAttepts});
                StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_WAITING, result);
                this.brokerStatus.applyModification(edit);
                this.tasksHeap.insertTask(taskId, task.getType(), task.getUserId());
                return;
            }
            case Task.STATUS_WAITING:
            case Task.STATUS_RUNNING:
                // impossible
                throw new IllegalStateException("bad finalstatus:" + finalstatus);

        }

    }

    public void workerConnected(String workerId, String processId, String nodeLocation, Set<Long> actualRunningTasks, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_CONNECTED(workerId, processId, nodeLocation, actualRunningTasks, timestamp);
        this.brokerStatus.applyModification(edit);
    }

    public void declareWorkerDisconnected(String workerId, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_DISCONNECTED(workerId, timestamp);
        this.brokerStatus.applyModification(edit);
    }

    public void declareWorkerDead(String workerId, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_DIED(workerId, timestamp);
        this.brokerStatus.applyModification(edit);
    }

}

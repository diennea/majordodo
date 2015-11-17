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

import majordodo.clientfacade.ClientFacade;
import majordodo.network.jvm.JVMBrokerSupportInterface;
import majordodo.network.jvm.JVMBrokersRegistry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.AuthenticationManager;
import majordodo.clientfacade.BrokerStatusView;
import majordodo.clientfacade.HeapStatusView;
import majordodo.clientfacade.HeapStatusView.TaskStatus;
import majordodo.clientfacade.SlotsStatusView;
import majordodo.clientfacade.TransactionsStatusView;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Global status of the broker
 *
 * @author enrico.olivelli
 */
public class Broker implements AutoCloseable, JVMBrokerSupportInterface, BrokerFailureListener {

    private static final Logger LOGGER = Logger.getLogger(Broker.class.getName());
    private String brokerId = UUID.randomUUID().toString();
    private Callable<Void> externalProcessChecker; // PIDFILECHECKER
    private Runnable brokerDiedCallback;
    private AuthenticationManager authenticationManager;

    public AuthenticationManager getAuthenticationManager() {
        return authenticationManager;
    }

    public void setAuthenticationManager(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    public Callable<Void> getExternalProcessChecker() {
        return externalProcessChecker;
    }

    public void setExternalProcessChecker(Callable<Void> externalProcessChecker) {
        this.externalProcessChecker = externalProcessChecker;
    }

    public Runnable getBrokerDiedCallback() {
        return brokerDiedCallback;
    }

    public void setBrokerDiedCallback(Runnable brokerDiedCallback) {
        if (brokerDiedCallback != null) {
            this.brokerDiedCallback = () -> {
                try {
                    brokerDiedCallback.run();
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, "BrokerDiedCallback error", t);
                }
            };
        }
    }

    public String getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(String brokerId) {
        this.brokerId = brokerId;
    }

    public static String VERSION() {
        return "0.1.19-BETA5";
    }

    private final Workers workers;
    public final TasksHeap tasksHeap;
    private final BrokerStatus brokerStatus;
    private final StatusChangesLog log;
    private final BrokerServerEndpoint acceptor;
    private final ClientFacade client;
    private volatile boolean started;
    private volatile boolean stopped;
    private final CountDownLatch stopperLatch = new CountDownLatch(1);

    private final BrokerConfiguration configuration;
    private final CheckpointScheduler checkpointScheduler;
    private final GroupMapperScheduler groupMapperScheduler;
    private final FinishedTaskCollectorScheduler finishedTaskCollectorScheduler;
    private final BrokerStatusMonitor brokerStatusMonitor;
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
        this.authenticationManager = new SingleUserAuthenticationManager("admin", "password");
        this.client = new ClientFacade(this);
        this.brokerStatus = new BrokerStatus(log);
        this.tasksHeap = tasksHeap;
        this.log = log;
        this.log.setFailureListener(this);
        this.checkpointScheduler = new CheckpointScheduler(configuration, this);
        this.groupMapperScheduler = new GroupMapperScheduler(configuration, this);
        this.finishedTaskCollectorScheduler = new FinishedTaskCollectorScheduler(configuration, this);
        this.brokerStatusMonitor = new BrokerStatusMonitor(configuration, this);
        this.brokerLifeThread = new Thread(brokerLife, "broker-life");
        this.brokerLifeThread.setDaemon(true);
        this.log.setSharedSecret(configuration.getSharedSecret());
    }

    private boolean recoveryInProgress = false;

    public void start() {
        LOGGER.log(Level.SEVERE, "Booting Majordodo Broker, version {0}", VERSION());
        JVMBrokersRegistry.registerBroker(brokerId, this);
        if (configuration.isClearStatusAtBoot()) {
            try {
                this.log.clear();
            } catch (LogNotAvailableException error) {
                LOGGER.log(Level.SEVERE, "Could not clear status at boot", error);
                throw new RuntimeException(error);
            }
        }
        try {
            recoveryInProgress = true;
            this.brokerStatus.recover();
        } finally {
            recoveryInProgress = false;
        }
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
    public static boolean PERFORM_CHECKPOINT_AT_LEADERSHIP = true;

    private final Runnable brokerLife = new Runnable() {

        @Override
        public void run() {
            try {
                brokerStatusMonitor.start();
                LOGGER.log(Level.SEVERE, "Waiting to become leader...");
                brokerStatus.followTheLeader();
                if (stopped || failed) {
                    return;
                }
                LOGGER.log(Level.SEVERE, "Starting as leader");
                brokerStatus.recoverForLeadership();
                brokerStatus.startWriting();
                for (Task task : brokerStatus.getTasksAtBoot()) {
                    switch (task.getStatus()) {
                        case Task.STATUS_WAITING:
                            LOGGER.log(Level.SEVERE, "Task " + task.getTaskId() + ", " + task.getType() + ", user=" + task.getUserId() + " is to be scheduled");
                            tasksHeap.insertTask(task.getTaskId(), task.getType(), task.getUserId());
                            break;
                    }
                }
                Map<String, Collection<Long>> deadWorkerTasks = new HashMap<>();
                List<String> workersConnectedAtBoot = new ArrayList<>();
                workers.start(brokerStatus, deadWorkerTasks, workersConnectedAtBoot);
                started = true;
                for (Map.Entry<String, Collection<Long>> workerTasksToRecovery : deadWorkerTasks.entrySet()) {
                    tasksNeedsRecoveryDueToWorkerDeath(workerTasksToRecovery.getValue(), workerTasksToRecovery.getKey());
                }
                if (PERFORM_CHECKPOINT_AT_LEADERSHIP) {
                    checkpoint();
                }
                finishedTaskCollectorScheduler.start();
                try {
                    while (!stopped && !failed) {
                        noop(); // write something to log, this simple action detects fencing and forces flushes to other follower brokers
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
        try {
            checkpoint();
        } catch (LogNotAvailableException cannotCheckpoint) {
            LOGGER.log(Level.SEVERE, "checkpoint on shutdown failed", cannotCheckpoint);
        }
        stopperLatch.countDown();
        stopped = true;
        JVMBrokersRegistry.unregisterBroker(brokerId);
        this.brokerStatusMonitor.stop();
        this.finishedTaskCollectorScheduler.stop();
        this.checkpointScheduler.stop();
        this.groupMapperScheduler.stop();
        this.workers.stop();
        this.brokerStatus.close();

        if (brokerDiedCallback != null) {
            brokerDiedCallback.run();
        }
    }

    public boolean isStopped() {
        return stopped;
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

    public boolean isWritable() {
        return log.isWritable();
    }

    public List<Long> assignTasksToWorker(int max, Map<String, Integer> availableSpace, List<Integer> groups, Set<Integer> excludedGroups, String workerId) throws LogNotAvailableException {
        if (!started) {
            return Collections.emptyList();
        }
        long start = System.currentTimeMillis();
        List<Long> tasks = tasksHeap.takeTasks(max, groups, excludedGroups, availableSpace);
        long now = System.currentTimeMillis();
        List<StatusEdit> edits = new ArrayList<>();
        for (long taskId : tasks) {
            Task task = this.brokerStatus.getTask(taskId);
            if (task != null) {
                StatusEdit edit = StatusEdit.ASSIGN_TASK_TO_WORKER(taskId, workerId, task.getAttempts() + 1);
                edits.add(edit);
            }
        }
        this.brokerStatus.applyModifications(edits);

        long end = System.currentTimeMillis();
        LOGGER.log(Level.FINER, "assignTaskToWorker count {3} take: {0}, assign:{1}, total:{2}", new Object[]{now - start, end - now, end - start, (tasks.size())});
        return tasks;
    }

    public void checkpoint() throws LogNotAvailableException {
        this.brokerStatus.checkpoint(configuration.getTransactionsTtl());
    }

    void purgeTasks() {
        if (!started) {
            return;
        }
        Set<Long> expired = this.brokerStatus.purgeFinishedTasksAndSignalExpiredTasks(configuration.getFinishedTasksRetention(), configuration.getMaxExpiredTasksPerCycle());
        if (expired.isEmpty()) {
            return;
        }
        List<StatusEdit> expirededits = new ArrayList<>();
        expired.stream().forEach((taskId) -> {
            StatusEdit change = StatusEdit.TASK_STATUS_CHANGE(taskId, null, Task.STATUS_ERROR, "deadline_expired");
            expirededits.add(change);

        });
        try {
            this.tasksHeap.removeExpiredTasks(expired);
            this.brokerStatus.applyModifications(expirededits);
        } catch (LogNotAvailableException logNotAvailableException) {
            LOGGER.log(Level.SEVERE, "error while expiring tasks " + expired, logNotAvailableException);
        }
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
        assertBrokerAvailableForClients();
        long transactionId = brokerStatus.nextTransactionId();
        StatusEdit edit = StatusEdit.BEGIN_TRANSACTION(transactionId, System.currentTimeMillis());
        BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(edit);
        if (result.error != null) {
            throw new IllegalActionException(result.error);
        }
        return transactionId;
    }

    public void commitTransaction(long id) throws LogNotAvailableException, IllegalActionException {
        assertBrokerAvailableForClients();
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

    private void assertBrokerAvailableForClients() throws LogNotAvailableException {
        if (recoveryInProgress) {
            throw new LogNotAvailableException(new Exception("recovery_in_progress"));
        }
        if (!started) {
            throw new LogNotAvailableException(new Exception("broker_not_leader"));
        }
    }

    public void rollbackTransaction(long id) throws LogNotAvailableException, IllegalActionException {
        assertBrokerAvailableForClients();
        StatusEdit edit = StatusEdit.ROLLBACK_TRANSACTION(id);
        BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(edit);
        if (result.error != null) {
            throw new IllegalActionException(result.error);
        }
    }

    public BrokerStatusView createBrokerStatusView() {
        BrokerStatusView res = new BrokerStatusView();
        if (recoveryInProgress) {
            res.setClusterMode("RECOVERY");
        } else if (log.isClosed()) {
            res.setClusterMode("CLOSED");
        } else if (log.isLeader()) {
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

    public TransactionsStatusView getTransactionsStatusView() {
        TransactionsStatusView res = new TransactionsStatusView();
        res.setTransactions(brokerStatus.getAllTransactions());
        return res;
    }

    public SlotsStatusView getSlotsStatusView() {
        SlotsStatusView res = new SlotsStatusView();
        res.setBusySlots(brokerStatus.getBusySlots());
        return res;
    }

    public static interface ActionCallback {

        public void actionExecuted(StatusEdit action, ActionResult result);
    }

    public AddTaskResult addTask(AddTaskRequest request) throws LogNotAvailableException {
        assertBrokerAvailableForClients();
        Long taskId = brokerStatus.nextTaskId();
        if (request.transaction > 0) {
            StatusEdit addTask = StatusEdit.PREPARE_ADD_TASK(request.transaction, taskId, request.taskType, request.data, request.userId, request.maxattempts, request.deadline, request.slot, request.attempt);
            BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(addTask);
            return new AddTaskResult((Long) result.data, result.error);
        } else {
            StatusEdit addTask = StatusEdit.ADD_TASK(taskId, request.taskType, request.data, request.userId, request.maxattempts, request.deadline, request.slot, request.attempt);
            BrokerStatus.ModificationResult result = this.brokerStatus.applyModification(addTask);
            taskId = (Long) result.data;
            if (taskId > 0 && result.error == null) {
                this.tasksHeap.insertTask(taskId, request.taskType, request.userId);
            }
            return new AddTaskResult((Long) result.data, result.error);
        }
    }

    public List<AddTaskResult> addTasks(List<AddTaskRequest> requests) throws LogNotAvailableException {
        assertBrokerAvailableForClients();
        int size = requests.size();
        List<AddTaskResult> res = new ArrayList<>(size);
        List<StatusEdit> edits = new ArrayList<>(size);
        for (AddTaskRequest request : requests) {
            Long taskId = brokerStatus.nextTaskId();
            if (request.transaction > 0) {
                StatusEdit addTask = StatusEdit.PREPARE_ADD_TASK(request.transaction, taskId, request.taskType, request.data, request.userId, request.maxattempts, request.deadline, request.slot, request.attempt);
                edits.add(addTask);
            } else {
                StatusEdit addTask = StatusEdit.ADD_TASK(taskId, request.taskType, request.data, request.userId, request.maxattempts, request.deadline, request.slot, request.attempt);
                edits.add(addTask);
            }
        }
        List<BrokerStatus.ModificationResult> batch = this.brokerStatus.applyModifications(edits);
        for (int i = 0; i < size; i++) {
            StatusEdit addTask = edits.get(i);
            BrokerStatus.ModificationResult result = batch.get(i);
            if (addTask.editType == StatusEdit.TYPE_PREPARE_ADD_TASK) {
                res.add(new AddTaskResult((Long) result.data, result.error));
            } else {
                Long taskId = (Long) result.data;
                if (taskId != null && taskId > 0 && result.error == null) {
                    this.tasksHeap.insertTask(taskId, addTask.taskType, addTask.userid);
                }
                res.add(new AddTaskResult((Long) result.data, result.error));
            }
        }
        return res;
    }

    public void tasksNeedsRecoveryDueToWorkerDeath(Collection<Long> tasksId, String workerId) throws LogNotAvailableException {
        if (tasksId.isEmpty()) {
            return;
        }
        List<TaskFinishedData> data = new ArrayList<>();
        tasksId.forEach(
                taskId -> {
                    Task task = brokerStatus.getTask(taskId);
                    if (task != null && task.getStatus() == Task.STATUS_RUNNING) {
                        data.add(new TaskFinishedData(taskId, "worker " + workerId + " died", Task.STATUS_ERROR));
                    } else if (task != null) {
                        LOGGER.log(Level.SEVERE, "task {0} is in {1} status. no real need to recovery", new Object[]{task, Task.statusToString(task.getStatus())});
                    } else {
                        LOGGER.log(Level.SEVERE, "task {0} no more exists, no real need to recovery", new Object[]{taskId});
                    }
                }
        );
        tasksFinished(workerId, data);
    }

    public void tasksFinished(String workerId, List<TaskFinishedData> tasks) throws LogNotAvailableException {
        assertBrokerAvailableForClients();
        LOGGER.log(Level.FINE, "tasksFinished worker {0}, num: {1}", new Object[]{workerId, tasks.size()});
        List<StatusEdit> edits = new ArrayList<>();
        List<Task> toSchedule = new ArrayList<>();
        for (TaskFinishedData taskData : tasks) {
            long taskId = taskData.taskid;
            int finalstatus = taskData.finalStatus;
            String result = taskData.result;
            Task task = this.brokerStatus.getTask(taskId);
            if (task == null) {
                LOGGER.log(Level.SEVERE, "taskFinished {0}, task does not exist", taskId);
                continue;
            }
            workers.getWorkerManager(workerId).taskFinished(taskId);
            if (task.getStatus() != Task.STATUS_RUNNING) {
                LOGGER.log(Level.SEVERE, "taskFinished {0}, task already in status {1}", new Object[]{taskId, Task.statusToString(task.getStatus())});
                continue;
            }
            switch (finalstatus) {
                case Task.STATUS_FINISHED: {
                    StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, finalstatus, result);
                    edits.add(edit);
                    break;
                }
                case Task.STATUS_ERROR: {
                    int maxAttepts = task.getMaxattempts();
                    int attempt = task.getAttempts();
                    long deadline = task.getExecutionDeadline();
                    if (maxAttepts > 0 && attempt >= maxAttepts) {
                        // too many attempts
                        LOGGER.log(Level.SEVERE, "taskFinished {0} {4}, too many attempts {1}/{2} ({3})", new Object[]{taskId, attempt, maxAttepts, task.getResult() + "", Task.statusToString(task.getStatus())});
                        StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_ERROR, result);
                        edits.add(edit);

                    } else if (deadline > 0 && deadline < System.currentTimeMillis()) {
                        // deadline expired
                        LOGGER.log(Level.SEVERE, "taskFinished {0}, deadline expired {1} ({2})", new Object[]{taskId, new java.sql.Timestamp(deadline), task.getResult() + ""});
                        StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_ERROR, result);
                        edits.add(edit);
                    } else {
                        // submit for new execution
                        LOGGER.log(Level.SEVERE, "taskFinished {0}  {4}, attempts {1}/{2}, scheduling for retry ({3})", new Object[]{taskId, attempt, maxAttepts, task.getResult() + "", Task.statusToString(task.getStatus())});
                        StatusEdit edit = StatusEdit.TASK_STATUS_CHANGE(taskId, workerId, Task.STATUS_WAITING, result);
                        edits.add(edit);
                        toSchedule.add(task);
                    }
                    break;
                }
                case Task.STATUS_WAITING:
                case Task.STATUS_RUNNING:
                    // impossible
                    throw new IllegalStateException("bad finalstatus:" + finalstatus);
            }
        }
        brokerStatus.applyModifications(edits);
        for (Task task : toSchedule) {
            LOGGER.log(Level.SEVERE, "Schedule task for recovery {0} {1} {2} ({3})", new Object[]{task.getTaskId(), task.getType(), task.getUserId(), task.getResult() + ""});
            this.tasksHeap.insertTask(task.getTaskId(), task.getType(), task.getUserId());
        }

    }

    public void workerConnected(String workerId, String processId, String nodeLocation, Set<Long> actualRunningTasks, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_CONNECTED(workerId, processId, nodeLocation, actualRunningTasks, timestamp);
        this.brokerStatus.applyModification(edit);

        List<Long> tasksActuallyAssigned = brokerStatus.getRunningTasksAssignedToWorker(workerId);
        LOGGER.log(Level.SEVERE, "tasks assigned to worker {0}, actuallyRunning {1} ", new Object[]{tasksActuallyAssigned, edit.actualRunningTasks});
        tasksActuallyAssigned.removeAll(edit.actualRunningTasks);
        tasksNeedsRecoveryDueToWorkerDeath(tasksActuallyAssigned, workerId);
    }

    public void declareWorkerDisconnected(String workerId, long timestamp) throws LogNotAvailableException {
        if (stopped || failed) {
            // cannot write this on log, because we are stopped or failed, log will surely be not available
            return;
        }
        StatusEdit edit = StatusEdit.WORKER_DISCONNECTED(workerId, timestamp);
        this.brokerStatus.applyModification(edit);
    }

    public void declareWorkerDead(String workerId, long timestamp) throws LogNotAvailableException {
        StatusEdit edit = StatusEdit.WORKER_DIED(workerId, timestamp);
        this.brokerStatus.applyModification(edit);
    }

    private volatile boolean failed;

    @Override
    public void brokerFailed() {
        LOGGER.log(Level.SEVERE, "brokerFailed!");
        failed = true;
        if (brokerStatus != null) {
            brokerStatus.brokerFailed();
        }
        if (brokerDiedCallback != null) {
            brokerDiedCallback.run();
        }
    }

}

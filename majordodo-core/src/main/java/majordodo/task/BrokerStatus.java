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

import majordodo.clientfacade.TaskStatusView;
import majordodo.clientfacade.WorkerStatusView;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import majordodo.clientfacade.CodePoolView;
import majordodo.clientfacade.TransactionStatus;
import majordodo.codepools.CodePool;
import majordodo.utils.IntCounter;

/**
 * Replicated status of the broker. Each broker, leader or follower, contains a copy of this status. The status is
 * replicated to follower by reading the StatusChangesLog
 *
 * @author enrico.olivelli
 */
public final class BrokerStatus {
    
    private static final Logger LOGGER = Logger.getLogger(BrokerStatus.class.getName());

    private final Map<Long, Task> tasks = new HashMap<>();
    private final Map<Long, Transaction> transactions = new HashMap<>();

    private final Map<String, WorkerStatus> workers = new HashMap<>();
    private final Map<String, CodePool> codePools = new HashMap<>();
    private final AtomicLong newTaskId = new AtomicLong();
    private final AtomicLong newTransactionId = new AtomicLong();
    private long maxTaskId = -1;
    private long maxTransactionId = -1;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final StatusChangesLog log;
    private LogSequenceNumber lastLogSequenceNumber;
    private final AtomicInteger checkpointsCount = new AtomicInteger();
    private final SlotsManager slotsManager = new SlotsManager();
    private final BrokerStatusStats stats = new BrokerStatusStats();

    public WorkerStatus getWorkerStatus(String workerId) {
        return workers.get(workerId);
    }

    public BrokerStatus(StatusChangesLog log) {
        this.log = log;
    }

    public Map<String, Long> getActualSlots() {
        return slotsManager.getActualSlots();
    }

    public List<TransactionStatus> getAllTransactions() {
        List<TransactionStatus> result = new ArrayList<>();
        lock.readLock().lock();
        try {
            transactions.values().stream().forEach((k) -> {
                result.add(createTransactionStatusView(k));
            });
        } finally {
            lock.readLock().unlock();
        }
        return result;
    }

    public List<WorkerStatusView> getAllWorkers() {
        List<WorkerStatusView> result = new ArrayList<>();
        lock.readLock().lock();
        try {
            workers.values().stream().forEach((k) -> {
                result.add(createWorkerStatusView(k));
            });
        } finally {
            lock.readLock().unlock();
        }
        return result;
    }

    public List<TaskStatusView> getAllTasks() {
        List<TaskStatusView> result = new ArrayList<>();
        lock.readLock().lock();
        try {
            tasks.values().stream().forEach((k) -> {
                result.add(createTaskStatusView(k));
            });
        } finally {
            lock.readLock().unlock();
        }
        return result;
    }

    private TaskStatusView createTaskStatusView(Task task) {
        if (task == null) {
            return null;
        }
        TaskStatusView s = new TaskStatusView();
        s.setCreatedTimestamp(task.getCreatedTimestamp());
        s.setUser(task.getUserId());
        s.setWorkerId(task.getWorkerId());
        s.setStatus(task.getStatus());
        s.setTaskId(task.getTaskId());
        s.setData(task.getParameter());
        s.setType(task.getType());
        s.setResult(task.getResult());
        s.setAttempts(task.getAttempts());
        s.setMaxattempts(task.getMaxattempts());
        s.setSlot(task.getSlot());
        s.setRequestedStartTime(task.getRequestedStartTime());
        s.setExecutionDeadline(task.getExecutionDeadline());
        if (task.getMode() != null && !Task.MODE_EXECUTE_FACTORY.equals(task.getMode())) {
            s.setMode(task.getMode());
        }
        if (task.getCodepool() != null) {
            s.setCodePoolId(task.getCodepool());
        }
        s.setResources(task.getResources());
        return s;
    }

    private WorkerStatusView createWorkerStatusView(WorkerStatus k) {
        WorkerStatusView res = new WorkerStatusView();
        res.setId(k.getWorkerId());
        res.setLocation(k.getWorkerLocation());
        res.setLastConnectionTs(k.getLastConnectionTs());
        String s;
        switch (k.getStatus()) {
            case WorkerStatus.STATUS_CONNECTED:
                s = "CONNECTED";
                break;
            case WorkerStatus.STATUS_DEAD:
                s = "DEAD";
                break;
            case WorkerStatus.STATUS_DISCONNECTED:
                s = "DISCONNECTED";
                break;
            default:
                s = "?" + k.getStatus();
        }
        res.setProcessId(k.getProcessId());
        res.setStatus(s);
        return res;
    }

    public Collection<WorkerStatus> getWorkersAtBoot() {
        return workers.values();
    }

    public Collection<Task> getTasksAtBoot() {
        return tasks.values();
    }

    public Collection<Transaction> getTransactionsAtBoot() {
        return transactions.values();
    }

    public long nextTaskId() {
        return newTaskId.incrementAndGet();
    }

    public long nextTransactionId() {
        return newTransactionId.incrementAndGet();
    }

    public int getCheckpointsCount() {
        return checkpointsCount.get();
    }

    private void purgeAbandonedCodePools() throws LogNotAvailableException {
        List<String> toPurge = new ArrayList<>();
        lock.readLock().lock();
        try {
            for (CodePool codePool : codePools.values()) {
                if (codePool.getTtl() > 0) {
                    long delta = System.currentTimeMillis() - codePool.getCreationTimestamp();
                    if (delta > codePool.getTtl()) {
                        String codePoolId = codePool.getId();
                        boolean hasTasks = false;
                        for (Task t : tasks.values()) {
                            if (codePoolId.equals(t.getCodepool())) {
                                switch (t.getStatus()) {
                                    case Task.STATUS_RUNNING:
                                    case Task.STATUS_WAITING:
                                    case Task.STATUS_DELAYED:
                                        hasTasks = true;
                                        break;
                                    default:
                                        // not interesting
                                        break;
                                }
                                if (hasTasks) {
                                    break;
                                }
                            }
                        }
                        if (!hasTasks) {
                            toPurge.add(codePoolId);
                        }
                    }

                }
            }
        } finally {
            lock.readLock().unlock();
        }
        for (String codePoolId : toPurge) {
            StatusEdit rollback = StatusEdit.DELETE_CODEPOOL(codePoolId);
            LOGGER.log(Level.SEVERE, "Detected abandoned codepool {0}, dropping", new Object[]{codePoolId});
            applyModification(rollback);
        }
    }

    private void purgeAbandonedTransactions(long ttl) throws LogNotAvailableException {
        List<Long> staleTransactions = new ArrayList<>();
        lock.readLock().lock();
        try {
            long deadLine = System.currentTimeMillis() - ttl;
            transactions.values().stream().filter((t) -> (t.getCreationTimestamp() < deadLine)).forEach((t) -> {
                staleTransactions.add(t.getTransactionId());
            });
        } finally {
            lock.readLock().unlock();
        }
        for (long transactionId : staleTransactions) {
            StatusEdit rollback = StatusEdit.ROLLBACK_TRANSACTION(transactionId);
            LOGGER.log(Level.SEVERE, "Detected abandoned transaction {0}, Forcing rollback", new Object[]{transactionId});
            applyModification(rollback);
        }
    }

    public void checkpoint(long transactionsTtl) throws LogNotAvailableException {
        checkpointsCount.incrementAndGet();
        LOGGER.info("checkpoint");

        if (transactionsTtl > 0) {
            purgeAbandonedTransactions(transactionsTtl);
        }
        purgeAbandonedCodePools();

        BrokerStatusSnapshot snapshot = createSnapshot();

        this.log.checkpoint(snapshot);
    }

    public BrokerStatusSnapshot createSnapshot() {
        lock.readLock().lock();
        try {
            BrokerStatusSnapshot snap = new BrokerStatusSnapshot(maxTaskId, maxTransactionId, lastLogSequenceNumber);
            for (Task task : tasks.values()) {
                snap.tasks.add(task.cloneForSnapshot());
            }
            for (WorkerStatus status : workers.values()) {
                snap.workers.add(status.cloneForSnapshot());
            }
            for (Transaction status : transactions.values()) {
                snap.transactions.add(status.cloneForSnapshot());
            }
            return snap;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() {
        try {
            this.log.close();
        } catch (LogNotAvailableException sorry) {
            LOGGER.log(Level.SEVERE, "Error while closing transaction log", sorry);
        }

    }

    public Set<Long> purgeFinishedTasksAndSignalExpiredTasks(int finishedTasksRetention, int maxExpiredPerCycle) {
        long now = System.currentTimeMillis();
        long finished_deadline = now - finishedTasksRetention;

        Set<Long> expired = new HashSet<>();
        int expiredcount = 0;
        this.lock.writeLock().lock();
        try {
            // when running in FOLLOWER MODE we cannot expire tasks, but we need to remove them from memory, see MAJ-58
            boolean allowExpire = this.log.isLeader() && this.log.isWritable();
            // tasks are only purged from memry, not from logs
            // in case of broker restart it may re-appear
            for (Iterator<Map.Entry<Long, Task>> it = tasks.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Long, Task> taskEntry = it.next();
                Task t = taskEntry.getValue();
                switch (t.getStatus()) {
                    case Task.STATUS_WAITING:
                    case Task.STATUS_DELAYED:
                        if (expiredcount < maxExpiredPerCycle && allowExpire) {
                            long taskdeadline = t.getExecutionDeadline();
                            if (taskdeadline > 0 && taskdeadline < now) {
                                expired.add(t.getTaskId());
                                expiredcount++;
                                LOGGER.log(Level.INFO, "task {0}, created at {1}, expired, deadline {2}", new Object[]{t.getTaskId(), new java.util.Date(t.getCreatedTimestamp()), new java.util.Date(taskdeadline)});
                            }
                        }
                        break;
                    case Task.STATUS_ERROR:
                    case Task.STATUS_FINISHED:
                        if (t.getCreatedTimestamp() < finished_deadline) {
                            LOGGER.log(Level.INFO, "purging finished task {0} slot {2}, created at {1}", new Object[]{t.getTaskId(), new java.util.Date(t.getCreatedTimestamp()), t.getSlot()});
                            it.remove();
                            stats.taskStatusChange(t.getStatus(), -1);
                        }
                        break;
                    default:
                        // not interesting
                        break;
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return expired;
    }

    public void followTheLeader() throws InterruptedException {
        try {
            log.requestLeadership();
            while (!log.isLeader() && !log.isClosed() && !brokerFailed) {
                log.followTheLeader(this.lastLogSequenceNumber,
                    (logSeqNumber, edit) -> {
                        LOGGER.log(Level.INFO, "following the leader {0} {1}", new Object[]{logSeqNumber, edit});
                        applyEdit(logSeqNumber, edit);
                    });
                Thread.sleep(1000);
            }
        } catch (LogNotAvailableException err) {
            throw new RuntimeException(err);
        }
    }

    List<Long> getRunningTasksAssignedToWorker(String workerId) {
        this.lock.readLock().lock();
        try {
            return tasks.values().stream().filter(t -> t.getStatus() == Task.STATUS_RUNNING && workerId.equals(t.getWorkerId())).map(Task::getTaskId).collect(Collectors.toList());
        } finally {
            this.lock.readLock().unlock();
        }
    }

    private TransactionStatus createTransactionStatusView(Transaction k) {
        int countTasks = 0;
        Set<String> taskTypes = Collections.emptySet();
        if (k.getPreparedTasks() != null) {
            countTasks = k.getPreparedTasks().size();
            taskTypes = k.getPreparedTasks().stream().map(Task::getType).collect(Collectors.toSet());
        }
        return new TransactionStatus(k.getTransactionId(), k.getCreationTimestamp(), countTasks, taskTypes);
    }

    private boolean brokerFailed;

    void brokerFailed() {
        LOGGER.severe("brokerFailed!");
        brokerFailed = true;
    }

    void recoverForLeadership() {
        try {
            // we have to be sure that we are in sych we the global status                        
            LOGGER.severe("recoverForLeadership, from " + this.lastLogSequenceNumber);
            AtomicInteger done = new AtomicInteger();
            log.recovery(this.lastLogSequenceNumber,
                (logSeqNumber, edit) -> {
                    applyEdit(logSeqNumber, edit);
                    done.incrementAndGet();
                    if (brokerFailed) {
                        throw new RuntimeException("broker failed");
                    }
                }, false);
            newTaskId.set(maxTaskId + 1);
            newTransactionId.set(maxTransactionId + 1);
            LOGGER.severe("recovered gap of " + done.get() + " entries before entering leadership mode. newTaskId=" + newTaskId + " newTransactionId=" + newTransactionId);
        } catch (LogNotAvailableException err) {
            throw new RuntimeException(err);
        }

        LOGGER.log(Level.SEVERE, "After recoverForLeadership maxTaskId=" + maxTaskId + ", maxTransactionId=" + maxTransactionId + ", lastLogSequenceNumber=" + lastLogSequenceNumber);
    }

    int applyRunningTasksFilterToAssignTasksRequest(String workerId, Map<String, Integer> availableSpace) {
        lock.readLock().lock();
        try {
            AtomicInteger running = new AtomicInteger();
            tasks.values().forEach(t -> {
                if (t.getStatus() != Task.STATUS_RUNNING || !workerId.equals(t.getWorkerId())) {
                    return;
                }
                running.incrementAndGet();
                String taskType = t.getType();
                Integer count = availableSpace.get(taskType);
                if (count != null) {
                    int newCount = count - 1;
                    if (newCount > 0) {
                        availableSpace.put(taskType, newCount);
                    } else {
                        availableSpace.remove(taskType);
                    }
                }
            });
            return running.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    void reloadBusySlotsAtBoot(Map<String, Long> busySlots) {
        slotsManager.loadBusySlots(busySlots);
    }

    public TransactionStatus getTransaction(long transactionId) {
        lock.readLock().lock();
        try {
            Transaction t = transactions.get(transactionId);
            if (t == null) {
                return null;
            }
            return createTransactionStatusView(t);
        } finally {
            lock.readLock().unlock();
        }
    }

    CodePoolView getCodePoolView(String codePoolId) {
        lock.readLock().lock();
        try {
            CodePool codePool = codePools.get(codePoolId);
            if (codePool == null) {
                return null;
            }
            return createCodePoolView(codePool);
        } finally {
            lock.readLock().unlock();
        }
    }

    private CodePoolView createCodePoolView(CodePool codePool) {
        CodePoolView v = new CodePoolView();
        v.setCodePoolId(codePool.getId());
        v.setCreationTimestamp(codePool.getCreationTimestamp());
        v.setTtl(codePool.getTtl());
        return v;
    }

    CodePool getCodePool(String codePoolId) {
        lock.readLock().lock();
        try {
            // codepools are immutable, no copy is needed
            return codePools.get(codePoolId);
        } finally {
            lock.readLock().unlock();
        }
    }

    Map<TaskTypeUser, IntCounter> collectMaxAvailableSpacePerUserOnWorker(String workerId,
        int maxThreadPerUserPerTaskTypePercent,
        Map<String, Integer> startingAvailableSpace) {
        Map<TaskTypeUser, IntCounter> res = new HashMap<>();
        lock.readLock().lock();
        try {
            this.tasks
                .values()
                .stream()
                .filter(t -> t.getStatus() == Task.STATUS_RUNNING
                && workerId.equals(t.getWorkerId()))
                .forEach(t -> {
                    String type = t.getType();
                    Integer startingMaxAvailableSpacePerUser = startingAvailableSpace.get(type);
                    if (startingMaxAvailableSpacePerUser == null) {
                        startingMaxAvailableSpacePerUser = startingAvailableSpace.get(Task.TASKTYPE_ANY);
                    }
                    if (startingMaxAvailableSpacePerUser != null && startingMaxAvailableSpacePerUser > 0) {
                        TaskTypeUser key = new TaskTypeUser(type, t.getUserId());
                        IntCounter count = res.get(key);
                        if (count == null) {
                            int effectiveBoundForUser
                                = (startingMaxAvailableSpacePerUser * maxThreadPerUserPerTaskTypePercent) / 100;
                            if (effectiveBoundForUser <= 0) {
                                effectiveBoundForUser = 1;
                            }
                            LOGGER.log(Level.FINEST, "collectMaxAvailableSpacePerUserOnWorker {0} -> for user {1} we are starting from {2} - bound is {3}", new Object[]{workerId, t.getUserId(), startingMaxAvailableSpacePerUser, effectiveBoundForUser});
                            count = new IntCounter(effectiveBoundForUser);
                            res.put(key, count);

                        }
                        LOGGER.log(Level.FINEST, "collectMaxAvailableSpacePerUserOnWorker {0} -> for user {1} found task {2} - id {3}", new Object[]{workerId, t.getUserId(), t.getType(), t.getTaskId()});
                        count.count--;
                    }
                });
        } finally {
            lock.readLock().unlock();
        }
        return res;

    }

    public static final class ModificationResult {

        public final LogSequenceNumber sequenceNumber;
        public final String error;
        public final Object data;

        public ModificationResult(LogSequenceNumber sequenceNumber, Object data, String error) {
            this.sequenceNumber = sequenceNumber;
            this.data = data;
            this.error = error;
        }

        @Override
        public String toString() {
            return "ModificationResult{" + "sequenceNumber=" + sequenceNumber + ", error=" + error + ", data=" + data + '}';
        }

    }

    List<ModificationResult> applyModifications(List<StatusEdit> edits) throws LogNotAvailableException {
        if (brokerFailed) {
            throw new LogNotAvailableException("broker failed");
        }
        List<ModificationResult> results = new ArrayList<>();
        Set<Integer> skip = new HashSet<>();
        int index = 0;
        List<StatusEdit> toLog = new ArrayList<>();
        for (StatusEdit edit : edits) {
            if ((edit.editType == StatusEdit.TYPE_ADD_TASK || edit.editType == StatusEdit.TYPE_PREPARE_ADD_TASK)
                && edit.slot != null) {
                if (!slotsManager.assignSlot(edit.slot, edit.taskId)) {
                    skip.add(index);
                } else {
                    toLog.add(edit);
                }
            } else {
                toLog.add(edit);
            }
            index++;
        }
        try {
            List<LogSequenceNumber> num = log.logStatusEditBatch(toLog);
            int max = edits.size();
            int numberSequence = 0;
            for (int i = 0; i < max; i++) {
                StatusEdit edit = edits.get(i);
                if (skip.contains(i)) {
                    results.add(new ModificationResult(null, null, "slot " + edit.slot + " already assigned"));
                } else {
                    boolean ok = true;
                    if (edit.editType == StatusEdit.TYPE_CREATECODEPOOL) {
                        if (edit.codepool == null || edit.codepool.isEmpty()) {
                            results.add(new ModificationResult(null, edit.codepool, "codepoolid must not be empty"));
                            ok = false;
                        } else if (codePools.containsKey(edit.codepool)) {
                            results.add(new ModificationResult(null, edit.codepool, "codepool " + edit.codepool + " already exists"));
                            ok = false;
                        }
                    }
                    if (ok) {
                        LogSequenceNumber n = num.get(numberSequence++);
                        results.add(applyEdit(n, edit));
                    }
                }
            }
            return results;
        } catch (LogNotAvailableException err) {
            for (StatusEdit edit : edits) {
                if ((edit.editType == StatusEdit.TYPE_ADD_TASK
                    || edit.editType == StatusEdit.TYPE_PREPARE_ADD_TASK)
                    && edit.slot != null) {
                    slotsManager.releaseSlot(edit.slot, edit.taskId);

                } else {
                    toLog.add(edit);
                }
                index++;
            }
            throw err;
        }
    }

    public ModificationResult applyModification(StatusEdit edit) throws LogNotAvailableException {
        if (brokerFailed) {
            throw new LogNotAvailableException("broker failed");
        }
        LOGGER.log(Level.FINEST, "applyModification {0}", edit);
        if ((edit.editType == StatusEdit.TYPE_ADD_TASK || edit.editType == StatusEdit.TYPE_PREPARE_ADD_TASK)
            && edit.slot != null) {
            if (slotsManager.assignSlot(edit.slot, edit.taskId)) {
                try {
                    LogSequenceNumber num = log.logStatusEdit(edit); // ? out of the lock ?        
                    return applyEdit(num, edit);
                } catch (LogNotAvailableException releaseSlot) {
                    slotsManager.releaseSlot(edit.slot, edit.taskId);
                    throw releaseSlot;
                }
            } else {
                // slot already assigned
                LOGGER.log(Level.FINEST, "slot {0} already assigned", edit.slot);
                return new ModificationResult(null, null, "slot " + edit.slot + " already assigned");
            }
        } else {
            if (edit.editType == StatusEdit.TYPE_CREATECODEPOOL) {
                if (edit.codepool == null || edit.codepool.isEmpty()) {
                    return new ModificationResult(null, edit.codepool, "codepoolid must not be empty");
                } else if (codePools.containsKey(edit.codepool)) {
                    return new ModificationResult(null, edit.codepool, "codepool " + edit.codepool + " already exists");
                }
            }
            LogSequenceNumber num = log.logStatusEdit(edit); // ? out of the lock ?        
            return applyEdit(num, edit);
        }
    }

    /**
     * Apply the modification to the status, this operation cannot fail, a failure MUST lead to the death of the JVM
     * because it will not be recoverable as the broker will go out of synch
     *
     * @param edit
     */
    private ModificationResult applyEdit(LogSequenceNumber num, StatusEdit edit) {
        LOGGER.log(Level.FINEST, "applyEdit {0}", edit);

        lock.writeLock().lock();
        try {
            lastLogSequenceNumber = num;
            switch (edit.editType) {
                case StatusEdit.TYPE_ASSIGN_TASK_TO_WORKER: {
                    long taskId = edit.taskId;
                    String workerId = edit.workerId;
                    String resources = edit.resources;
                    Task task = tasks.get(taskId);
                    if (task == null) {
                        throw new RuntimeException("task " + taskId + " not present in brokerstatus. maybe you are recovering broken snapshot");
                    }
                    int oldStatus = task.getStatus();
                    task.setStatus(Task.STATUS_RUNNING);
                    if (workerId == null || workerId.isEmpty()) {
                        throw new RuntimeException("bug " + edit);
                    }
                    task.setWorkerId(workerId.intern());
                    if (resources != null) {
                        task.setResources(resources.intern());
                    }
                    task.setAttempts(edit.attempt);
                    stats.taskStatusChange(oldStatus, task.getStatus());
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_TASK_STATUS_CHANGE: {
                    long taskId = edit.taskId;
                    Task task = tasks.get(taskId);
                    if (task == null) {
                        throw new IllegalStateException("task " + taskId + " does not exist");
                    }
                    int oldStatus = task.getStatus();
                    task.setStatus(edit.taskStatus);
                    task.setResult(edit.result);
                    if (task.getSlot() != null) {
                        switch (edit.taskStatus) {
                            case Task.STATUS_FINISHED:
                            case Task.STATUS_ERROR: {
                                slotsManager.releaseSlot(task.getSlot(), task.getTaskId());
                                break;
                            }
                            default:
                                // not interesting
                                break;
                        }
                    }

                    stats.taskStatusChange(oldStatus, edit.taskStatus);

                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_BEGIN_TRANSACTION: {
                    if (maxTransactionId < edit.transactionId) {
                        maxTransactionId = edit.transactionId;
                    }
                    Transaction transaction = new Transaction(edit.transactionId, edit.timestamp);
                    transactions.put(transaction.getTransactionId(), transaction);
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_COMMIT_TRANSACTION: {
                    Transaction transaction = transactions.get(edit.transactionId);
                    if (transaction == null) {
                        LOGGER.log(Level.SEVERE, "No transaction {0}", new Object[]{edit.transactionId});
                        return new ModificationResult(num, 0L, "no transaction " + edit.transactionId);
                    }
                    for (Task task : transaction.getPreparedTasks()) {
                        tasks.put(task.getTaskId(), task);
                        stats.taskStatusChange(-1, task.getStatus());
                    }
                    transactions.remove(edit.transactionId);
                    return new ModificationResult(num, transaction.getPreparedTasks(), null);
                }
                case StatusEdit.TYPE_ROLLBACK_TRANSACTION: {
                    Transaction transaction = transactions.get(edit.transactionId);
                    if (transaction == null) {
                        LOGGER.log(Level.SEVERE, "No transaction {0}", new Object[]{edit.transactionId});
                        return new ModificationResult(num, 0L, "no transaction " + edit.transactionId);
                    }
                    // release slots
                    for (Task task : transaction.getPreparedTasks()) {
                        if (task.getSlot() != null && !task.getSlot().isEmpty()) {
                            LOGGER.log(Level.SEVERE, "Rollback transaction {0}, released slot {1}", new Object[]{edit.transactionId, task.getSlot()});
                            slotsManager.releaseSlot(task.getSlot(), task.getTaskId());
                        }
                    }
                    // then discard the transaction
                    transactions.remove(edit.transactionId);
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_ADD_TASK: {
                    Task task = new Task();
                    task.setTaskId(edit.taskId);
                    if (maxTaskId < edit.taskId) {
                        maxTaskId = edit.taskId;
                    }
                    task.setCreatedTimestamp(System.currentTimeMillis());
                    task.setParameter(edit.parameter);
                    task.setType(edit.taskType.intern());
                    task.setUserId(edit.userid.intern());
                    if (edit.codepool != null) {
                        task.setCodepool(edit.codepool.intern());
                    }
                    if (edit.mode != null) {
                        task.setMode(edit.mode.intern());
                    }
                    task.setMaxattempts(edit.maxattempts);
                    task.setAttempts(edit.attempt);
                    task.setRequestedStartTime(edit.requestedStartTime);
                    task.setExecutionDeadline(edit.executionDeadline);
                    task.setSlot(edit.slot);
                    if (task.getRequestedStartTime() > 0 && task.getCreatedTimestamp() < task.getRequestedStartTime()) {
                        task.setStatus(Task.STATUS_DELAYED);
                    } else {
                        task.setStatus(Task.STATUS_WAITING);
                    }
                    tasks.put(edit.taskId, task);
                    stats.taskStatusChange(-1, task.getStatus());
                    
                    if (edit.slot != null) {
                        // we need this, for log-replay on recovery and on followers
                        slotsManager.assignSlot(edit.slot, edit.taskId);
                    }
                    return new ModificationResult(num, task, null);
                }
                case StatusEdit.TYPE_PREPARE_ADD_TASK: {
                    Transaction transaction = transactions.get(edit.transactionId);
                    if (transaction == null) {
                        LOGGER.log(Level.SEVERE, "No transaction {0}", new Object[]{edit.transactionId});
                        return new ModificationResult(num, null, "no transaction " + edit.transactionId);
                    }
                    Task task = new Task();
                    task.setTaskId(edit.taskId);
                    if (maxTaskId < edit.taskId) {
                        maxTaskId = edit.taskId;
                    }
                    task.setCreatedTimestamp(System.currentTimeMillis());
                    task.setParameter(edit.parameter);
                    task.setType(edit.taskType.intern());
                    task.setUserId(edit.userid.intern());
                    if (edit.codepool != null) {
                        task.setCodepool(edit.codepool.intern());
                    }
                    if (edit.mode != null) {
                        task.setMode(edit.mode.intern());
                    }
                    task.setMaxattempts(edit.maxattempts);
                    task.setAttempts(edit.attempt);
                    task.setRequestedStartTime(edit.requestedStartTime);
                    task.setExecutionDeadline(edit.executionDeadline);
                    task.setSlot(edit.slot);
                    if (task.getRequestedStartTime() > 0 && task.getCreatedTimestamp() < task.getRequestedStartTime()) {
                        task.setStatus(Task.STATUS_DELAYED);
                    } else {
                        task.setStatus(Task.STATUS_WAITING);
                    }
                    if (edit.slot != null) {
                        // we need this, for log-replay on recovery and on followers
                        slotsManager.assignSlot(edit.slot, edit.taskId);
                    }
                    // the slot it acquired on prepare (and eventually released on rollback)
                    // task is not really submitted not, it will be submitted on commit
                    transaction.getPreparedTasks().add(task);
                    
                    return new ModificationResult(num, task, null);
                }
                case StatusEdit.TYPE_WORKER_CONNECTED: {
                    WorkerStatus node = workers.get(edit.workerId);
                    if (node == null) {
                        node = new WorkerStatus();
                        node.setWorkerId(edit.workerId);
                        workers.put(edit.workerId, node);
                    }
                    node.setStatus(WorkerStatus.STATUS_CONNECTED);
                    node.setWorkerLocation(edit.workerLocation);
                    node.setProcessId(edit.workerProcessId);
                    node.setLastConnectionTs(edit.timestamp);
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_WORKER_DISCONNECTED: {
                    WorkerStatus node = workers.get(edit.workerId);
                    if (node == null) {
                        node = new WorkerStatus();
                        node.setWorkerId(edit.workerId);
                        workers.put(edit.workerId, node);
                    }
                    node.setStatus(WorkerStatus.STATUS_DISCONNECTED);
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_WORKER_DIED: {
                    WorkerStatus node = workers.get(edit.workerId);
                    if (node == null) {
                        node = new WorkerStatus();
                        node.setWorkerId(edit.workerId);
                        workers.put(edit.workerId, node);
                    }
                    node.setStatus(WorkerStatus.STATUS_DEAD);
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_NOOP: {
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_DELETECODEPOOL: {
                    codePools.remove(edit.codepool);
                    return new ModificationResult(num, null, null);
                }
                case StatusEdit.TYPE_CREATECODEPOOL: {
                    if (codePools.containsKey(edit.codepool)) {
                        throw new IllegalArgumentException("codepool " + edit.codepool + " already exists");
                    }
                    CodePool codePool = new CodePool(edit.codepool, edit.timestamp, edit.payload, edit.executionDeadline);
                    codePools.put(edit.codepool, codePool);
                    return new ModificationResult(num, null, null);
                }
                default:
                    throw new IllegalArgumentException();

            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    public BrokerStatusStats getStats() {
        return stats;
    }

    public void recover() {

        try {
            BrokerStatusSnapshot snapshot = log.loadBrokerStatusSnapshot();
            this.maxTaskId = snapshot.getMaxTaskId();
            this.newTaskId.set(maxTaskId + 1);
            this.maxTransactionId = snapshot.getMaxTransactionId();
            this.newTransactionId.set(maxTransactionId + 1);
            this.lastLogSequenceNumber = snapshot.getActualLogSequenceNumber();
            Map<String, Long> busySlots = new HashMap<>();
            for (Task task : snapshot.getTasks()) {
                long taskId = task.getTaskId();
                this.tasks.put(taskId, task);
                if (maxTaskId < taskId) {
                    maxTaskId = taskId;
                }
                stats.taskStatusChange(-1, task.getStatus());
                switch (task.getStatus()) {
                    case Task.STATUS_RUNNING:
                    case Task.STATUS_WAITING:
                    case Task.STATUS_DELAYED: {
                        if (task.getSlot() != null && !task.getSlot().isEmpty()) {
                            busySlots.put(task.getSlot(), taskId);
                        }
                        break;
                    }
                    default:
                        // not interesting
                        break;
                }

            }
            for (WorkerStatus worker : snapshot.getWorkers()) {
                this.workers.put(worker.getWorkerId(), worker);
            }
            for (Transaction tx : snapshot.getTransactions()) {
                long transactionId = tx.getTransactionId();
                if (maxTransactionId < transactionId) {
                    maxTransactionId = transactionId;
                }
                this.transactions.put(transactionId, tx);
            }
            for (CodePool codePool : snapshot.getCodePools()) {
                this.codePools.put(codePool.getId(), codePool);
            }
            this.slotsManager.loadBusySlots(busySlots);
            log.recovery(snapshot.getActualLogSequenceNumber(),
                (logSeqNumber, edit) -> {
                    applyEdit(logSeqNumber, edit);
                    if (brokerFailed) {
                        throw new RuntimeException("broker failed");
                    }
                }, false);
            newTaskId.set(maxTaskId + 1);
            newTransactionId.set(maxTransactionId + 1);
        } catch (LogNotAvailableException err) {
            LOGGER.log(Level.SEVERE, "error during recovery", err);
            throw new RuntimeException(err);
        }

        LOGGER.log(Level.SEVERE, "After recovery maxTaskId=" + maxTaskId + ", maxTransactionId=" + maxTransactionId + ", lastLogSequenceNumber=" + lastLogSequenceNumber);
    }

    public void startWriting() {
        lock.writeLock().lock();
        try {
            log.startWriting();
        } catch (LogNotAvailableException err) {
            throw new RuntimeException(err);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Task getTask(long taskId) {
        lock.readLock().lock();
        try {
            return tasks.get(taskId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public TaskStatusView getTaskStatus(long taskId) {
        Task task;
        lock.readLock().lock();
        try {
            task = tasks.get(taskId);
        } finally {
            lock.readLock().unlock();
        }

        TaskStatusView s = createTaskStatusView(task);
        return s;
    }

}

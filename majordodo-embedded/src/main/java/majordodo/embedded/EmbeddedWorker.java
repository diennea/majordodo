/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.embedded;

import majordodo.executors.TaskExecutorFactory;
import majordodo.network.BrokerLocator;
import majordodo.network.jvm.JVMBrokerLocator;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.replication.ZKBrokerLocator;
import majordodo.task.Broker;
import majordodo.task.FileCommitLog;
import majordodo.task.MemoryCommitLog;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Tools for embedded Majordod worker
 *
 * @author enrico.olivelli
 */
public class EmbeddedWorker {

    private WorkerCore workerCore;
    private final WorkerCoreConfiguration workerConfiguration = new WorkerCoreConfiguration();
    private TaskExecutorFactory taskExecutorFactory;
    private BrokerLocator brokerLocator;
    private final EmbeddedWorkerConfiguration configuration;

    public EmbeddedWorker(EmbeddedWorkerConfiguration configuration) {
        this.configuration = configuration;
    }

    public WorkerCore getWorkerCore() {
        return workerCore;
    }

    public WorkerCoreConfiguration getWorkerConfiguration() {
        return workerConfiguration;
    }

    public TaskExecutorFactory getTaskExecutorFactory() {
        return taskExecutorFactory;
    }

    public void setTaskExecutorFactory(TaskExecutorFactory taskExecutorFactory) {
        this.taskExecutorFactory = taskExecutorFactory;
    }

    public BrokerLocator getBrokerLocator() {
        return brokerLocator;
    }

    public void setBrokerLocator(BrokerLocator brokerLocator) {
        this.brokerLocator = brokerLocator;
    }

    public void start() throws Exception {
        String host = configuration.getStringProperty(EmbeddedWorkerConfiguration.KEY_HOST, "localhost");
        int port = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_PORT, 7862);
        String mode = configuration.getStringProperty(EmbeddedWorkerConfiguration.KEY_MODE, EmbeddedWorkerConfiguration.MODE_SINGLEBROKER);
        String zkAdress = configuration.getStringProperty(EmbeddedWorkerConfiguration.KEY_ZKADDRESS, "localhost:1281");
        String zkPath = configuration.getStringProperty(EmbeddedWorkerConfiguration.KEY_ZKPATH, "/majordodo");
        int zkSessionTimeout = configuration.getIntProperty(EmbeddedWorkerConfiguration.KEY_ZKSESSIONTIMEOUT, 40000);

        switch (mode) {
            case EmbeddedWorkerConfiguration.MODE_JVMONLY:
                brokerLocator = new JVMBrokerLocator("embedded");
                break;
            case EmbeddedWorkerConfiguration.MODE_SIGLESERVER:
                brokerLocator = new NettyBrokerLocator(host, port);
                break;
            case EmbeddedWorkerConfiguration.MODE_CLUSTERED:
                brokerLocator = new ZKBrokerLocator(zkAdress, zkSessionTimeout, zkPath);
                break;
        }
        String processId = ManagementFactory.getRuntimeMXBean().getName() + "_" + UUID.randomUUID().toString();
        workerCore = new WorkerCore(workerConfiguration, processId, brokerLocator, null);
        if (taskExecutorFactory != null) {
            workerCore.setExecutorFactory(taskExecutorFactory);
        }
        workerCore.start();
    }

    public void stop() {
        if (workerCore != null) {
            workerCore.stop();
        }
    }
}

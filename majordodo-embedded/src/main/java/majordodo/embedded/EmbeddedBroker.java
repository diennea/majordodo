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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import majordodo.task.FileCommitLog;
import majordodo.task.TaskPropertiesMapperFunction;
import majordodo.task.TaskProperties;
import majordodo.task.MemoryCommitLog;
import majordodo.task.StatusChangesLog;
import majordodo.task.TasksHeap;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import majordodo.clientfacade.AuthenticationManager;
import majordodo.network.BrokerHostData;

/**
 * Utility to embed a Majordodo Broker
 *
 * @author enrico.olivelli
 */
public class EmbeddedBroker implements AutoCloseable {

    private Broker broker;
    private BrokerConfiguration brokerConfiguration;

    private TaskPropertiesMapperFunction taskPropertiesMapperFunction = new TaskPropertiesMapperFunction() {
        @Override
        public TaskProperties getTaskProperties(long taskid, String taskType, String userid) {
            return new TaskProperties(1, null);
        }
    };
    private StatusChangesLog statusChangesLog;
    private NettyChannelAcceptor server;
    private final EmbeddedBrokerConfiguration configuration;
    private Runnable brokerDiedCallback;
    private AuthenticationManager authenticationManager;

    public AuthenticationManager getAuthenticationManager() {
        return authenticationManager;
    }

    public void setAuthenticationManager(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    /**
     * This callback wil be called when the broker dies, for example in case of
     * "leadership lost" or "broker commit log"
     *
     * @return
     */
    public Runnable getBrokerDiedCallback() {
        return brokerDiedCallback;
    }

    /**
     * This callback wil be called when the broker dies, for example in case of
     * "leadership lost" or "broker commit log"
     *
     * @param brokerDiedCallback
     */
    public void setBrokerDiedCallback(Runnable brokerDiedCallback) {
        this.brokerDiedCallback = brokerDiedCallback;
    }

    public EmbeddedBroker(EmbeddedBrokerConfiguration configuration) {
        this.configuration = configuration;
    }

    public EmbeddedBrokerConfiguration getConfiguration() {
        return configuration;
    }

    public TaskPropertiesMapperFunction getTaskPropertiesMapperFunction() {
        return taskPropertiesMapperFunction;
    }

    public void setTaskPropertiesMapperFunction(TaskPropertiesMapperFunction taskPropertiesMapperFunction) {
        this.taskPropertiesMapperFunction = taskPropertiesMapperFunction;
    }

    public Broker getBroker() {
        return broker;
    }

    public BrokerConfiguration getBrokerConfiguration() {
        return brokerConfiguration;
    }

    public void start() throws Exception {
        String id = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_BROKERID, "");
        if (id.isEmpty()) {
            id = UUID.randomUUID().toString();
        }
        String host = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_HOST, "localhost");
        int workerthreads = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BROKERWORKERTHREADS, 16);
        int port = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_PORT, 7862);
        boolean ssl = configuration.getBooleanProperty(EmbeddedBrokerConfiguration.KEY_SSL, false);
        File certfile = (File) configuration.getProperty(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_FILE, null);
        File certchainfile = (File) configuration.getProperty(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_CHAIN_FILE, null);
        String sslciphers = configuration.getProperty(EmbeddedBrokerConfiguration.SSL_CIPHERS, "").toString();
        String certpassword = configuration.getStringProperty(EmbeddedBrokerConfiguration.SSL_CERTIFICATE_PASSWORD, null);
        String mode = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_MODE, EmbeddedBrokerConfiguration.MODE_SIGLESERVER);
        String logDirectory = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_LOGSDIRECTORY, "txlog");
        String snapshotsDirectory = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_SNAPSHOTSDIRECTORY, "snapshots");
        String zkAdress = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_ZKADDRESS, "localhost:1281");
        String zkPath = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_ZKPATH, "/majordodo");
        String clientapiurl = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_CLIENTAPIURL, "");
        int zkSessionTimeout = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_ZKSESSIONTIMEOUT, 40000);
        long maxFileSize = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_LOGSMAXFILESIZE, 1024 * 1024);
        Map<String, String> additionalInfo = new HashMap<>();
        additionalInfo.put("client.api.url", clientapiurl);
        additionalInfo.put("broker.id", id);
        switch (mode) {
            case EmbeddedBrokerConfiguration.MODE_JVMONLY: {
                statusChangesLog = new MemoryCommitLog();
                break;
            }
            case EmbeddedBrokerConfiguration.MODE_SIGLESERVER: {
                Path _logDirectory = Paths.get(logDirectory);
                if (!Files.isDirectory(_logDirectory)) {
                    Files.createDirectory(_logDirectory);
                }
                Path _snapshotsDirectory = Paths.get(snapshotsDirectory);
                if (!Files.isDirectory(_snapshotsDirectory)) {
                    Files.createDirectory(_snapshotsDirectory);
                }
                statusChangesLog = new FileCommitLog(_logDirectory, _snapshotsDirectory, maxFileSize);
                break;
            }
            case EmbeddedBrokerConfiguration.MODE_CLUSTERED: {
                Path _snapshotsDirectory = Paths.get(snapshotsDirectory);
                if (!Files.isDirectory(_snapshotsDirectory)) {
                    Files.createDirectory(_snapshotsDirectory);
                }
                ReplicatedCommitLog _statusChangesLog = new ReplicatedCommitLog(zkAdress, zkSessionTimeout, zkPath, _snapshotsDirectory,
                        BrokerHostData.formatHostdata(new BrokerHostData(host, port, Broker.VERSION(), ssl, additionalInfo))
                );
                statusChangesLog = _statusChangesLog;
                int ensemble = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BK_ENSEMBLE_SIZE, _statusChangesLog.getEnsemble());
                int writeQuorumSize = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BK_WRITEQUORUMSIZE, _statusChangesLog.getWriteQuorumSize());
                int ackQuorumSize = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BK_ACKQUORUMSIZE, _statusChangesLog.getAckQuorumSize());
                long ledgersRetentionPeriod = configuration.getLongProperty(EmbeddedBrokerConfiguration.KEY_BK_LEDGERSRETENTIONPERIOD, _statusChangesLog.getLedgersRetentionPeriod());
                _statusChangesLog.setAckQuorumSize(ackQuorumSize);
                _statusChangesLog.setEnsemble(ensemble);
                _statusChangesLog.setLedgersRetentionPeriod(ledgersRetentionPeriod);
                _statusChangesLog.setWriteQuorumSize(writeQuorumSize);

                break;
            }
        }
        brokerConfiguration = new BrokerConfiguration();
        String sharedSecret = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_SHAREDSECRET, EmbeddedBrokerConfiguration.KEY_SHAREDSECRET_DEFAULT);
        brokerConfiguration.setSharedSecret(sharedSecret);
        brokerConfiguration.read(configuration.getProperties());
        broker = new Broker(brokerConfiguration, statusChangesLog, new TasksHeap(brokerConfiguration.getTasksHeapSize(), taskPropertiesMapperFunction));
        broker.setAuthenticationManager(authenticationManager);
        broker.setBrokerId(id);
        switch (mode) {
            case EmbeddedBrokerConfiguration.MODE_JVMONLY:
                break;
            case EmbeddedBrokerConfiguration.MODE_SIGLESERVER:
            case EmbeddedBrokerConfiguration.MODE_CLUSTERED:
                server = new NettyChannelAcceptor(broker.getAcceptor(), host, port);
                server.setSslCertChainFile(certchainfile);
                if (sslciphers != null && !sslciphers.isEmpty()) {
                    server.setSslCiphers(Stream.of(sslciphers.split(",")).map(s -> s.trim()).filter(s -> !s.isEmpty()).collect(Collectors.toList()));
                }
                server.setSslCertFile(certfile);
                server.setSslCertPassword(certpassword);
                server.setSsl(ssl);
                server.setWorkerThreads(workerthreads);
                break;
        }
        broker.setBrokerDiedCallback(brokerDiedCallback);
        broker.start();
        if (server != null) {
            server.start();
        }
    }

    public void stop() {
        if (server != null) {
            server.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    @Override
    public void close() {
        stop();
    }

}

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

import java.nio.file.Files;
import java.nio.file.Path;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.replication.ReplicatedCommitLog;
import majordodo.task.Broker;
import majordodo.task.BrokerConfiguration;
import majordodo.task.FileCommitLog;
import majordodo.task.GroupMapperFunction;
import majordodo.task.MemoryCommitLog;
import majordodo.task.StatusChangesLog;
import majordodo.task.TasksHeap;
import java.nio.file.Paths;

/**
 * Utility to embed a Majordodo Broker
 *
 * @author enrico.olivelli
 */
public class EmbeddedBroker {

    private Broker broker;
    private BrokerConfiguration brokerConfiguration;
    private GroupMapperFunction groupMapperFunction;
    private StatusChangesLog statusChangesLog;
    private NettyChannelAcceptor server;
    private final EmbeddedBrokerConfiguration configuration;

    public EmbeddedBroker(EmbeddedBrokerConfiguration configuration) {
        this.configuration = configuration;
    }

    public EmbeddedBrokerConfiguration getConfiguration() {
        return configuration;
    }

    public GroupMapperFunction getGroupMapperFunction() {
        return groupMapperFunction;
    }

    public void setGroupMapperFunction(GroupMapperFunction groupMapperFunction) {
        this.groupMapperFunction = groupMapperFunction;
    }

    public Broker getBroker() {
        return broker;
    }

    public BrokerConfiguration getBrokerConfiguration() {
        return brokerConfiguration;
    }

    public void start() throws Exception {
        String host = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_HOST, "localhost");
        int port = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_PORT, 7862);
        String mode = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_MODE, EmbeddedBrokerConfiguration.MODE_SIGLESERVER);
        String logDirectory = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_LOGSDIRECTORY, "txlog");
        String snapshotsDirectory = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_SNAPSHOTSDIRECTORY, "snapshots");
        String zkAdress = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_ZKADDRESS, "localhost:1281");
        String zkPath = configuration.getStringProperty(EmbeddedBrokerConfiguration.KEY_ZKPATH, "/majordodo");
        int zkSessionTimeout = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_ZKSESSIONTIMEOUT, 40000);

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
                statusChangesLog = new FileCommitLog(_logDirectory, _snapshotsDirectory);
                break;
            }
            case EmbeddedBrokerConfiguration.MODE_CLUSTERED: {
                Path _snapshotsDirectory = Paths.get(snapshotsDirectory);
                if (!Files.isDirectory(_snapshotsDirectory)) {
                    Files.createDirectory(_snapshotsDirectory);
                }
                statusChangesLog = new ReplicatedCommitLog(zkAdress, zkSessionTimeout, zkPath, _snapshotsDirectory, Broker.formatHostdata(host, port));
                int ensemble = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BK_ENSEMBLE_SIZE, 1);
                int writeQuorumSize = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BK_WRITEQUORUMSIZE, 1);
                int ackQuorumSize = configuration.getIntProperty(EmbeddedBrokerConfiguration.KEY_BK_ACKQUORUMSIZE, 1);
                break;
            }
        }
        brokerConfiguration = new BrokerConfiguration();
        brokerConfiguration.read(configuration.getProperties());
        broker = new Broker(brokerConfiguration, statusChangesLog, new TasksHeap(brokerConfiguration.getTasksHeapSize(), groupMapperFunction));
        broker.setBrokerId("embedded");
        switch (mode) {
            case EmbeddedBrokerConfiguration.MODE_JVMONLY:
                break;
            case EmbeddedBrokerConfiguration.MODE_SIGLESERVER:
            case EmbeddedBrokerConfiguration.MODE_CLUSTERED:
                server = new NettyChannelAcceptor(broker.getAcceptor(), host, port);
                break;
        }
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

}

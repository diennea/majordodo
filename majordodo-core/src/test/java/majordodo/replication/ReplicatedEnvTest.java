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
package majordodo.replication;

import majordodo.network.BrokerLocator;
import majordodo.network.jvm.JVMBrokerLocator;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.task.Broker;
import majordodo.task.SimpleBrokerSuite;
import majordodo.task.StatusChangesLog;
import org.junit.After;
import org.junit.Before;

/**
 * simple tests using real network connector
 *
 * @author enrico.olivelli
 */
public class ReplicatedEnvTest extends SimpleBrokerSuite {

    NettyChannelAcceptor server;
    ZKTestEnv zkEnv;
    String host = "localhost";
    int port = 7000;

    @Override
    protected BrokerLocator createBrokerLocator() throws Exception {
        return new ZKBrokerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath());
    }

    @Override
    protected StatusChangesLog createStatusChangesLog() throws Exception {
        return new ReplicatedCommitLog(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), workDir, Broker.formatHostdata(host, port, null));
    }

    @Override
    protected void beforeStartBroker() throws Exception {
        zkEnv = new ZKTestEnv(workDir);
        zkEnv.startBookie();
    }

    @Override
    protected void afterStartBroker() throws Exception {
        server = new NettyChannelAcceptor(broker.getAcceptor(), host, port);
        server.start();
    }

    @After
    public void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
        if (zkEnv != null) {
            zkEnv.close();
        }
    }

}

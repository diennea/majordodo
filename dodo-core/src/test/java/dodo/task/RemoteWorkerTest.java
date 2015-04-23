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
package dodo.task;

import dodo.network.BrokerLocator;
import dodo.network.jvm.JVMBrokerLocator;
import dodo.network.netty.NettyBrokerLocator;
import dodo.network.netty.NettyChannelAcceptor;
import org.junit.After;
import org.junit.Before;

/**
 * simple tests using real network connector
 *
 * @author enrico.olivelli
 */
public class RemoteWorkerTest extends SimpleBrokerSuite {

    NettyChannelAcceptor server;

    @Override
    protected BrokerLocator createBrokerLocator() {
        return new NettyBrokerLocator(server.getHost(), server.getPort());
    }

    @Before
    public void startServer() throws Exception {
        server = new NettyChannelAcceptor(broker.getAcceptor());
        server.start();
    }

    @After
    public void stopServer() throws Exception {
        if (server != null) {
            server.close();
        }
    }

}

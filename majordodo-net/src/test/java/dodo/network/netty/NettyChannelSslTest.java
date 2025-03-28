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
package dodo.network.netty;

import majordodo.network.netty.NettyConnector;
import majordodo.network.netty.NettyChannelAcceptor;
import majordodo.network.netty.NettyChannel;
import majordodo.network.Channel;
import majordodo.network.ChannelEventListener;
import majordodo.network.Message;
import majordodo.network.ReplyCallback;
import majordodo.network.ServerSideConnection;
import majordodo.network.ServerSideConnectionAcceptor;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Tests for netty channel
 *
 * @author enrico.olivelli
 */
public class NettyChannelSslTest {

    private static final class SimpleServerSideConnection implements ServerSideConnection {

        static final AtomicLong newConnectionId = new AtomicLong();
        long connectionId = newConnectionId.incrementAndGet();

        @Override
        public long getConnectionId() {
            return connectionId;
        }

        @Override
        public String toString() {
            return "SimpleServerSideConnection{" + "connectionId=" + connectionId + '}';
        }

    }

    @Test
    public void clientServerTest() throws Exception {
        List<Message> receivedFromServer = new CopyOnWriteArrayList<Message>();
        ServerSideConnectionAcceptor acceptor = new ServerSideConnectionAcceptor() {

            @Override
            public ServerSideConnection createConnection(final Channel channel) {
                channel.setMessagesReceiver(new ChannelEventListener() {

                    @Override
                    public void messageReceived(Message message) {
                        receivedFromServer.add(message);
                        channel.sendReplyMessage(message, Message.ACK("ok"));
                    }

                    @Override
                    public void channelClosed() {
                    }

                });
                return new SimpleServerSideConnection();
            }

        };
        BlockingQueue<Message> receivedFromClient = new ArrayBlockingQueue<>(100);
        BlockingQueue<Message> replyReceivedFromClient = new ArrayBlockingQueue<>(100);

        try (NettyChannelAcceptor server = new NettyChannelAcceptor(acceptor);) {
            server.setHost("0.0.0.0");
            server.setSsl(true);
            server.start();
            try (NettyConnector connector = new NettyConnector(new ChannelEventListener() {

                @Override
                public void messageReceived(Message message) {
                    receivedFromClient.add(message);
                }

                @Override
                public void channelClosed() {
                }
            })) {
                connector.setSsl(true);
                NettyChannel channel = connector.connect();
                Message message = Message.KILL_WORKER("testrequest");
                channel.sendMessageWithAsyncReply(message, 10000, new ReplyCallback() {

                    @Override
                    public void replyReceived(Message originalMessage, Message message, Throwable error) {
                        replyReceivedFromClient.add(message);
                    }
                });
                Message response = replyReceivedFromClient.take();
                assertEquals(Message.TYPE_ACK, response.type);
            }
        }

    }

}

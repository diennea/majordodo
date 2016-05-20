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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for netty channel
 *
 * @author enrico.olivelli
 */
public class NettyChannelTest {

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

//    @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.ALL;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter() {
//
//            @Override
//            public synchronized String format(LogRecord record) {
//                if (record.getThrown() != null) {
//                    return super.format(record);
//                } else {
//                    try {
//                        return record.getThreadID() + " - " + record.getLoggerName() + " - " + java.text.MessageFormat.format(record.getMessage(), record.getParameters()) + "\r\n";
//                    } catch (IllegalArgumentException er) {
//                        return record.getThreadID() + " - " + record.getLoggerName() + " - " + record.getMessage() + "\r\n";
//                    }
//                }
//            }
//
//        };
//
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }
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

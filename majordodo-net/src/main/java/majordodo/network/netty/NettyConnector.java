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
package majordodo.network.netty;

import majordodo.network.ChannelEventListener;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker-side connector
 *
 * @author enrico.olivelli
 */
public class NettyConnector implements AutoCloseable {

    private int port = 7000;
    private String host = "localhost";
    private NettyChannel channel;
    private Channel socketchannel;
    private EventLoopGroup group;
    private SslContext sslCtx;
    private boolean ssl;
    private boolean sslUnsecure = true;
    private final ChannelEventListener receiver;
    private final ExecutorService callbackExecutor = Executors.newCachedThreadPool();

    public boolean isSslUnsecure() {
        return sslUnsecure;
    }

    public void setSslUnsecure(boolean sslUnsecure) {
        this.sslUnsecure = sslUnsecure;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public NettyConnector(ChannelEventListener receiver) {
        this.receiver = receiver;
    }

    public NettyChannel connect() throws Exception {
        boolean useOpenSSL = NetworkUtils.isOpenSslAvailable();
        if (ssl) {
            if (sslUnsecure) {
                this.sslCtx = SslContextBuilder
                    .forClient()
                    .sslProvider(useOpenSSL ? SslProvider.OPENSSL : SslProvider.JDK)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            } else {
                this.sslCtx = SslContextBuilder
                    .forClient()
                    .sslProvider(useOpenSSL ? SslProvider.OPENSSL : SslProvider.JDK)
                    .build();
            }
        }
        if (NetworkUtils.isEnableEpollNative()) {
            group = new EpollEventLoopGroup();
        } else {
            group = new NioEventLoopGroup();
        }
        LOG.log(Level.INFO, "Trying to connect to broker at " + host + ":" + port
            + " ssl:" + ssl + ", sslUnsecure:" + sslUnsecure + " openSsl:" + useOpenSSL);

        Bootstrap b = new Bootstrap();
        b.group(group)
            .channel(NetworkUtils.isEnableEpollNative() ? EpollSocketChannel.class : NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    channel = new NettyChannel(host + ":" + port, ch, callbackExecutor, NettyConnector.this);
                    channel.setMessagesReceiver(receiver);
                    channel.setRemoteHost(host);
                    if (ssl) {
                        ch.pipeline().addLast(sslCtx.newHandler(ch.alloc(), host, port));
                    }
                    ch.pipeline().addLast("lengthprepender", new LengthFieldPrepender(4));
                    ch.pipeline().addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//
                    ch.pipeline().addLast("messageencoder", new DodoMessageEncoder());
                    ch.pipeline().addLast("messagedecoder", new DodoMessageDecoder());
                    ch.pipeline().addLast(new InboundMessageHandler(channel));
                }
            });

        ChannelFuture f = b.connect(host, port).sync();
        socketchannel = f.channel();
        return channel;

    }
    private static final Logger LOG = Logger.getLogger(NettyConnector.class.getName());

    public NettyChannel getChannel() {
        return channel;
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
        }
        if (socketchannel != null) {
            try {
                socketchannel.close();
            } finally {
                socketchannel = null;
            }
        }
        if (group != null) {
            try {
                group.shutdownGracefully();
            } finally {
                group = null;
            }
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdown();
        }
    }

    public void setHost(String host) {
        this.host = host;
    }

}

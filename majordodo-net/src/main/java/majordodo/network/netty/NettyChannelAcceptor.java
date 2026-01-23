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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import majordodo.network.ServerSideConnectionAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts connections from workers
 *
 * @author enrico.olivelli
 */
public class NettyChannelAcceptor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyChannelAcceptor.class);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private int port = 7000;
    private String host = "localhost";
    private boolean ssl;
    private ServerSideConnectionAcceptor acceptor;
    private SslContext sslCtx;
    private List<String> sslCiphers;
    private File sslCertChainFile;
    private File sslCertFile;
    private String sslCertPassword;
    private int workerThreads = 16;
    private final ExecutorService callbackExecutor = Executors.newCachedThreadPool();
    private final Set<NettyChannel> activeChannels = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "dodo-netty-acceptor-reply-deadlines");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> timerTask;

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public File getSslCertChainFile() {
        return sslCertChainFile;
    }

    public void setSslCertChainFile(File sslCertChainFile) {
        this.sslCertChainFile = sslCertChainFile;
    }

    public File getSslCertFile() {
        return sslCertFile;
    }

    public void setSslCertFile(File sslCertFile) {
        this.sslCertFile = sslCertFile;
    }

    public String getSslCertPassword() {
        return sslCertPassword;
    }

    public void setSslCertPassword(String sslCertPassword) {
        this.sslCertPassword = sslCertPassword;
    }

    public List<String> getSslCiphers() {
        return sslCiphers;
    }

    public void setSslCiphers(List<String> sslCiphers) {
        this.sslCiphers = sslCiphers;
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

    public void setHost(String host) {
        this.host = host;
    }

    private Channel channel;

    public NettyChannelAcceptor(ServerSideConnectionAcceptor acceptor) {
        this.acceptor = acceptor;
    }

    public NettyChannelAcceptor(ServerSideConnectionAcceptor acceptor, String host, int port) {
        this.acceptor = acceptor;
        this.host = host;
        this.port = port;
    }

    public void start() throws Exception {
        this.timerTask = timer.scheduleAtFixedRate(() -> {
            var channelsToRemove = new HashSet<NettyChannel>();
            for (NettyChannel channel : activeChannels) {
                if (channel.isValid()) {
                    channel.processPendingReplyMessagesDeadline();
                } else {
                    channelsToRemove.add(channel);
                }
            }
            activeChannels.removeAll(channelsToRemove);
        }, 5000, 5000, TimeUnit.MILLISECONDS);
        boolean useOpenSSL = NetworkUtils.isOpenSslAvailable();
        if (ssl) {

            if (sslCertFile == null) {
                LOGGER.error("start SSL with self-signed auto-generated certificate, useOpenSSL:{}", useOpenSSL);
                if (sslCiphers != null) {
                    LOGGER.error("required sslCiphers {}", sslCiphers);
                }
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                try {
                    sslCtx = SslContextBuilder
                            .forServer(ssc.certificate(), ssc.privateKey())
                            .sslProvider(useOpenSSL ? SslProvider.OPENSSL : SslProvider.JDK)
                            .ciphers(sslCiphers).build();
                } finally {
                    ssc.delete();
                }
            } else {
                LOGGER.error("start SSL with certificate {} chain file {} , useOpenSSL:{}", sslCertFile.getAbsolutePath(),
                        (sslCertChainFile == null ? "null" : sslCertChainFile.getAbsolutePath()), useOpenSSL);
                if (sslCiphers != null) {
                    LOGGER.error("required sslCiphers {}", sslCiphers);
                }
                SslContextBuilder builder;
                if (sslCertFile.getName().endsWith(".p12") || sslCertFile.getName().endsWith(".pfx")) {
                    try (FileInputStream fis = new FileInputStream(sslCertFile)) {
                        KeyStore ks = KeyStore.getInstance("PKCS12");
                        ks.load(fis, sslCertPassword.toCharArray());

                        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(ks, sslCertPassword.toCharArray());

                        builder = SslContextBuilder.forServer(kmf);
                    } catch (Exception e) {
                        throw new SSLException("provided certFile looks like a PKCS12 file but could not be loaded", e);
                    }
                } else {
                    builder = SslContextBuilder.forServer(sslCertChainFile, sslCertFile, sslCertPassword);
                }
                sslCtx = builder.sslProvider(useOpenSSL ? SslProvider.OPENSSL : SslProvider.JDK)
                        .ciphers(sslCiphers)
                        .build();
            }

        }
        if (NetworkUtils.isEnableEpollNative()) {
            bossGroup = new EpollEventLoopGroup(workerThreads);
            workerGroup = new EpollEventLoopGroup(workerThreads);
            LOGGER.info("Using netty-native-epoll network type");
        } else {
            bossGroup = new NioEventLoopGroup(workerThreads);
            workerGroup = new NioEventLoopGroup(workerThreads);
        }
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NetworkUtils.isEnableEpollNative() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        NettyChannel session = new NettyChannel("client", ch, callbackExecutor, null);
                        activeChannels.add(session);
                        if (acceptor != null) {
                            acceptor.createConnection(session);
                        }
                        
                        // Add SSL handler first to encrypt and decrypt everything.
                        if (ssl) {
                            ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()));
                        }

                        ch.pipeline().addLast("lengthprepender", new LengthFieldPrepender(4));
                        ch.pipeline().addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                        ch.pipeline().addLast("messageencoder", new DodoMessageEncoder());
                        ch.pipeline().addLast("messagedecoder", new DodoMessageDecoder());
                        ch.pipeline().addLast(new InboundMessageHandler(session));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        ChannelFuture f = b.bind(host, port).sync(); // (7)
        this.channel = f.channel();

    }

    @Override
    public void close() {
        activeChannels.clear();
        if (timerTask != null) {
            timerTask.cancel(false);
        }
        timer.shutdown();
        if (channel != null) {
            channel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (callbackExecutor != null) {
            callbackExecutor.shutdown();
        }
    }
}

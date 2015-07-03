package dodo.network.netty;

import dodo.network.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class NettyBrokerLocator implements BrokerLocator {

    private final List<InetSocketAddress> servers = new ArrayList<>();
    private final AtomicInteger index = new AtomicInteger();

    public NettyBrokerLocator(String host, int port) {
        this.servers.add(new InetSocketAddress(host, port));
    }

    public NettyBrokerLocator(final List<InetSocketAddress> servers) {
        this.servers.addAll(servers);
    }
    
    public NettyBrokerLocator(String host, int port, String host2, int port2) {
        this.servers.add(new InetSocketAddress(host, port));
        this.servers.add(new InetSocketAddress(host2, port2));
    }

    @Override
    public Channel connect(ChannelEventListener messageReceiver, ConnectionRequestInfo workerInfo) throws InterruptedException, BrokerNotAvailableException, BrokerRejectedConnectionException {
        boolean ok = false;
        NettyConnector connector = new NettyConnector(messageReceiver);
        try {
            InetSocketAddress addre = servers.get(index.get() % servers.size());
            connector.setPort(addre.getPort());
            connector.setHost(addre.getAddress().getHostAddress());
            NettyChannel channel;
            try {
                channel = connector.connect();
            } catch (final Exception e) {
                index.incrementAndGet();
                throw new BrokerNotAvailableException(e);
            }

            Message acceptMessage = Message.WORKER_CONNECTION_REQUEST(workerInfo.getWorkerId(), workerInfo.getProcessId(), workerInfo.getLocation(), workerInfo.getRunningTaskIds());
            try {
                Message connectionResponse = channel.sendMessageWithReply(acceptMessage, 10000);
                if (connectionResponse.type == Message.TYPE_ACK) {
                    ok = true;
                    return channel;
                } else {
                    throw new BrokerRejectedConnectionException("Broker rejected connection, response message:" + connectionResponse);
                }
            } catch (TimeoutException err) {
                throw new BrokerNotAvailableException(err);
            }
        } finally {
            if (!ok && connector != null) {
                connector.close();
            }
        }
    }
}

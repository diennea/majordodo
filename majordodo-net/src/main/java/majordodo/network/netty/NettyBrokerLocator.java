package majordodo.network.netty;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class NettyBrokerLocator extends GenericNettyBrokerLocator {

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
    protected InetSocketAddress getServer() {
        return servers.get(index.get() % servers.size());
    }

    @Override
    public void brokerDisconnected() {
        index.incrementAndGet();
    }

}

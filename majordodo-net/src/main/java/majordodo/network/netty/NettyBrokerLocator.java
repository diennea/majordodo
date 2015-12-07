package majordodo.network.netty;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import majordodo.network.BrokerHostData;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class NettyBrokerLocator extends GenericNettyBrokerLocator {

    private final List<BrokerHostData> servers = new ArrayList<>();
    private final AtomicInteger index = new AtomicInteger();
    private boolean sslUnsecure = true;

    public NettyBrokerLocator(String host, int port, boolean ssl) {
        this.servers.add(new BrokerHostData(host, port, "", ssl, new HashMap<String, String>()));
    }

    public NettyBrokerLocator(final List<InetSocketAddress> servers, boolean ssl) {
        servers.forEach(s -> {
            this.servers.add(new BrokerHostData(s.getHostName(), s.getPort(), "", ssl, new HashMap<>()));
        });

    }

    public NettyBrokerLocator(String host, int port, String host2, int port2) {
        this(Arrays.asList(new InetSocketAddress(host, port), new InetSocketAddress(host2, port2)), false);
    }

    @Override
    protected BrokerHostData getServer() {
        return servers.get(index.get() % servers.size());
    }

    @Override
    public void brokerDisconnected() {
        index.incrementAndGet();
    }

    public boolean isSslUnsecure() {
        return sslUnsecure;
    }

    public void setSslUnsecure(boolean sslUnsecure) {
        this.sslUnsecure = sslUnsecure;
    }

}

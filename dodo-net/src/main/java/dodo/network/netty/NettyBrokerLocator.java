package dodo.network.netty;

import dodo.network.*;

import java.util.concurrent.TimeoutException;

/**
 * Created by enrico.olivelli on 24/03/2015.
 */
public class NettyBrokerLocator implements BrokerLocator  {

    private String host;
    private int port;

    public NettyBrokerLocator(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public Channel connect(ChannelEventListener messageReceiver, ConnectionRequestInfo workerInfo) throws InterruptedException, BrokerNotAvailableException, BrokerRejectedConnectionException {
        boolean ok = false;
        NettyConnector connector = new NettyConnector(messageReceiver);
        try {
            connector.setPort(port);
            connector.setHost(host);
            NettyChannel channel;
            try {
                channel = connector.connect();
            } catch (final Exception e) {
                throw new BrokerNotAvailableException(e);
            }

            Message acceptMessage = Message.WORKER_CONNECTION_REQUEST(workerInfo.getWorkerId(), workerInfo.getProcessId(), workerInfo.getMaximumThreadPerTag(), workerInfo.getLocation(), workerInfo.getRunningTaskIds());
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

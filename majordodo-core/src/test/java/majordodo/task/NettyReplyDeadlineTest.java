package majordodo.task;

import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.TaskStatusView;
import majordodo.network.Message;
import majordodo.network.netty.NettyBrokerLocator;
import majordodo.utils.TestUtils;
import majordodo.worker.WorkerCore;
import majordodo.worker.WorkerCoreConfiguration;
import majordodo.worker.WorkerStatusListener;
import org.junit.Assert;
import org.junit.Test;

@BrokerTestUtils.StartBroker
public class NettyReplyDeadlineTest extends BrokerTestUtils {

    @Test
    public void testWorkerRetryTaskFinished() throws Exception {
        String workerId = "abc";
        String taskParams = "param";

        AtomicBoolean swallowTaskFinished = new AtomicBoolean(true);
        AtomicLong taskFinishedReceived = new AtomicLong(0);
        AtomicLong taskId = new AtomicLong(0);

        broker.getAcceptor().setConnectionFactory((channel, broker1) -> {
            BrokerSideConnection conn = new BrokerSideConnection() {
                @Override
                public void messageReceived(Message message) {
                    if (message.type == Message.TYPE_TASK_FINISHED) {
                        taskFinishedReceived.incrementAndGet();
                        if (swallowTaskFinished.get()) {
                            System.out.println("[DEBUG_LOG] Swallowing TASK_FINISHED for " + taskId.get());
                            swallowTaskFinished.set(false);
                            return;
                        }
                    }
                    super.messageReceived(message);
                }

                @Override
                public boolean validate() {
                    return true;
                }
            };
            conn.setBroker(broker1);
            conn.setChannel(channel);
            return conn;
        });

        try (NettyBrokerLocator locator = new NettyBrokerLocator(server.getHost(), server.getPort(), server.isSsl())) {
            CountDownLatch connectedLatch = new CountDownLatch(1);
            WorkerStatusListener listener = new WorkerStatusListener() {
                @Override
                public void connectionEvent(String event, WorkerCore core) {
                    if (event.equals(WorkerStatusListener.EVENT_CONNECTED)) {
                        connectedLatch.countDown();
                    }
                }
            };

            Map<String, Integer> tags = new HashMap<>();
            tags.put(TASKTYPE_MYTYPE, 1);

            WorkerCoreConfiguration config = new WorkerCoreConfiguration();
            config.setWorkerId(workerId);
            config.setMaxThreadsByTaskType(tags);
            config.setGroups(Arrays.asList(group));
            config.setMaxPendingFinishedTaskNotifications(1);
            config.setMaxWaitPendingFinishedTaskNotifications(10000);
            config.setMaxKeepAliveTime(1000);
            config.setNetworkTimeout(1000); // reply deadline 1 second

            try (WorkerCore worker = new WorkerCore(config, workerId, locator, listener)) {
                worker.setExecutorFactory((String taskType, Map<String, Object> parameters) -> new majordodo.executors.TaskExecutor() {
                    @Override
                    public String executeTask(Map<String, Object> parameters) throws Exception {
                        return "ok";
                    }
                });
                worker.start();

                assertTrue(connectedLatch.await(10, TimeUnit.SECONDS));

                // send task
                // swallow first task_finished
                // process the second one (the retry)
                // assert that task is finished on broker side and we received both task_finished messages

                taskId.set(broker.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 0, 0, 0, null, 0, null, null)).getTaskId());

                TestUtils.waitForCondition(() -> {
                    TaskStatusView task = broker.getClient().getTask(taskId.get());
                    return task != null && task.getStatus() == Task.STATUS_RUNNING;
                }, TestUtils.NOOP, 10);

                TestUtils.waitForCondition(() -> {
                    TaskStatusView task = broker.getClient().getTask(taskId.get());
                    return task != null && task.getStatus() == Task.STATUS_FINISHED;
                }, TestUtils.NOOP, 20);

                Assert.assertEquals(2, taskFinishedReceived.get());
            }

        }
    }
}

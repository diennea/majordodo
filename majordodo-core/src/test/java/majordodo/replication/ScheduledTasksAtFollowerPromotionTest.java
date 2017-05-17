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
package majordodo.replication;

import java.util.Map;
import majordodo.clientfacade.AddTaskRequest;
import majordodo.clientfacade.DelayedTasksQueueView;
import majordodo.task.BrokerTestUtils;
import majordodo.task.Task;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author francesco.caliumi
 */
@BrokerTestUtils.StartReplicatedBrokers
@BrokerTestUtils.LogLevel(level="ALL")
public class ScheduledTasksAtFollowerPromotionTest extends BrokerTestUtils {
    
    @Before
    public void before() {
        broker1Config.setMaxWorkerIdleTime(1000);
        broker2Config.setMaxWorkerIdleTime(1000);
    }

    @Test
    public void checkDelayedStatusOnFollowerTest() throws Exception {
        String taskParams = "param";
        String slot = "slot";
        Map<String, Long> busySlots;
        DelayedTasksQueueView delayedTasks;

        long requestedStartTime = System.currentTimeMillis() + 100*1000;
        
        long taskId = broker1.getClient().submitTask(new AddTaskRequest(0, TASKTYPE_MYTYPE, userId, taskParams, 1, requestedStartTime, 0, slot, 0, null, null)).getTaskId();
        
        checkTaskStatus(broker1, taskId, Task.STATUS_DELAYED, null, 30*1000, true);
        checkTaskStatus(broker2, taskId, Task.STATUS_DELAYED, null, 30*1000, false);
        
        busySlots = broker1.getSlotsStatusView().getBusySlots();
        assertEquals(1, busySlots.size());
        assertEquals(Long.valueOf(taskId), busySlots.get(slot));
        
        busySlots = broker2.getSlotsStatusView().getBusySlots();
        assertEquals(1, busySlots.size());
        assertEquals(Long.valueOf(taskId), busySlots.get(slot));
        
        delayedTasks = broker2.getDelayedTasksQueueView();
        assertEquals(0, delayedTasks.getTasks().size());
        
        broker1.die();
        
        waitForBrokerToBecomeLeader(broker2, 30*1000);
        
        checkTaskStatus(broker2, taskId, Task.STATUS_DELAYED, null, 30*1000, false);
        
        busySlots = broker2.getSlotsStatusView().getBusySlots();
        assertEquals(1, busySlots.size());
        assertEquals(Long.valueOf(taskId), busySlots.get(slot));
        
        delayedTasks = broker2.getDelayedTasksQueueView();
        assertEquals(1, delayedTasks.getTasks().size());
        assertEquals(taskId, delayedTasks.getTasks().get(0).getTaskId());
        
    }
}
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

import dodo.network.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author enrico.olivelli
 */
public class DodoMessageUtilsTest {

    @Test
    public void testEncodeMessage() {
        ByteBuf encoded = Unpooled.buffer();
        Map<String, Object> pp = new HashMap<>();
        pp.put("1", 1);
        pp.put("2", 2L);
        pp.put("3", "3");
        Message m = new Message("a", Message.TYPE_KILL_WORKER, pp);
        m.replyMessageId = "b";
        m.messageId = "c";
        DodoMessageUtils.encodeMessage(encoded, m);
        Message read = DodoMessageUtils.decodeMessage(encoded);
        assertEquals(read.messageId, m.messageId);
        assertEquals(read.replyMessageId, m.replyMessageId);
        assertEquals(read.workerProcessId, m.workerProcessId);
        assertEquals(read.parameters.size(), m.parameters.size());
        assertEquals(read.parameters.get("1"), m.parameters.get("1"));
        assertEquals(read.parameters.get("2"), m.parameters.get("2"));
        assertEquals(read.parameters.get("3"), m.parameters.get("3"));
    }

}

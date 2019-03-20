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

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *
 * @author dennis.mercuriali
 */
public class LedgerMetadataUtils {

    private static final String APPLICATION_METADATA = "application";
    private static final byte[] APPLICATION_METADATA_VALUE = "majordodo".getBytes(StandardCharsets.UTF_8);
    
    private static final String COMPONENT_METADATA = "component";
    private static final byte[] COMPONENT_METADATA_VALUE = "broker".getBytes(StandardCharsets.UTF_8);
    
    private static final String CREATED_BY_METADATA = "broker-id";

    public static Map<String, byte[]> buildBrokerLedgerMetadata(String brokerId) {
        byte[] createdByValue = brokerId == null ? null : brokerId.getBytes(StandardCharsets.UTF_8);
        
        return ImmutableMap.of(
            APPLICATION_METADATA, APPLICATION_METADATA_VALUE,
            COMPONENT_METADATA, COMPONENT_METADATA_VALUE,
            CREATED_BY_METADATA, createdByValue
        );
    }
}

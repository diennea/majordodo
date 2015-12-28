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
package majordodo.codepools;

/**
 * A pool of Code (JAR files/Java Classes) used for the implementation of Tasks.
 * Code pools are immutable
 *
 * @author enrico.olivelli
 */
public class CodePool {

    private final String id;
    private final long creationTimestamp;
    private final byte[] codePoolData;
    private final long ttl;

    public CodePool(String id, long creationTimestamp, byte[] codePoolData, long ttl) {
        this.id = id;
        this.creationTimestamp = creationTimestamp;
        this.codePoolData = codePoolData;
        this.ttl = ttl;
    }

    public String getId() {
        return id;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public byte[] getCodePoolData() {
        return codePoolData;
    }

    public long getTtl() {
        return ttl;
    }

}

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
package majordodo.client;

/**
 * Request to create a new CodePool. Each CodePool has an unique ID. A CodePool
 * is immutable, you cannot update a CodePool once created
 *
 * @author enrico.olivelli
 */
public class CreateCodePoolRequest {

    private String codePoolID;
    private long ttl;
    private String codePoolData;

    public CreateCodePoolRequest() {
    }

    public String getCodePoolID() {
        return codePoolID;
    }

    public void setCodePoolID(String codePoolID) {
        this.codePoolID = codePoolID;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public String getCodePoolData() {
        return codePoolData;
    }

    public void setCodePoolData(String codePoolData) {
        this.codePoolData = codePoolData;
    }

}

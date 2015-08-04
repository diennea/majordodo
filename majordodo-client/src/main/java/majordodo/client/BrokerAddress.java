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

import java.util.Map;

/**
 * Address of a broker
 *
 * @author enrico.olivelli
 */
public final class BrokerAddress {

    private String address;
    private int port;
    private String path = "/majordodo";
    private Map<String, String> info;
    private String protocol = "http";

    public static BrokerAddress http(String host, int port) {
        BrokerAddress r = new BrokerAddress();
        r.setAddress(host);
        r.setPort(port);
        r.setProtocol("http");
        return r;
    }

    public static BrokerAddress https(String host, int port) {
        BrokerAddress r = new BrokerAddress();
        r.setAddress(host);
        r.setPort(port);
        r.setProtocol("https");
        return r;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Map<String, String> getInfo() {
        return info;
    }

    public void setInfo(Map<String, String> info) {
        this.info = info;
    }

}

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
package majordodo.client.http;

import java.net.InetSocketAddress;
import java.util.List;
import majordodo.client.BrokerDiscoveryService;
import majordodo.client.discovery.StaticBrokerDiscoveryService;

/**
 * Client configuration
 *
 * @author enrico.olivelli
 */
public class ClientConfiguration {

    private boolean disableHttpsVerification = false;
    private int maxConnTotal = 10;
    private int maxConnPerRoute = 10;
    private int sotimeout = 240000;
    private int keepAlive = 30000;
    private BrokerDiscoveryService brokerDiscoveryService = new StaticBrokerDiscoveryService(null);
    private String username;
    private String password;
    private int connectionTimeout = 30000;

    public static ClientConfiguration defaultConfiguration() {
        return new ClientConfiguration();
    }

    private ClientConfiguration() {
    }

    public BrokerDiscoveryService getBrokerDiscoveryService() {
        return brokerDiscoveryService;
    }

    public ClientConfiguration setBrokerDiscoveryService(BrokerDiscoveryService brokerDiscoveryService) {
        this.brokerDiscoveryService = brokerDiscoveryService;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ClientConfiguration setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ClientConfiguration setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public ClientConfiguration setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDisableHttpsVerification() {
        return disableHttpsVerification;
    }

    public ClientConfiguration setDisableHttpsVerification(boolean disableHttpsVerification) {
        this.disableHttpsVerification = disableHttpsVerification;
        return this;
    }

    public int getMaxConnTotal() {
        return maxConnTotal;
    }

    public ClientConfiguration setMaxConnTotal(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
        return this;
    }

    public int getMaxConnPerRoute() {
        return maxConnPerRoute;
    }

    public ClientConfiguration setMaxConnPerRoute(int maxConnPerRoute) {
        this.maxConnPerRoute = maxConnPerRoute;
        return this;
    }

    public int getSotimeout() {
        return sotimeout;
    }

    public ClientConfiguration setSotimeout(int sotimeout) {
        this.sotimeout = sotimeout;
        return this;
    }

    public int getKeepAlive() {
        return keepAlive;
    }

    public ClientConfiguration setKeepAlive(int keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

}

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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import majordodo.client.BrokerAddress;

import org.apache.http.HttpResponse;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

/**
 * HTTP Client of Majordodo. Provides connection pooling
 *
 * @author enrico.olivelli
 */
public class Client implements AutoCloseable {

    /**
     * Per autorizzare tutti i certificati SSL
     */
    private static class MyTrustManager implements X509TrustManager {

        MyTrustManager() { // constructor
            // create/load keystore
        }

        @Override
        public void checkClientTrusted(X509Certificate chain[], String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate chain[], String authType) throws CertificateException {
            // special handling such as poping dialog boxes
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            X509Certificate[] res = new X509Certificate[0];
            return res;
        }
    }
    private CloseableHttpClient httpclient;
    private PoolingHttpClientConnectionManager poolManager;
    private final ClientConfiguration configuration;

    public Client(ClientConfiguration configuration) {
        this.configuration = configuration;
        createClient();
    }

    public HTTPClientConnection openConnection() throws IOException {
        CloseableHttpClient _client = httpclient;
        if (_client == null) {
            throw new IOException("shared connection pool is closed");
        }

        return new HTTPClientConnection(_client, configuration, configuration.getBrokerDiscoveryService());
    }

    public void close() {
        if (httpclient != null) {
            try {
                httpclient.close();
            } catch (IOException err) {
                err.printStackTrace();
            } finally {
                httpclient = null;
                poolManager = null;
            }
        }
    }

    private void createClient() {

        try {
            SSLContext sslContext;
            SSLConnectionSocketFactory sslsf;
            if (configuration.isDisableHttpsVerification()) {
                sslContext = SSLContext.getInstance("SSL");
                TrustManager mytm[] = {new MyTrustManager()};
                sslContext.init(null, mytm, null);
                sslsf = new SSLConnectionSocketFactory(
                        sslContext,
                        SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            } else {
                sslContext = SSLContexts.custom().build();
                sslsf = new SSLConnectionSocketFactory(
                        sslContext,
                        SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
            }

            Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", new PlainConnectionSocketFactory())
                    .register("https", sslsf)
                    .build();

            poolManager = new PoolingHttpClientConnectionManager(r);

            if (configuration.getMaxConnTotal() > 0) {
                poolManager.setMaxTotal(configuration.getMaxConnTotal());
            }
            if (configuration.getMaxConnPerRoute() > 0) {
                poolManager.setDefaultMaxPerRoute(configuration.getMaxConnPerRoute());
            }

            poolManager.setDefaultSocketConfig(SocketConfig.custom().setSoKeepAlive(true).setSoReuseAddress(true).setTcpNoDelay(false).setSoTimeout(configuration.getSotimeout()).build());

            ConnectionKeepAliveStrategy myStrategy = (HttpResponse response, HttpContext context) -> configuration.getKeepAlive();

            httpclient = HttpClients
                    .custom()
                    .setConnectionManager(poolManager)
                    .setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
                    .setKeepAliveStrategy(myStrategy)
                    .build();
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            throw new RuntimeException(ex);
        }

    }

}

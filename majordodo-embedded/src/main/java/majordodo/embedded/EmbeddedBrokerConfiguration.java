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
package majordodo.embedded;

/**
 * Configuration for the embedded broker
 *
 * @author enrico.olivelli
 */
public class EmbeddedBrokerConfiguration extends AbstractEmbeddedServiceConfiguration {

    public static final String KEY_LOGSDIRECTORY = "logsdirectory";
    public static final String KEY_LOGSMAXFILESIZE = "logsmaxfilesize";
    public static final String KEY_SNAPSHOTSDIRECTORY = "snapshotsdirectory";
    public static final String KEY_HOST = "broker.host";
    public static final String KEY_PORT = "broker.port";
    public static final String KEY_SSL = "broker.ssl";
    public static final String KEY_SSL_UNSECURE = "broker.ssl.unsecure";
    public static final String KEY_CLIENTAPIURL = "broker.clientapiurl";
    public static final String KEY_BROKERID = "broker.id";

    public static final String SSL_CERTIFICATE_FILE = "broker.ssl.certificatefile";
    public static final String SSL_CERTIFICATE_CHAIN_FILE = "broker.ssl.certificatechainfile";
    public static final String SSL_CERTIFICATE_PASSWORD = "broker.ssl.certificatefilepassword";
    public static final String SSL_CIPHERS = "broker.ssl.ciphers";

    public static final String KEY_BK_ENSEMBLE_SIZE = "bookkeeper.ensemblesize";
    public static final String KEY_BK_WRITEQUORUMSIZE = "bookkeeper.writequorumsize";
    public static final String KEY_BK_ACKQUORUMSIZE = "bookkeeper.ackquorumsize";
    public static final String KEY_BK_LEDGERSRETENTIONPERIOD = "bookkeeper.ledgersretentionperiod";

    public static final String KEY_BROKERWORKERTHREADS = "broker.worker.threads";
    public static final String KEY_REQUIREAUTHENTICATION = "broker.requireauthentication";
    public static final boolean KEY_REQUIREAUTHENTICATION_DEFAULT = true;

}

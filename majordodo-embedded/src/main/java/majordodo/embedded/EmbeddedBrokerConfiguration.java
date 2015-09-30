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

import java.io.File;

/**
 * Configuration for the embedded broker
 *
 * @author enrico.olivelli
 */
public class EmbeddedBrokerConfiguration extends AbstractEmbeddedServiceConfiguration {

    public static String KEY_LOGSDIRECTORY = "logsdirectory";
    public static String KEY_LOGSMAXFILESIZE = "logsmaxfilesize";
    public static String KEY_SNAPSHOTSDIRECTORY = "snapshotsdirectory";
    public static String KEY_HOST = "broker.host";
    public static String KEY_PORT = "broker.port";
    public static String KEY_SSL = "broker.ssl";
    public static String KEY_CLIENTAPIURL = "broker.clientapiurl";
    public static String KEY_BROKERID = "broker.id";

    public static String SSL_CERTIFICATE_FILE = "broker.ssl.certificatefile";
    public static String SSL_CERTIFICATE_CHAIN_FILE = "broker.ssl.certificatechainfile";
    public static String SSL_CERTIFICATE_PASSWORD = "broker.ssl.certificatefilepassword";

    public static String KEY_BK_ENSEMBLE_SIZE = "bookkeeper.ensemblesize";
    public static String KEY_BK_WRITEQUORUMSIZE = "bookkeeper.writequorumsize";
    public static String KEY_BK_ACKQUORUMSIZE = "bookkeeper.ackquorumsize";
    public static String KEY_BK_LEDGERSRETENTIONPERIOD = "bookkeeper.ledgersretentionperiod";

}

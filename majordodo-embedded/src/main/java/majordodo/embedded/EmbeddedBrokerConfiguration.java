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

    public static String KEY_LOGSDIRECTORY = "logsdirectory";
    public static String KEY_LOGSMAXFILESIZE = "logsmaxfilesize";
    public static String KEY_SNAPSHOTSDIRECTORY = "snapshotsdirectory";
    public static String KEY_HOST = "broker.host";
    public static String KEY_PORT = "broker.port";
    public static String KEY_CLIENTAPIURL = "broker.clientapiurl";
    public static String KEY_BROKERID = "broker.id";

    public static String KEY_BK_ENSEMBLE_SIZE = "bookeeper.ensemblesize";
    public static String KEY_BK_WRITEQUORUMSIZE = "bookeeper.writequorumsize";
    public static String KEY_BK_ACKQUORUMSIZE = "bookeeper.ackquorumsize";
    public static String KEY_BK_LEDGERSRETENTIONPERIOD = "bookeeper.ledgersretentionperiod";

}

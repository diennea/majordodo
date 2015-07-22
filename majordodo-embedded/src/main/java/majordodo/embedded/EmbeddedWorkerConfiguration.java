/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.embedded;

/**
 * Configuration for worker configuration
 *
 * @author enrico.olivelli
 */
public class EmbeddedWorkerConfiguration extends AbstractEmbeddedServiceConfiguration {

    public static final String MODE_SINGLEBROKER = "singlebroker";
    public static final String MODE_CLUSTERED = "clustered";
    public static final String MODE_JVMONLY = "jvmonly";

    public static final String KEY_MODE = "worker.mode";

    public static String KEY_HOST = "worker.broker.host";
    public static String KEY_PORT = "worker.broker.port";
}

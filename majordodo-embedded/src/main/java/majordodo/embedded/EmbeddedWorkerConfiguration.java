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

    public static String KEY_HOST = "worker.broker.host";
    public static String KEY_PORT = "worker.broker.port";
}

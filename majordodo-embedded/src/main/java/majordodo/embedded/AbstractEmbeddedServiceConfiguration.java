/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.embedded;

import java.util.HashMap;
import java.util.Map;

/**
 * utility
 *
 * @author enrico.olivelli
 */
public abstract class AbstractEmbeddedServiceConfiguration {

    public static String KEY_ZKADDRESS = "zk.address";
    public static String KEY_ZKSESSIONTIMEOUT = "zk.sessiontimeout";
    public static String KEY_ZKPATH = "zk.path";

    public static final String MODE_SIGLESERVER = "singleserver";
    public static final String MODE_CLUSTERED = "clustered";
    public static final String MODE_JVMONLY = "jvmonly";

    public static final String KEY_MODE = "mode";

    private final Map<String, Object> properties = new HashMap<>();

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getStringProperty(String key, String defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value.toString();
    }

    public int getIntProperty(String key, int defaultValue) {
        Object value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value.toString());
    }
}

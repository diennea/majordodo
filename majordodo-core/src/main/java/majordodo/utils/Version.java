package majordodo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Versioning Utils
 *
 * @author enrico.olivelli
 */
public abstract class Version {

    private static final String VERSION;

    private Version() {
    }

    static {
        Properties pluginProperties = new Properties();
        try (InputStream in = Version.class.getResourceAsStream("/majordodo/version/version.properties")) {
            pluginProperties.load(in);
            VERSION = (String) pluginProperties.get("version");
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    public static String getVersion() {
        return VERSION;
    }
}

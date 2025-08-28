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
package majordodo.network.netty;

import io.netty.handler.ssl.OpenSsl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkUtils {

    private static final boolean ENABLE_EPOLL_NATIVE = System.getProperty("os.name").equalsIgnoreCase("linux")
        && !Boolean.getBoolean("majordodo.network.disablenativeepoll");

    private static final boolean ENABLE_OPENSSL
        = !Boolean.getBoolean("majordodo.network.disableopenssl");

    public static boolean isEnableEpollNative() {
        return ENABLE_EPOLL_NATIVE;
    }

    private static Boolean openSslAvailable;
    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    public static boolean isOpenSslAvailable() {
        if (openSslAvailable != null) {
            return openSslAvailable;
        }
        if (ENABLE_OPENSSL && OpenSsl.isAvailable()) {
            OpenSsl.ensureAvailability();
            openSslAvailable = true;
        } else {
            Throwable cause = OpenSsl.unavailabilityCause();
            LOG.info("Native OpenSSL support is not available on this platform: {}", cause);
            openSslAvailable = false;
        }
        return openSslAvailable;
    }

}

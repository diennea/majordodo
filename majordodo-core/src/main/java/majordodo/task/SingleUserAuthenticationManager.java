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
package majordodo.task;

import majordodo.clientfacade.AuthenticatedUser;
import majordodo.clientfacade.AuthenticationManager;
import majordodo.clientfacade.UserRole;

/**
 * Simple Authentication Manager, only one user, which is an administrator
 *
 * @author enrico.olivelli
 */
public class SingleUserAuthenticationManager extends AuthenticationManager {

    private final String adminUsername;
    private final String adminPassword;

    public SingleUserAuthenticationManager(String adminUsername, String adminPassword) {
        this.adminUsername = adminUsername;
        this.adminPassword = adminPassword;
    }

    @Override
    public AuthenticatedUser login(String username, String password) {
        if (adminUsername.equalsIgnoreCase(username) && adminPassword.equals(password)) {
            return new AuthenticatedUser(username, UserRole.ADMINISTRATOR);
        }
        return null;
    }

}

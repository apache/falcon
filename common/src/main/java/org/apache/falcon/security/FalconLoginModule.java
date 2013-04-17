/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.security;


import com.sun.security.auth.UnixPrincipal;
import org.apache.log4j.Logger;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.security.Principal;
import java.util.Map;

/**
 * Falcon JAAS login module.
 */
public class FalconLoginModule implements LoginModule {
    private static final Logger LOG = Logger.getLogger(FalconLoginModule.class);

    private Subject subject;

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    private <T extends Principal> T getCanonicalUser(Class<T> cls) {
        for (T user : subject.getPrincipals(cls)) {
            return user;
        }
        return null;
    }

    @Override
    public boolean commit() throws LoginException {
        if (!subject.getPrincipals(SecurityConstants.OS_PRINCIPAL_CLASS).
                isEmpty()) {
            return true;
        }

        Principal user = getCanonicalUser(SecurityConstants.OS_PRINCIPAL_CLASS);
        if (user != null) {
            subject.getPrincipals().add(new UnixPrincipal(user.getName()));
            return true;
        }
        LOG.error("No such user " + subject);
        throw new LoginException("No such user " + subject);
    }

    //SUSPEND CHECKSTYLE CHECK
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
    }
    //RESUME CHECKSTYLE CHECK

    @Override
    public boolean login() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }
}

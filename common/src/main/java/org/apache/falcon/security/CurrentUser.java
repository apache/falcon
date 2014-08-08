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

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Current authenticated user via REST.
 * Also doles out proxied UserGroupInformation. Caches proxied users.
 */
public final class CurrentUser {

    private static final Logger LOG = LoggerFactory.getLogger(CurrentUser.class);

    private static final CurrentUser INSTANCE = new CurrentUser();

    private CurrentUser() {}

    public static CurrentUser get() {
        return INSTANCE;
    }

    private final ThreadLocal<Subject> currentSubject = new ThreadLocal<Subject>();

    public static void authenticate(String user) {
        if (user == null || user.isEmpty()) {
            throw new IllegalStateException("Bad user name sent for authentication");
        }
        if (user.equals(getUserInternal())) {
            return;
        }

        Subject subject = new Subject();
        subject.getPrincipals().add(new FalconPrincipal(user));
        LOG.info("Logging in {}", user);
        INSTANCE.currentSubject.set(subject);
    }

    public static Subject getSubject() {
        return INSTANCE.currentSubject.get();
    }

    public static String getUser() {
        String user = getUserInternal();
        if (user == null) {
            throw new IllegalStateException("No user logged into the system");
        } else {
            return user;
        }
    }

    private static String getUserInternal() {
        Subject subject = getSubject();
        if (subject == null) {
            return null;
        } else {
            for (FalconPrincipal principal : subject.
                    getPrincipals(FalconPrincipal.class)) {
                return principal.getName();
            }
            return null;
        }
    }

    private static ConcurrentMap<String, UserGroupInformation> userUgiMap =
            new ConcurrentHashMap<String, UserGroupInformation>();

    /**
     * Dole out a proxy UGI object for the current authenticated user.
     *
     * @return UGI object
     * @throws java.io.IOException
     */
    public static UserGroupInformation getProxyUgi() throws IOException {
        String proxyUser = getUser();

        UserGroupInformation proxyUgi = userUgiMap.get(proxyUser);
        if (proxyUgi == null) {
            // taking care of a race condition, the latest UGI will be discarded
            proxyUgi = UserGroupInformation
                    .createProxyUser(proxyUser, UserGroupInformation.getLoginUser());
            userUgiMap.putIfAbsent(proxyUser, proxyUgi);
        }

        return proxyUgi;
    }

    public static Set<String> getGroupNames() throws IOException {
        HashSet<String> s = new HashSet<String>(Arrays.asList(getProxyUgi().getGroupNames()));
        return Collections.unmodifiableSet(s);
    }
}

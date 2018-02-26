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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.service.ProxyUserService;
import org.apache.falcon.service.Services;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Current authenticated user via REST. Also captures the proxy user from authorized entity
 * and doles out proxied UserGroupInformation. Caches proxied users.
 */
public final class CurrentUser {

    private static final Logger LOG = LoggerFactory.getLogger(CurrentUser.class);
    private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

    private final String authenticatedUser;
    private String proxyUser;

    private CurrentUser(String authenticatedUser) {
        this.authenticatedUser = authenticatedUser;
        this.proxyUser = authenticatedUser;
    }

    private static final ThreadLocal<CurrentUser> CURRENT_USER = new ThreadLocal<CurrentUser>();

    /**
     * Captures the authenticated user.
     *
     * @param user   authenticated user
     */
    public static void authenticate(final String user) {
        if (StringUtils.isEmpty(user)) {
            throw new IllegalStateException("Bad user name sent for authentication");
        }

        LOG.info("Logging in {}", user);
        CurrentUser currentUser = new CurrentUser(user);
        CURRENT_USER.set(currentUser);
    }

    /**
     * Proxies doAs user.
     *
     * @param doAsUser doAs user
     * @param proxyHost proxy host
     */
    public static void proxyDoAsUser(final String doAsUser, final String proxyHost) {
        if (!isAuthenticated()) {
            throw new IllegalStateException("Authentication not done");
        }

        String currentUser = CURRENT_USER.get().authenticatedUser;
        if (StringUtils.isNotEmpty(doAsUser) && !doAsUser.equalsIgnoreCase(currentUser)) {
            if (StringUtils.isEmpty(proxyHost)) {
                throw new IllegalArgumentException("proxy host cannot be null or empty");
            }
            ProxyUserService proxyUserService = Services.get().getService(ProxyUserService.SERVICE_NAME);
            try {
                proxyUserService.validate(currentUser, proxyHost, doAsUser);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            CurrentUser user = CURRENT_USER.get();
            LOG.info("Authenticated user {} is proxying doAs user {} from host {}",
                    user.authenticatedUser, doAsUser, proxyHost);
            AUDIT.info("Authenticated user {} is proxying doAs user {} from host {}",
                    user.authenticatedUser, doAsUser, proxyHost);
            user.proxyUser = doAsUser;
        }
    }

    /**
     * Captures the entity owner if authenticated user is a super user.
     *
     * @param aclOwner entity acl owner
     * @param aclGroup entity acl group
     */
    public static void proxy(final String aclOwner, final String aclGroup) {
        if (!isAuthenticated() || StringUtils.isEmpty(aclOwner)) {
            throw new IllegalStateException("Authentication not done or Bad user name");
        }

        CurrentUser user = CURRENT_USER.get();
        LOG.info("Authenticated user {} is proxying entity owner {}/{}",
                user.authenticatedUser, aclOwner, aclGroup);
        AUDIT.info("Authenticated user {} is proxying entity owner {}/{}",
                user.authenticatedUser, aclOwner, aclGroup);
        user.proxyUser = aclOwner;
    }

    /**
     * Clears the context.
     */
    public static void clear() {
        CURRENT_USER.remove();
    }

    /**
     * Checks if the authenticate method is already called.
     *
     * @return true if authenticated user is set else false
     */
    public static boolean isAuthenticated() {
        CurrentUser user = CURRENT_USER.get();
        return user != null && user.authenticatedUser != null;
    }

    /**
     * Returns authenticated user.
     *
     * @return logged in authenticated user.
     */
    public static String getAuthenticatedUser() {
        CurrentUser user = CURRENT_USER.get();
        if (user == null || user.authenticatedUser == null) {
            throw new IllegalStateException("No user logged into the system");
        } else {
            return user.authenticatedUser;
        }
    }

    /**
     * Dole out a UGI object for the current authenticated user if authenticated
     * else return current user.
     *
     * @return UGI object
     * @throws java.io.IOException
     */
    public static UserGroupInformation getAuthenticatedUGI() throws IOException {
        return CurrentUser.isAuthenticated()
            ? createProxyUGI(getAuthenticatedUser()) : UserGroupInformation.getCurrentUser();
    }

    /**
     * Returns the proxy user.
     *
     * @return proxy user
     */
    public static String getUser() {
        CurrentUser user = CURRENT_USER.get();
        if (user == null || user.proxyUser == null) {
            throw new IllegalStateException("No user logged into the system");
        } else {
            return user.proxyUser;
        }
    }

    private static ConcurrentMap<String, UserGroupInformation> userUgiMap =
            new ConcurrentHashMap<String, UserGroupInformation>();

    /**
     * Create a proxy UGI object for the proxy user.
     *
     * @param proxyUser logged in user
     * @return UGI object
     * @throws IOException
     */
    public static UserGroupInformation createProxyUGI(String proxyUser) throws IOException {
        UserGroupInformation proxyUgi = userUgiMap.get(proxyUser);
        if (proxyUgi == null) {
            // taking care of a race condition, the latest UGI will be discarded
            proxyUgi = UserGroupInformation.createProxyUser(
                    proxyUser, UserGroupInformation.getLoginUser());
            userUgiMap.putIfAbsent(proxyUser, proxyUgi);
        }

        return proxyUgi;
    }

    /**
     * Dole out a proxy UGI object for the current authenticated user if authenticated
     * else return current user.
     *
     * @return UGI object
     * @throws java.io.IOException
     */
    public static UserGroupInformation getProxyUGI() throws IOException {
        return CurrentUser.isAuthenticated()
            ? createProxyUGI(getUser()) : UserGroupInformation.getCurrentUser();
    }

    /**
     * Gets a collection of group names the proxy user belongs to.
     *
     * @return group names
     * @throws IOException
     */
    public static Set<String> getGroupNames() throws IOException {
        HashSet<String> s = new HashSet<String>(Arrays.asList(getProxyUGI().getGroupNames()));
        return Collections.unmodifiableSet(s);
    }

    /**
     * Returns the primary group name for the proxy user.
     *
     * @return primary group name for the proxy user
     */
    public static String getPrimaryGroupName() {
        try {
            String[] groups = getProxyUGI().getGroupNames();
            if (groups.length > 0) {
                return groups[0];
            }
        } catch (IOException ignore) {
            // ignored
        }

        return "unknown"; // this can only happen in tests
    }
}

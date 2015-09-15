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

package org.apache.falcon.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.util.RuntimeProperties;

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessControlException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ProxyUserService checks if a user of a request has proxyuser privileges.
 * <p>
 * This check is based on the following criteria:
 * <p>
 * <ul>
 *     <li>The user of the request must be configured as proxy user in Falcon runtime properties.</li>
 *     <li>The user of the request must be making the request from a whitelisted host.</li>
 *     <li>The user of the request must be making the request on behalf of a user of a whitelisted group.</li>
 * </ul>
 * <p>
 */
public class ProxyUserService implements FalconService {
    private static final Logger LOG = LoggerFactory.getLogger(ProxyUserService.class);


    private Map<String, Set<String>> proxyUserHosts = new HashMap<>();
    private Map<String, Set<String>> proxyUserGroups = new HashMap<>();

    private static final String CONF_PREFIX = "falcon.service.ProxyUserService.proxyuser.";
    private static final String GROUPS = ".groups";
    private static final String HOSTS = ".hosts";
    public static final String SERVICE_NAME = ProxyUserService.class.getSimpleName();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    /**
     * Initializes the service.
     * @throws FalconException thrown if the service could not be configured correctly.
     */
    @Override
    public void init() throws FalconException {
        Set<Map.Entry<Object, Object>> entrySet = RuntimeProperties.get().entrySet();

        for (Map.Entry<Object, Object> entry : entrySet) {
            String key = (String) entry.getKey();

            if (key.startsWith(CONF_PREFIX) && key.endsWith(GROUPS)) {
                String proxyUser = key.substring(0, key.lastIndexOf(GROUPS));
                if (RuntimeProperties.get().getProperty(proxyUser + HOSTS) == null) {
                    throw new FalconException(CONF_PREFIX + proxyUser + HOSTS + " property not set in runtime "
                            + "properties. Please add it.");
                }
                proxyUser = proxyUser.substring(CONF_PREFIX.length());
                String value = ((String) entry.getValue()).trim();
                LOG.info("Loading proxyuser settings [{}]=[{}]", key, value);
                Set<String> values = null;
                if (!value.equals("*")) {
                    values = new HashSet<>(Arrays.asList(value.split(",")));
                }
                proxyUserGroups.put(proxyUser, values);
            }
            if (key.startsWith(CONF_PREFIX) && key.endsWith(HOSTS)) {
                String proxyUser = key.substring(0, key.lastIndexOf(HOSTS));
                if (RuntimeProperties.get().getProperty(proxyUser + GROUPS) == null) {
                    throw new FalconException(CONF_PREFIX + proxyUser + GROUPS + " property not set in runtime "
                            + "properties. Please add it.");
                }
                proxyUser = proxyUser.substring(CONF_PREFIX.length());
                String value = ((String) entry.getValue()).trim();
                LOG.info("Loading proxyuser settings [{}]=[{}]", key, value);
                Set<String> values = null;
                if (!value.equals("*")) {
                    String[] hosts = value.split(",");
                    for (int i = 0; i < hosts.length; i++) {
                        String hostName = hosts[i];
                        try {
                            hosts[i] = normalizeHostname(hostName);
                        } catch (Exception ex) {
                            throw new FalconException("Exception normalizing host name: " + hostName + "."
                                    + ex.getMessage(), ex);
                        }
                        LOG.info("Hostname, original [{}], normalized [{}]", hostName, hosts[i]);
                    }
                    values = new HashSet<>(Arrays.asList(hosts));
                }
                proxyUserHosts.put(proxyUser, values);
            }
        }
    }

    /**
     * Verifies a proxyuser.
     *
     * @param proxyUser user name of the proxy user.
     * @param proxyHost host the proxy user is making the request from.
     * @param doAsUser user the proxy user is impersonating.
     * @throws java.io.IOException thrown if an error during the validation has occurred.
     * @throws java.security.AccessControlException thrown if the user is not allowed to perform the proxyuser request.
     */
    public void validate(String proxyUser, String proxyHost, String doAsUser) throws IOException {
        validateNotEmpty(proxyUser, "proxyUser",
                "If you're attempting to use user-impersonation via a proxy user, please make sure that "
                        + "falcon.service.ProxyUserService.proxyuser.#USER#.hosts and "
                        + "falcon.service.ProxyUserService.proxyuser.#USER#.groups are configured correctly"
        );
        validateNotEmpty(proxyHost, "proxyHost",
                "If you're attempting to use user-impersonation via a proxy user, please make sure that "
                        + "falcon.service.ProxyUserService.proxyuser." + proxyUser + ".hosts and "
                        + "falcon.service.ProxyUserService.proxyuser." + proxyUser + ".groups are configured correctly"
        );
        validateNotEmpty(doAsUser, "doAsUser", null);
        LOG.debug("Authorization check proxyuser [{}] host [{}] doAs [{}]",
                proxyUser, proxyHost, doAsUser);
        if (proxyUserHosts.containsKey(proxyUser)) {
            validateRequestorHost(proxyUser, proxyHost, proxyUserHosts.get(proxyUser));
            validateGroup(proxyUser, doAsUser, proxyUserGroups.get(proxyUser));
        } else {
            throw new AccessControlException(MessageFormat.format("User [{0}] not defined as proxyuser. Please add it"
                            + " to runtime properties.", proxyUser));
        }
    }

    private void validateRequestorHost(String proxyUser, String hostname, Set<String> validHosts)
        throws IOException {
        if (validHosts != null) {
            if (!validHosts.contains(hostname) && !validHosts.contains(normalizeHostname(hostname))) {
                throw new AccessControlException(MessageFormat.format("Unauthorized host [{0}] for proxyuser [{1}]",
                        hostname, proxyUser));
            }
        }
    }

    private void validateGroup(String proxyUser, String user, Set<String> validGroups) throws IOException {
        if (validGroups != null) {
            List<String> userGroups =  Services.get().<GroupsService>getService(GroupsService.SERVICE_NAME)
            .getGroups(user);
            for (String g : validGroups) {
                if (userGroups.contains(g)) {
                    return;
                }
            }
            throw new AccessControlException(
                    MessageFormat.format("Unauthorized proxyuser [{0}] for user [{1}], not in proxyuser groups",
                            proxyUser, user));
        }
    }

    private String normalizeHostname(String name) {
        try {
            InetAddress address = InetAddress.getByName(name);
            return address.getCanonicalHostName();
        }  catch (IOException ex) {
            throw new AccessControlException(MessageFormat.format("Could not resolve host [{0}], [{1}]", name,
                    ex.getMessage()));
        }
    }

    private static void validateNotEmpty(String str, String name, String info) {
        if (StringUtils.isBlank(str)) {
            throw new IllegalArgumentException(name + " cannot be null or empty" + (info == null ? "" : ", " + info));
        }
    }

    /**
     * Destroys the service.
     */
    @Override
    public void destroy() {
    }

}

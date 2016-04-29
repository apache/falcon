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
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Security Util - bunch of security related helper methods.
 */
public final class SecurityUtil {

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String CONFIG_PREFIX = "falcon.authentication.";

    /**
     * Constant for the configuration property that indicates the authentication type.
     */
    public static final String AUTHENTICATION_TYPE = CONFIG_PREFIX + "type";

    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    /**
     * Constant for the configuration property that indicates the
     * Resource Manager principal.   This is useful when the remote cluster realm
     * (with cross domain trust) or the auth to local rule definition results in a
     * different RM principal than in Falcon server cluster.
     */
    public static final String RM_PRINCIPAL = "yarn.resourcemanager.principal";
    /**
     * Constant for the configuration property that indicates the Name node principal.
     * This is used to talk to Hive Meta Store during parsing and validations only.
     */
    public static final String HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";

    public static final String METASTORE_USE_THRIFT_SASL = "hive.metastore.sasl.enabled";

    public static final String METASTORE_PRINCIPAL = "hcat.metastore.principal";

    private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

    private SecurityUtil() {
    }

    public static String getAuthenticationType() {
        return StartupProperties.get().getProperty(
                AUTHENTICATION_TYPE, PseudoAuthenticationHandler.TYPE);
    }

    /**
     * Checks if kerberos authentication is enabled in the configuration.
     *
     * @return true if falcon.authentication.type is kerberos, false otherwise
     */
    public static boolean isSecurityEnabled() {
        String authenticationType = StartupProperties.get().getProperty(
                AUTHENTICATION_TYPE, PseudoAuthenticationHandler.TYPE);

        final boolean useKerberos;
        if (authenticationType == null || PseudoAuthenticationHandler.TYPE.equals(authenticationType)) {
            useKerberos = false;
        } else if (KerberosAuthenticationHandler.TYPE.equals(authenticationType)) {
            useKerberos = true;
        } else {
            throw new IllegalArgumentException("Invalid attribute value for "
                    + AUTHENTICATION_TYPE + " of " + authenticationType);
        }

        return useKerberos;
    }

    public static String getLocalHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    /**
     * Checks if authorization is enabled in the configuration.
     *
     * @return true if falcon.security.authorization.enabled is enabled, false otherwise
     */
    public static boolean isAuthorizationEnabled() {
        return Boolean.valueOf(StartupProperties.get().getProperty(
                "falcon.security.authorization.enabled", "false"));
    }

    public static AuthorizationProvider getAuthorizationProvider() throws FalconException {
        String providerClassName = StartupProperties.get().getProperty(
                "falcon.security.authorization.provider",
                "org.apache.falcon.security.DefaultAuthorizationProvider");
        return ReflectionUtils.getInstanceByClassName(providerClassName);
    }

    public static void tryProxy(Entity entity, final String doAsUser) throws IOException, FalconException {
        if (entity != null && entity.getACL() != null && SecurityUtil.isAuthorizationEnabled()) {
            final String aclOwner = entity.getACL().getOwner();
            final String aclGroup = entity.getACL().getGroup();

            if (StringUtils.isNotEmpty(doAsUser)) {
                if (!doAsUser.equalsIgnoreCase(aclOwner)) {
                    LOG.warn("doAs user {} not same as acl owner {}. Ignoring acl owner.", doAsUser, aclOwner);
                    throw new FalconException("doAs user and ACL owner mismatch. doAs user " + doAsUser
                            +  " should be same as ACL owner " + aclOwner);
                }
                return;
            }
            if (SecurityUtil.getAuthorizationProvider().shouldProxy(
                    CurrentUser.getAuthenticatedUGI(), aclOwner, aclGroup)) {
                CurrentUser.proxy(aclOwner, aclGroup);
            }
        }
    }

}

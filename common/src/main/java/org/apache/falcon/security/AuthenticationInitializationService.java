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
import org.apache.commons.lang.Validate;
import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;


/**
 * Authentication Service at startup that initializes the authentication credentials
 * based on authentication type. If Kerberos is enabled, it logs in the user with the key tab.
 */
public class AuthenticationInitializationService implements FalconService {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationInitializationService.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    protected static final String CONFIG_PREFIX = "falcon.service.authentication.";

    /**
     * Constant for the configuration property that indicates the keytab file path.
     */
    protected static final String KERBEROS_KEYTAB = CONFIG_PREFIX + KerberosAuthenticationHandler.KEYTAB;

    /**
     * Constant for the configuration property that indicates the kerberos principal.
     */
    protected static final String KERBEROS_PRINCIPAL = CONFIG_PREFIX + KerberosAuthenticationHandler.PRINCIPAL;

    /**
     * Constant for the configuration property that indicates the authentication token validity time in seconds.
     */
    protected static final String AUTH_TOKEN_VALIDITY_SECONDS = CONFIG_PREFIX + "token.validity";
    private static UserGroupInformation loginUser;

    private Timer timer = new Timer();
    private static final String SERVICE_NAME = "Authentication initialization service";
    private static final long DEFAULT_VALIDATE_FREQUENCY_SECS = 86300;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {

        if (SecurityUtil.isSecurityEnabled()) {
            LOG.info("Falcon Kerberos Authentication Enabled!");
            initializeKerberos();

            String authTokenValidity = StartupProperties.get().getProperty(AUTH_TOKEN_VALIDITY_SECONDS);
            long validateFrequency;
            try {
                // -100 so that revalidation is done before expiry.
                validateFrequency = (StringUtils.isNotEmpty(authTokenValidity))
                        ? (Long.parseLong(authTokenValidity) - 100) : DEFAULT_VALIDATE_FREQUENCY_SECS;
                if (validateFrequency < 0) {
                    throw new NumberFormatException("Value provided for startup property \""
                            + AUTH_TOKEN_VALIDITY_SECONDS + "\" should be greater than 100.");
                }
            } catch (NumberFormatException nfe) {
                throw new FalconException("Invalid value provided for startup property \""
                        + AUTH_TOKEN_VALIDITY_SECONDS + "\", please provide a valid long number", nfe);
            }
            timer.schedule(new TokenValidationThread(), 0, validateFrequency*1000);
        } else {
            LOG.info("Falcon Simple Authentication Enabled!");
            Configuration ugiConf = new Configuration();
            ugiConf.set("hadoop.security.authentication", "simple");
            UserGroupInformation.setConfiguration(ugiConf);
        }
    }

    protected static void initializeKerberos() throws FalconException {
        try {
            Properties configuration = StartupProperties.get();
            String principal = configuration.getProperty(KERBEROS_PRINCIPAL);
            Validate.notEmpty(principal,
                    "Missing required configuration property: " + KERBEROS_PRINCIPAL);
            principal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(
                    principal, SecurityUtil.getLocalHostName());

            String keytabFilePath = configuration.getProperty(KERBEROS_KEYTAB);
            Validate.notEmpty(keytabFilePath,
                    "Missing required configuration property: " + KERBEROS_KEYTAB);
            checkIsReadable(keytabFilePath);

            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "kerberos");

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabFilePath);

            LOG.info("Got Kerberos ticket, keytab: {}, Falcon principal: {}", keytabFilePath, principal);
            loginUser = UserGroupInformation.getLoginUser();
        } catch (Exception ex) {
            throw new FalconException("Could not initialize " + SERVICE_NAME
                    + ": " + ex.getMessage(), ex);
        }
    }

    private static void checkIsReadable(String keytabFilePath) {
        File keytabFile = new File(keytabFilePath);
        if (!keytabFile.exists()) {
            throw new IllegalArgumentException("The keytab file does not exist! " + keytabFilePath);
        }

        if (!keytabFile.isFile()) {
            throw new IllegalArgumentException("The keytab file cannot be a directory! " + keytabFilePath);
        }

        if (!keytabFile.canRead()) {
            throw new IllegalArgumentException("The keytab file is not readable! " + keytabFilePath);
        }
    }

    @Override
    public void destroy() throws FalconException {
        timer.cancel();
    }

    public static UserGroupInformation getLoginUser() {
        return loginUser;
    }

    private static class TokenValidationThread extends TimerTask {
        @Override
        public void run() {
            try {
                LOG.debug("Revalidating Auth Token at : {} with auth method {}", new Date(),
                        UserGroupInformation.getLoginUser().getAuthenticationMethod().name());

                // Relogin does not work in Hadoop 2.6 or 2.7 as isKeyTab check returns false
                // UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
                // Do a regular loginUserFromKeytab, till the issue is addressed.
                initializeKerberos();
            } catch (Throwable t) {
                LOG.error("Error in Auth Token revalidation task: ", t);
                GenericAlert.initializeKerberosFailed("Exception in Auth Token revalidation : ", t);
            }
        }
    }
}

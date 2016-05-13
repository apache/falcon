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

import org.apache.falcon.FalconException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Helper class for Hadoop credential provider functionality. Reflection to used to avoid
 * directly referencing the classes and methods so that version dependency is not introduced
 * as the Hadoop credential provider is only introduced in 2.6.0 and later.
 */

public final class CredentialProviderHelper {

    private static final Logger LOG = LoggerFactory.getLogger(CredentialProviderHelper.class);

    private static Class<?> clsCredProvider;
    private static Class<?> clsCredProviderFactory;
    private static Method methGetPassword;
    private static Method methCreateCredEntry;
    private static Method methFlush;
    private static Method methGetProviders;

    public static final String CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

    static {
        try {
            LOG.debug("Reflecting credential provider classes and methods");
            clsCredProvider = Class.forName("org.apache.hadoop.security.alias.CredentialProvider");
            clsCredProviderFactory = Class.forName("org.apache.hadoop.security.alias.CredentialProviderFactory");
            methCreateCredEntry = clsCredProvider.getMethod("createCredentialEntry", String.class, char[].class);
            methFlush = clsCredProvider.getMethod("flush");
            methGetPassword = Configuration.class.getMethod("getPassword", String.class);
            methGetProviders = clsCredProviderFactory.getMethod("getProviders", new Class[] { Configuration.class });
            LOG.debug("Found CredentialProviderFactory#getProviders");
        } catch (ClassNotFoundException | NoSuchMethodException cnfe) {
            LOG.debug("Ignoring exception", cnfe);
        }
    }

    private CredentialProviderHelper() {

    }

    public static boolean isProviderAvailable() {
        return !(clsCredProvider == null
                || clsCredProviderFactory == null
                || methCreateCredEntry == null
                || methGetPassword == null
                || methFlush == null);
    }

    public static String resolveAlias(Configuration conf, String alias) throws FalconException {
        try {
            char[] cred = (char[]) methGetPassword.invoke(conf, alias);
            if (cred == null) {
                throw new FalconException("The provided alias cannot be resolved");
            }
            return new String(cred);
        } catch (InvocationTargetException ite) {
            throw new FalconException("Error resolving password "
                    + " from the credential providers ", ite.getTargetException());
        } catch (IllegalAccessException iae) {
            throw new FalconException("Error invoking the credential provider method", iae);
        }
    }

    public static void createCredentialEntry(Configuration conf, String alias, String credential)
        throws FalconException {
        if (!isProviderAvailable()) {
            throw new FalconException("CredentialProvider facility not available in the hadoop environment");
        }

        try {
            List<?> result = (List<?>) methGetProviders.invoke(null, new Object[] { conf });
            Object provider = result.get(0);
            LOG.debug("Using credential provider " + provider);

            methCreateCredEntry.invoke(provider, new Object[] { alias, credential.toCharArray() });
            methFlush.invoke(provider, new Object[] {});
        } catch (InvocationTargetException ite) {
            throw new FalconException(
                    "Error creating credential entry using the credential provider", ite.getTargetException());
        } catch (IllegalAccessException iae) {
            throw new FalconException("Error accessing the credential create method", iae);
        }
    }
}

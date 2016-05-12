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

package org.apache.falcon.util;

import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.CredentialProviderHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Properties read during application startup.
 */
public final class StartupProperties extends ApplicationProperties {

    public static final String SAFEMODE_PROPERTY = "falcon.safeMode";
    private static final String SAFEMODE_FILE = ".safemode";
    private static final String CONFIGSTORE_PROPERTY = "config.store.uri";
    private static final String CREDENTIAL_PROVIDER_PROPERTY = "hadoop.security.credential.provider.path";
    private static final String ALIAS_PROPERTY_PREFIX = "hadoop.security.alias.";

    private static FileSystem fileSystem;
    private static Path storePath;

    private static final String PROPERTY_FILE = "startup.properties";
    private static final Logger LOG = LoggerFactory.getLogger(StartupProperties.class);

    private static final AtomicReference<StartupProperties> INSTANCE =
            new AtomicReference<StartupProperties>();

    private StartupProperties() throws FalconException {
        super();
        resolveAlias();
    }

    private void resolveAlias() throws FalconException {
        try {
            final Configuration conf = new Configuration();
            String providerPath = getProperty(CREDENTIAL_PROVIDER_PROPERTY);
            if (providerPath != null) {
                conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH, providerPath);
            }

            Properties aliasProperties = new Properties();
            for (Object keyObj : keySet()) {
                String key = (String) keyObj;
                if (key.startsWith(ALIAS_PROPERTY_PREFIX)) {
                    String propertyKey = key.substring(ALIAS_PROPERTY_PREFIX.length());
                    String propertyValue = CredentialProviderHelper.resolveAlias(conf, getProperty(key));
                    aliasProperties.setProperty(propertyKey, propertyValue);
                }
            }
            LOG.info("Resolved alias properties: {}", aliasProperties.stringPropertyNames());
            putAll(aliasProperties);
        } catch (Exception e) {
            LOG.error("Exception while resolving credential alias", e);
            throw new FalconException("Exception while resolving credential alias", e);
        }
    }

    @Override
    protected String getPropertyFile() {
        return PROPERTY_FILE;
    }

    public static Properties get() {
        try {
            if (INSTANCE.get() == null) {
                INSTANCE.compareAndSet(null, new StartupProperties());
                storePath = new Path((INSTANCE.get().getProperty(CONFIGSTORE_PROPERTY)));
                fileSystem = HadoopClientFactory.get().createFalconFileSystem(storePath.toUri());
                String isSafeMode = (doesSafemodeFileExist()) ? "true" : "false";
                LOG.info("Initializing Falcon StartupProperties with safemode set to {}.", isSafeMode);
                INSTANCE.get().setProperty(SAFEMODE_PROPERTY, isSafeMode);
            }
            return INSTANCE.get();
        } catch (FalconException e) {
            throw new RuntimeException("Unable to read application startup properties", e);
        } catch (IOException e) {
            throw new RuntimeException("Unable to verify Falcon safemode", e);
        }
    }

    public static void createSafemodeFile() throws IOException {
        Path safemodeFilePath = getSafemodeFilePath();
        if (!doesSafemodeFileExist()) {
            boolean success = fileSystem.createNewFile(safemodeFilePath);
            if (!success) {
                LOG.error("Failed to create safemode file at {}", safemodeFilePath.toUri());
                throw new IOException("Failed to create safemode file at " + safemodeFilePath.toUri());
            }
        }
        INSTANCE.get().setProperty(SAFEMODE_PROPERTY, "true");
    }

    public static boolean deleteSafemodeFile() throws IOException {
        INSTANCE.get().setProperty(SAFEMODE_PROPERTY, "false");
        return !doesSafemodeFileExist() || fileSystem.delete(getSafemodeFilePath(), true);
    }

    public static boolean doesSafemodeFileExist() throws IOException {
        return fileSystem.exists(getSafemodeFilePath());
    }

    private static Path getSafemodeFilePath() {
        return new Path(storePath, SAFEMODE_FILE);
    }

    public static boolean isServerInSafeMode() {
        return Boolean.parseBoolean(StartupProperties.get().getProperty(StartupProperties.SAFEMODE_PROPERTY, "false"));
    }

}

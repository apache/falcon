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
            }
            return INSTANCE.get();
        } catch (FalconException e) {
            throw new RuntimeException("Unable to read application startup properties", e);
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

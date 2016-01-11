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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.expression.ExpressionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Properties for State Store during application startup.
 */
public final class StateStoreProperties extends ApplicationProperties {

    private static final Logger LOG = LoggerFactory.getLogger(StateStoreProperties.class);

    private static final String PROPERTY_FILE = "statestore.properties";
    private static final String CREDENTIALS_FILE= "falcon.statestore.credentials.file";
    private static final String DEFAULT_CREDENTIALS_FILE = "statestore.credentials";

    private static final AtomicReference<StateStoreProperties> INSTANCE =
            new AtomicReference<>();


    protected StateStoreProperties() throws FalconException {
        super();
    }

    @Override
    protected String getPropertyFile() {
        return PROPERTY_FILE;
    }

    @Override
    protected void loadProperties() throws FalconException {
        super.loadProperties();

        String credentialsFile = (String)get(CREDENTIALS_FILE);
        try {
            InputStream resourceAsStream = null;
            if (StringUtils.isNotBlank(credentialsFile)) {
                resourceAsStream = getResourceAsStream(new File(credentialsFile));
            }
            // fall back to class path.
            if (resourceAsStream == null) {
                resourceAsStream = checkClassPath(DEFAULT_CREDENTIALS_FILE);
            }
            if (resourceAsStream != null) {
                try {
                    loadCredentials(resourceAsStream);
                    return;
                } finally {
                    IOUtils.closeQuietly(resourceAsStream);
                }
            } else {
                throw new FalconException("Unable to find state store credentials file");
            }
        } catch (IOException e) {
            throw new FalconException("Error loading properties file: " + getPropertyFile(), e);
        }
    }

    private void loadCredentials(InputStream resourceAsStream) throws IOException {
        Properties origProps = new Properties();
        origProps.load(resourceAsStream);
        LOG.info("Initializing {} properties with domain {}", this.getClass().getName(), domain);
        Set<String> keys = getKeys(origProps.keySet());
        for (String key : keys) {
            String value = origProps.getProperty(domain + "." + key, origProps.getProperty("*." + key));
            if (value != null) {
                value = ExpressionHelper.substitute(value);
                LOG.debug("{}={}", key, value);
                put(key, value);
            }
        }
    }


    public static Properties get() {
        try {
            if (INSTANCE.get() == null) {
                INSTANCE.compareAndSet(null, new StateStoreProperties());
            }
            return INSTANCE.get();
        } catch (FalconException e) {
            throw new RuntimeException("Unable to read application state store properties", e);
        }
    }
}

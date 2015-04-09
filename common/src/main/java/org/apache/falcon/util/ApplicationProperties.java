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
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.expression.ExpressionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for reading application properties.
 */
public abstract class ApplicationProperties extends Properties {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    protected abstract String getPropertyFile();

    private String domain;

    protected ApplicationProperties() throws FalconException {
        init();
    }

    protected void init() throws FalconException {
        setDomain(System.getProperty("falcon.domain", System.getenv("FALCON_DOMAIN")));
        loadProperties();
    }

    protected void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDomain() {
        return domain;
    }

    protected void loadProperties() throws FalconException {
        String propertyFileName = getPropertyFile();
        String confDir = System.getProperty("config.location");
        loadProperties(propertyFileName, confDir);
    }

    /**
     * This method reads the given properties file in the following order:
     * config.location & classpath. It falls back in that specific order.
     *
     * @throws FalconException
     */
    protected void loadProperties(String propertyFileName, String confDir) throws FalconException {
        try {
            InputStream resourceAsStream = checkConfigLocation(propertyFileName, confDir);

            //Fallback to classpath
            if (resourceAsStream == null) {
                resourceAsStream = checkClassPath(propertyFileName);
            }

            if (resourceAsStream != null) {
                try {
                    doLoadProperties(resourceAsStream);
                    return;
                } finally {
                    IOUtils.closeQuietly(resourceAsStream);
                }
            }
            throw new FileNotFoundException("Unable to find: " + propertyFileName);
        } catch (IOException e) {
            throw new FalconException("Error loading properties file: " + getPropertyFile(), e);
        }
    }

    private InputStream checkConfigLocation(String propertyFileName, String confDir)
        throws FileNotFoundException {

        InputStream resourceAsStream = null;
        if (confDir != null) {
            File fileToLoad = new File(confDir, propertyFileName);
            if (fileToLoad.exists() && fileToLoad.isFile() && fileToLoad.canRead()) {
                LOG.info("config.location is set, using: {}/{}", confDir, propertyFileName);
                resourceAsStream = new FileInputStream(fileToLoad);
            }
        }
        return resourceAsStream;
    }

    private InputStream checkClassPath(String propertyFileName) {

        InputStream resourceAsStream = null;
        Class clazz = ApplicationProperties.class;
        URL resource = clazz.getResource("/" + propertyFileName);
        if (resource != null) {
            LOG.info("Fallback to classpath for: {}", resource);
            resourceAsStream = clazz.getResourceAsStream("/" + propertyFileName);
        } else {
            resource = clazz.getResource(propertyFileName);
            if (resource != null) {
                LOG.info("Fallback to classpath for: {}", resource);
                resourceAsStream = clazz.getResourceAsStream(propertyFileName);
            }
        }
        return resourceAsStream;
    }

    private void doLoadProperties(InputStream resourceAsStream) throws IOException, FalconException {
        Properties origProps = new Properties();
        origProps.load(resourceAsStream);
        if (domain == null) {
            domain = origProps.getProperty("*.domain");
            if (domain == null) {
                throw new FalconException("Domain is not set!");
            } else {
                domain = ExpressionHelper.substitute(domain);
            }
        }

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

    private Set<String> getKeys(Set<Object> keySet) {
        Set<String> keys = new HashSet<String>();
        for (Object keyObj : keySet) {
            String key = (String) keyObj;
            keys.add(key.substring(key.indexOf('.') + 1));
        }
        return keys;
    }

    @Override
    public String getProperty(String key) {
        return StringUtils.trim(super.getProperty(key));
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return StringUtils.trim(super.getProperty(key, defaultValue));
    }
}

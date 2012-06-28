/*
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

package org.apache.ivory.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.ivory.IvoryException;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.log4j.Logger;

public abstract class ApplicationProperties extends Properties {

    private static Logger LOG = Logger.getLogger(ApplicationProperties.class);

    protected enum LocationType {
        FILE, HOME, CLASSPATH
    }

    protected abstract String getPropertyFile();

    private String propertyFile;
    private LocationType location;
    private String domain;

    protected ApplicationProperties() throws IvoryException {
        initialize();
        loadProperties();
    }

    public String getDomain() {
        return domain;
    }
    
    protected void initialize() {
        String propFile = getPropertyFile();
        String userHome = System.getProperty("user.home");
        String confDir = System.getProperty("config.location");
        domain = System.getProperty("ivory.domain", System.getenv("IVORY_DOMAIN"));

        if (confDir == null && new File(userHome, propFile).exists()) {
            LOG.info("config.location is not set, and property file found in home dir" + userHome + "/" + propFile);
            location = LocationType.HOME;
            propertyFile = new File(userHome, propFile).getAbsolutePath();
        } else if (confDir != null) {
            LOG.info("config.location is set, using " + confDir + "/" + propFile);
            location = LocationType.FILE;
            propertyFile = new File(confDir, propFile).getAbsolutePath();
        } else {
            LOG.info("config.location is not set, properties file not present in " + "user home dir, falling back to classpath for "
                    + propFile);
            location = LocationType.CLASSPATH;
            propertyFile = propFile;
        }
    }

    protected void loadProperties() throws IvoryException {
        InputStream resource;
        try {
            if (location == LocationType.CLASSPATH) {
                if (getClass().getResource(propertyFile) != null) {
                    LOG.info("Property file being loaded from " +
                            getClass().getResource(propertyFile));
                    resource = getClass().getResourceAsStream(propertyFile);
                } else {
                    LOG.info("Property file being loaded from " +
                            getClass().getResource("/" + propertyFile));
                    resource = getClass().getResourceAsStream("/" + propertyFile);
                }
            } else {
                resource = new FileInputStream(propertyFile);
            }

            if (resource == null) {
                throw new FileNotFoundException(propertyFile + " not found in " + location);
            } else {
                try {
                    LOG.info("Loading properties from " + propertyFile);
                    Properties origProps = new Properties();
                    origProps.load(resource);
                    if(domain == null) {
                        domain = origProps.getProperty("*.domain");
                        if(domain == null)
                            throw new IvoryException("Domain is not set!");
                    }
                    LOG.info("Initializing properties with domain " + domain);
                    
                    Set<String> keys = getKeys(origProps.keySet());
                    for(String key:keys) {
                        String value = origProps.getProperty(domain + "." + key, origProps.getProperty("*." + key));
                        value = ExpressionHelper.substitute(value);
                        LOG.debug(key + "=" + value);
                        put(key, value);
                    }
                } finally {
                    resource.close();
                }
            }
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private Set<String> getKeys(Set<Object> keySet) {
        Set<String> keys = new HashSet<String>();
        for(Object keyObj:keySet) {
            String key = (String) keyObj;
            keys.add(key.substring(key.indexOf('.') + 1));
        }
        return keys;
    }
}

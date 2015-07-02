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

package org.apache.falcon.regression.core.util;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.log4j.Logger;
import org.testng.Assert;

/** Class for reading properties from Merlin.properties file. */
public final class Config {
    private static final Logger LOGGER = Logger.getLogger(Config.class);

    private static final String MERLIN_PROPERTIES = "Merlin.properties";
    private static final Config INSTANCE = new Config(MERLIN_PROPERTIES);

    private AbstractConfiguration confObj;
    private Config(String propFileName) {
        try {
            initConfig(propFileName);
        } catch (ConfigurationException e) {
            Assert.fail("Could not read properties because of exception: " + e);
        }
    }

    private void initConfig(String propFileName) throws ConfigurationException {
        CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
        LOGGER.info("Going to add properties from system properties.");
        compositeConfiguration.addConfiguration(new SystemConfiguration());

        LOGGER.info("Going to read properties from: " + propFileName);
        final PropertiesConfiguration merlinConfig =
            new PropertiesConfiguration(Config.class.getResource("/" + propFileName));
        //if changed configuration will be reloaded within 2 min
        final FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
        reloadingStrategy.setRefreshDelay(2 * 60 * 1000);
        merlinConfig.setReloadingStrategy(reloadingStrategy);
        compositeConfiguration.addConfiguration(merlinConfig);
        this.confObj = compositeConfiguration;
    }

    public static String getProperty(String key) {
        return INSTANCE.confObj.getString(key);
    }

    public static String[] getStringArray(String key) {
        return INSTANCE.confObj.getStringArray(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return INSTANCE.confObj.getString(key, defaultValue);
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        return INSTANCE.confObj.getBoolean(key, defaultValue);
    }

    public static int getInt(String key, int defaultValue) {
        return INSTANCE.confObj.getInt(key, defaultValue);
    }
}

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

import org.apache.ivory.expression.ExpressionHelper;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ApplicationProperties extends Properties {

  private static Logger LOG = Logger.getLogger(ApplicationProperties.class);

  protected enum LocationType {FILE, HOME, CLASSPATH}

  protected abstract String getPropertyFile();

  private String propertyFile;
  private LocationType location;

  protected ApplicationProperties() throws IOException {
    initialize();
    loadProperties();
  }

  protected void initialize() {
    String propFile = getPropertyFile();
    String userHome = System.getProperty("user.home");
    String confDir = System.getProperty("config.location");
    if (confDir == null && new File(userHome, propFile).exists()) {
      LOG.info("config.location is not set, and property file found in home dir" +
              userHome + "/" + propFile);
      location = LocationType.HOME;
      propertyFile = new File(userHome, propFile).getAbsolutePath();
    } else if (confDir != null) {
      LOG.info("config.location is set, using " + confDir + "/" + propFile);
      location = LocationType.FILE;
        propertyFile = new File(confDir, propFile).getAbsolutePath();
    } else {
      LOG.info("config.location is not set, properties file not present in " +
              "user home dir, falling back to classpath for " + propFile);
      location = LocationType.CLASSPATH;
      propertyFile = propFile;
    }
  }

  protected void loadProperties() throws IOException {
    InputStream resource;

    if (location == LocationType.CLASSPATH) {
      resource = getClass().getResourceAsStream("/" + propertyFile);
    } else {
      resource = new FileInputStream(propertyFile);
    }

    if (resource == null) {
      throw new FileNotFoundException(propertyFile +
          " not found in " + location);
    } else {
        try {
          //TODO:: Should we clear and reload? In which case we need to lock
          //TODO:: down the object.
          LOG.info("Loading properties from " + propertyFile);
          load(resource);
          for (Object key : keySet()) {
            put(key, ExpressionHelper.substitute(getProperty((String) key)));
          }
        } finally {
          resource.close();
        }
    }
  }
}

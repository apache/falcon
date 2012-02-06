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

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ApplicationProperties extends Properties {

  private static Logger LOG = Logger.getLogger(ApplicationProperties.class);

  protected enum LocationType {FILE, HOME, CLASSPATH}

  protected abstract String getPropertyFile();

  private final String propertyFile;
  private final LocationType location;

  private Pattern sysPropertyPattern = Pattern.compile("\\$\\{[A-Za-z0-9_.]+\\}");

  protected ApplicationProperties() throws IOException {
    propertyFile = getPropertyFile();
    location = getPropertyFileLocation();
    loadProperties();
  }

  protected LocationType getPropertyFileLocation() {
    String userHome = System.getProperty("user.home");
    String confDir = System.getProperty("config.location");
    if (confDir == null && new File(userHome, propertyFile).exists()) {
      LOG.info("config.location is not set, and property file found in home dir" +
              userHome + "/" + propertyFile);
      return LocationType.HOME;
    } else if (confDir != null) {
      LOG.info("config.location is set, using " + confDir + "/" + propertyFile);
      return LocationType.FILE;
    } else {
      LOG.info("config.location is not set, properties file not present in " +
              "user home dir, falling back to classpath for " + propertyFile);
      return LocationType.CLASSPATH;
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
            put(key, substitute(getProperty((String)key)));
          }
        } finally {
          resource.close();
        }
    }
  }

  private String substitute(String originalValue) {
    Matcher envVarMatcher = sysPropertyPattern.matcher(originalValue);
    while (envVarMatcher.find()) {
      String envVar = originalValue.substring(envVarMatcher.start() + 2,
          envVarMatcher.end() - 1);
      String envVal = System.getProperty(envVar, System.getenv(envVar));

      envVar = "\\$\\{" + envVar + "\\}";
      if (envVal != null) {
        originalValue = originalValue.replaceAll(envVar, envVal);
        envVarMatcher = sysPropertyPattern.matcher(originalValue);
      }
    }
    return originalValue;
  }

}

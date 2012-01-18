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

package org.apache.ivory.entity.common;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class Configuration implements Iterable<Map.Entry<String, String>>, Cloneable {

  private final Map<String, String> properties;

  public Configuration() {
    properties = new ConcurrentHashMap<String, String>();
  }

  public Configuration(Map<String, String> properties) {
    this.properties = properties;
  }

  public void addConfiguration(Configuration config) {
    for (Entry<String, String> entry : config) {
      properties.put(entry.getKey(), entry.getValue());
    }
  }

  public Configuration addAndReturnNewConfiguration(Configuration config) {
    Map<String, String> newProperties = new ConcurrentHashMap<String, String>(properties);
    for (Entry<String, String> entry : config) {
      newProperties.put(entry.getKey(), entry.getValue());
    }
    return new Configuration(newProperties);
  }

  public String getConf(String name) {
    return properties.get(name);
  }

  public void setConf(String name, String value) {
    properties.put(name, value);
  }

  public void setConf(String name, String value, String defaultValue) {
    if (value == null) {
      properties.put(name, defaultValue);
    } else {
      properties.put(name, value);
    }
  }

  @Override
  public Iterator<Entry<String, String>> iterator() {
    return properties.entrySet().iterator();
  }

}

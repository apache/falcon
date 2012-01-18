/*******************************************************************************
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
 ******************************************************************************/
package org.apache.ivory.entity.v0.datastore;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.ivory.entity.common.Configuration;
import org.apache.ivory.entity.v0.datastore.adapter.ConfigurationTypeAdapter;
import org.apache.ivory.entity.v0.datastore.adapter.InterfacesMapAdapter;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "configuration", "interfaces" })
@XmlRootElement(name = "Source")
public class Source {

  @XmlJavaTypeAdapter(ConfigurationTypeAdapter.class)
  @XmlElement(name = "Configuration")
  protected Configuration configuration;

  @XmlJavaTypeAdapter(InterfacesMapAdapter.class)
  @XmlElement(name = "Interface", required = true)
  protected Map<String, Interface> interfaces;

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration value) {
    configuration = value;
  }

  public Map<String, Interface> getInterfaces() {
    return interfaces;
  }

  public void setInterfaces(Map<String, Interface> interfaces) {
    this.interfaces = interfaces;
  }

}

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
package org.apache.ivory.entity.v0.dataset;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.ivory.entity.v0.dataset.adapter.ConsumersMapAdapter;
import org.apache.ivory.entity.v0.dataset.adapter.ParametersMapAdapter;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "owner", "consumers", "tags", "parameters" })
@XmlRootElement(name = "MetaInfo")
public class MetaInfo {

  @XmlElement(name = "Owner")
  protected Owner owner;

  @XmlJavaTypeAdapter(ConsumersMapAdapter.class)
  @XmlElement(name = "Consumer", required = true)
  protected Map<String, Consumer> consumers;

  @XmlElement(name = "Tags")
  protected String tags;

  @XmlJavaTypeAdapter(ParametersMapAdapter.class)
  @XmlElement(name = "Parameters")
  protected Map<String, String> parameters;

  public Owner getOwner() {
    return owner;
  }

  public void setOwner(Owner value) {
    owner = value;
  }

  public Map<String, Consumer> getConsumers() {
    return consumers;
  }

  public void setConsumers(Map<String, Consumer> consumers) {
    this.consumers = consumers;
  }

  public String getTags() {
    return tags;
  }

  public void setTags(String value) {
    tags = value;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(final Map<String, String> value) {
    parameters = value;
  }

  public void addParameter(final String name, final String value) {
    parameters.put(name, value);
  }

}

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
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.ivory.entity.v0.dataset.adapter.DatastoresMapAdapter;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "comments", "status", "priority", "type", "metaInfo", "partition",
    "sourceDataDefinition", "defaults", "datastores", "dataFlow", "customCode" })
@XmlRootElement(name = "Dataset")
public class Dataset {

  @XmlElement(name = "Comments")
  protected String comments;

  @XmlElement(name = "Status")
  protected String status;

  @XmlElement(name = "Priority")
  protected String priority;

  @XmlElement(name = "Type")
  protected String type;

  @XmlElement(name = "MetaInfo", required = true)
  protected MetaInfo metaInfo;

  @XmlElement(name = "Partition", required = true)
  protected Partition partition;

  @XmlElement(name = "SourceDataDefinition")
  protected SourceDataDefinition sourceDataDefinition;

  @XmlElement(name = "Defaults")
  protected Defaults defaults;

  @XmlJavaTypeAdapter(DatastoresMapAdapter.class)
  @XmlElement(name = "Datastore", required = true)
  protected Map<String, Datastore> datastores;

  @XmlElement(name = "DataFlow", required = true)
  protected DataFlow dataFlow;

  @XmlElement(name = "CustomCode")
  protected CustomCode customCode;

  @XmlAttribute(required = true)
  protected String name;

  @XmlAttribute(required = true)
  protected String catalog;

  @XmlAttribute(required = true)
  protected String description;

  @XmlAttribute(required = true)
  protected String gdmversion;

  public String getComments() {
    return comments;
  }

  public void setComments(String value) {
    comments = value;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String value) {
    status = value;
  }

  public String getPriority() {
    return priority;
  }

  public void setPriority(String value) {
    priority = value;
  }

  public String getType() {
    return type;
  }

  public void setType(String value) {
    type = value;
  }

  public MetaInfo getMetaInfo() {
    return metaInfo;
  }

  public void setMetaInfo(MetaInfo value) {
    metaInfo = value;
  }

  public Partition getPartition() {
    return partition;
  }

  public void setPartition(Partition value) {
    partition = value;
  }

  public SourceDataDefinition getSourceDataDefinition() {
    return sourceDataDefinition;
  }

  public void setSourceDataDefinition(SourceDataDefinition value) {
    sourceDataDefinition = value;
  }

  public Defaults getDefaults() {
    return defaults;
  }

  public void setDefaults(Defaults value) {
    defaults = value;
  }

  public Map<String, Datastore> getDatastores() {
    return datastores;
  }

  public void setDatastores(Map<String, Datastore> datastores) {
    this.datastores = datastores;
  }

  public DataFlow getDataFlow() {
    return dataFlow;
  }

  public void setDataFlow(DataFlow value) {
    dataFlow = value;
  }

  public CustomCode getCustomCode() {
    return customCode;
  }

  public void setCustomCode(CustomCode value) {
    customCode = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String value) {
    name = value;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String value) {
    catalog = value;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String value) {
    description = value;
  }

  public String getGdmversion() {
    return gdmversion;
  }

  public void setGdmversion(String value) {
    gdmversion = value;
  }

}

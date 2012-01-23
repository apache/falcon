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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "dataOutputFormat", "sortKey", "partitionKey" })
@XmlRootElement(name = "Transform")
public class Transform {

  @XmlElement(name = "DataOutputFormat")
  protected String dataOutputFormat;

  @XmlElement(name = "SortKey")
  protected String sortKey;

  @XmlElement(name = "PartitionKey")
  protected String partitionKey;

  @XmlAttribute
  protected Integer numMaps;

  @XmlAttribute
  protected Integer numReduce;

  public String getDataOutputFormat() {
    return dataOutputFormat;
  }

  public void setDataOutputFormat(String value) {
    dataOutputFormat = value;
  }

  public String getSortKey() {
    return sortKey;
  }

  public void setSortKey(String value) {
    sortKey = value;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String value) {
    partitionKey = value;
  }

  public Integer getNumMaps() {
    return numMaps;
  }

  public void setNumMaps(Integer value) {
    numMaps = value;
  }

  public Integer getNumReduce() {
    return numReduce;
  }

  public void setNumReduce(Integer value) {
    numReduce = value;
  }

}

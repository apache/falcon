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
@XmlType(propOrder = { "subPartition" })
@XmlRootElement(name = "Partition")
public class Partition {

  @XmlElement(name = "SubPartition")
  protected SubPartition subPartition;

  @XmlAttribute(required = true)
  protected String name;

  @XmlAttribute(required = true)
  protected String type;

  @XmlAttribute(required = true)
  protected String format;

  public SubPartition getSubPartition() {
    return subPartition;
  }

  public void setSubPartition(SubPartition value) {
    subPartition = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String value) {
    name = value;
  }

  public String getType() {
    return type;
  }

  public void setType(String value) {
    type = value;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String value) {
    format = value;
  }

}

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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.ivory.entity.v0.Entity;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "comments", "source", "sink" })
@XmlRootElement(name = "Datastore")
public class Datastore extends Entity{

  @XmlElement(name = "Comments")
  protected String comments;

  @XmlElement(name = "Source")
  protected Source source;

  @XmlElement(name = "Sink")
  protected Sink sink;

  @XmlAttribute(required = true)
  protected String name;

  @XmlAttribute(required = true)
  protected String type;

  @XmlAttribute(required = true)
  protected String colo;

  @XmlAttribute(required = true)
  protected String description;

  public String getComments() {
    return comments;
  }

  public void setComments(String value) {
    comments = value;
  }

  public Source getSource() {
    return source;
  }

  public void setSource(Source value) {
    source = value;
  }

  public Sink getSink() {
    return sink;
  }

  public void setSink(Sink value) {
    sink = value;
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

  public String getColo() {
    return colo;
  }

  public void setColo(String value) {
    colo = value;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String value) {
    description = value;
  }

}

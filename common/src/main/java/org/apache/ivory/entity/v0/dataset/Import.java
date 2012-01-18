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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "discovery", "source", "target", "load", "transform" })
@XmlRootElement(name = "Import")
public class Import {

  @XmlElement(name = "Discovery", required = true)
  protected Discovery discovery;

  @XmlElement(name = "Source", required = true)
  protected Source source;

  @XmlElement(name = "Target", required = true)
  protected List<Target> target;

  @XmlElement(name = "Load")
  protected Load load;

  @XmlElement(name = "Transform")
  protected Transform transform;

  public Discovery getDiscovery() {
    return discovery;
  }

  public void setDiscovery(Discovery value) {
    discovery = value;
  }

  public Source getSource() {
    return source;
  }

  public void setSource(Source value) {
    source = value;
  }

  public List<Target> getTarget() {
    if (target == null) {
      target = new ArrayList<Target>();
    }
    return target;
  }

  public Load getLoad() {
    return load;
  }

  public void setLoad(Load value) {
    load = value;
  }

  public Transform getTransform() {
    return transform;
  }

  public void setTransform(Transform value) {
    transform = value;
  }

}

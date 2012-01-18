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
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Discovery")
public class Discovery {

  @XmlAttribute(name = "interface", required = true)
  protected String _interface;

  @XmlAttribute(required = true)
  protected String start;

  @XmlAttribute(required = true)
  protected String frequency;

  public String getInterface() {
    return this._interface;
  }

  public void setInterface(String value) {
    this._interface = value;
  }

  public String getStart() {
    return this.start;
  }

  public void setStart(String value) {
    this.start = value;
  }

  public String getFrequency() {
    return this.frequency;
  }

  public void setFrequency(String value) {
    this.frequency = value;
  }

}

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
import javax.xml.bind.annotation.XmlSchemaType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Owner")
public class Owner {

  @XmlAttribute(required = true)
  @XmlSchemaType(name = "anySimpleType")
  protected String name;

  @XmlAttribute(name = "BU", required = true)
  @XmlSchemaType(name = "anySimpleType")
  protected String bu;

  @XmlAttribute(name = "admin-email", required = true)
  @XmlSchemaType(name = "anySimpleType")
  protected String adminEmail;

  @XmlAttribute(name = "user-email", required = true)
  @XmlSchemaType(name = "anySimpleType")
  protected String userEmail;

  public String getName() {
    return this.name;
  }

  public void setName(String value) {
    this.name = value;
  }

  public String getBU() {
    return this.bu;
  }

  public void setBU(String value) {
    this.bu = value;
  }

  public String getAdminEmail() {
    return this.adminEmail;
  }

  public void setAdminEmail(String value) {
    this.adminEmail = value;
  }

  public String getUserEmail() {
    return this.userEmail;
  }

  public void setUserEmail(String value) {
    this.userEmail = value;
  }

}

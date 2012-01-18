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
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Proxy")
public class Proxy {

  public enum Type {
    /**
     * Represents a direct connection, or the absence of a proxy.
     */
    DIRECT,
    /**
     * Represents proxy for high level protocols such as HTTP or FTP.
     */
    HTTP
  };

  public final static Proxy NO_PROXY = new Proxy();

  @XmlAttribute(required = true)
  protected String host;

  @XmlAttribute(required = true)
  protected Integer port;

  @XmlAttribute(required = true)
  protected Type type;

  public Proxy() {
    type = Type.DIRECT;
    host = null;
    port = -1;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String value) {
    host = value;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer value) {
    port = value;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type value) {
    type = value;
  }

}

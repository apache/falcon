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
package org.apache.ivory.entity.v0.cluster.adapter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfaces;
import org.apache.ivory.entity.v0.cluster.Interfacetype;

public class InterfacesMapAdapter extends XmlAdapter<Interfaces, Map<Interfacetype, Interface>> {

  @Override
  public Interfaces marshal(Map<Interfacetype, Interface> v) throws Exception {
    if (v == null) return null;
    Interfaces interfaces = new Interfaces();
    List<Interface> pathsList = interfaces.getInterface();
    for (Interface _interface : v.values()) {
      pathsList.add(_interface);
    }
    return interfaces;
  }

  @Override
  public Map<Interfacetype, Interface> unmarshal(Interfaces interfaces) throws Exception {
    if (interfaces == null) return null;
    Map<Interfacetype, Interface> map = new HashMap<Interfacetype, Interface>();
    for (Interface _interface : interfaces.getInterface()) {
      map.put(_interface.getType(), _interface);
    }
    return map;
  }

}

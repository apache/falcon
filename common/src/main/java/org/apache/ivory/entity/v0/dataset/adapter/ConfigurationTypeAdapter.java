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
/*
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
 */

package org.apache.ivory.entity.v0.dataset.adapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.ivory.entity.v0.dataset.Configuration;
import org.apache.ivory.entity.v0.datastore.Property;

public class ConfigurationTypeAdapter extends
    XmlAdapter<Configuration, org.apache.ivory.entity.common.Configuration> {

  @Override
  public org.apache.ivory.entity.common.Configuration unmarshal(Configuration v)
      throws Exception {
    Map<String, String> properties = new ConcurrentHashMap<String, String>();
    if(v!=null){
    List<Property> propertiesList = v.getProperty();
    for (Property property : propertiesList) {
      properties.put(property.getName(), property.getValue());
    }
    }
    return new org.apache.ivory.entity.common.Configuration(properties);
  }

  @Override
  public Configuration marshal(org.apache.ivory.entity.common.Configuration v)
      throws Exception {
    List<Property> propertiesList = new ArrayList<Property>();
    if(v!=null){
    for (Entry<String, String> entry : v) {
      propertiesList.add(new Property(entry.getKey(), entry.getValue()));
    }
  }
    return new Configuration(propertiesList);
  }

}

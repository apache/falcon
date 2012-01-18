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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.ivory.entity.v0.dataset.Attribute;
import org.apache.ivory.entity.v0.dataset.Parameters;

public class ParametersMapAdapter extends XmlAdapter<Parameters, Map<String, String>> {

  @Override
  public Parameters marshal(Map<String, String> v) throws Exception {
    Parameters parameters = new Parameters();
    List<Attribute> attributes = parameters.getAttribute();
    if(v!=null){
    	for (Map.Entry<String, String> entry : v.entrySet()) {
    		attributes.add(new Attribute(entry.getKey(), entry.getValue()));
    	}
    }
    return parameters;
  }

  @Override
  public Map<String, String> unmarshal(Parameters parameters) throws Exception {
    Map<String, String> map = new HashMap<String, String>();
    for (Attribute attribute : parameters.getAttribute()) {
      map.put(attribute.getName(), attribute.getValue());
    }
    return map;
  }

}

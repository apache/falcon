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
package org.apache.ivory.entity.v0.dataset.adapter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.ivory.entity.v0.dataset.Policies;
import org.apache.ivory.entity.v0.dataset.Policy;

public class PoliciesMapAdapter extends XmlAdapter<Policies, Map<String, Policy>> {

  @Override
  public Policies marshal(Map<String, Policy> v) throws Exception {
    if (v == null) return null;
    Policies policies = new Policies();
    List<Policy> policiesList = policies.getPolicy();
    for (Map.Entry<String, Policy> entry : v.entrySet()) {
      policiesList.add(entry.getValue());
    }
    return policies;
  }

  @Override
  public Map<String, Policy> unmarshal(Policies policies) throws Exception {
    if (policies == null) return null;
    Map<String, Policy> map = new HashMap<String, Policy>();
    for (Policy policy : policies.getPolicy()) {
      map.put(policy.getType(), policy);
    }
    return map;
  }
}

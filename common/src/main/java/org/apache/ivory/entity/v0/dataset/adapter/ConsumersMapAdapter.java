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

import org.apache.ivory.entity.v0.dataset.Consumer;
import org.apache.ivory.entity.v0.dataset.Consumers;

public class ConsumersMapAdapter extends XmlAdapter<Consumers, Map<String, Consumer>> {

  @Override
  public Consumers marshal(Map<String, Consumer> v) throws Exception {
    Consumers consumers = new Consumers();
    if (v != null) {
	    List<Consumer> consumersList = consumers.getConsumer();
	    for (Map.Entry<String, Consumer> entry : v.entrySet()) {
	      consumersList.add(entry.getValue());
	    }
    }
    return consumers;
  }

  @Override
  public Map<String, Consumer> unmarshal(Consumers consumers) throws Exception {
    Map<String, Consumer> map = new HashMap<String, Consumer>();
    for (Consumer consumer : consumers.getConsumer()) {
      map.put(consumer.getName(), consumer);
    }
    return map;
  }

}

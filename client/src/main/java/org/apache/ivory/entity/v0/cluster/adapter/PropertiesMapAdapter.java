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

import org.apache.ivory.entity.v0.cluster.Property;
import org.apache.ivory.entity.v0.cluster.Properties;

public class PropertiesMapAdapter extends
		XmlAdapter<Properties, Map<String, Property>> {

	@Override
	public Properties marshal(Map<String, Property> v) throws Exception {
		if (v == null)
			return null;
		Properties properties = new Properties();
		List<Property> propteriesList = properties.getProperty();
		for (Property property : v.values()) {
			propteriesList.add(property);
		}
		return properties;
	}

	@Override
	public Map<String, Property> unmarshal(Properties properties)
			throws Exception {
		if (properties == null)
			return null;
		Map<String, Property> map = new HashMap<String, Property>();
		for (Property property : properties.getProperty()) {
			map.put(property.getName(), property);
		}
		return map;
	}

}

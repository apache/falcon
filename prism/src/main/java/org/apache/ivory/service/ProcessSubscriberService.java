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
package org.apache.ivory.service;

import org.apache.ivory.IvoryException;
import org.apache.ivory.util.StartupProperties;

public class ProcessSubscriberService implements IvoryService {

    private IvoryTopicSubscriber subscriber;

	private static enum JMSprops {
		IvoryBrokerImplClass("broker.impl.class", "org.apache.activemq.ActiveMQConnectionFactory"), 
		IvoryBrokerUrl("broker.url", "tcp://localhost:61616?daemon=true"), 
		IvoryEntityTopic("entity.topic", "IVORY.ENTITY.TOPIC");

		private String propName;
		private String defaultPropValue;

		private JMSprops(String propName, String defaultPropValue) {
			this.propName = propName;
			this.defaultPropValue = defaultPropValue;
		}

	}

	@Override
	public String getName() {
		return ProcessSubscriberService.class.getSimpleName();
	}

	@Override
	public void init() throws IvoryException {
        String ivoryBrokerImplClass = getPropertyValue(JMSprops.IvoryBrokerImplClass);
        String ivoryBrokerUrl = getPropertyValue(JMSprops.IvoryBrokerUrl);
        String ivoryEntityTopic = getPropertyValue(JMSprops.IvoryEntityTopic);

		subscriber = new IvoryTopicSubscriber(ivoryBrokerImplClass, "", "",
                ivoryBrokerUrl, ivoryEntityTopic);
		subscriber.startSubscriber();
	}

	private String getPropertyValue(JMSprops prop) {
		return StartupProperties.get().getProperty(prop.propName,
				prop.defaultPropValue);
	}

	@Override
	public void destroy() throws IvoryException {
		subscriber.closeSubscriber();
	}
}

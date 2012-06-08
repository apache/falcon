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
package org.apache.ivory.aspect.instances;

import org.apache.ivory.IvoryException;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;

public class ProcessSubscriberService implements IvoryService {

	private static final Logger LOG = Logger
			.getLogger(ProcessSubscriberService.class);

	private String ivoryBrokerImplClass;
	private String ivoryBrokerUrl;
	private String ivoryProcessTopic;

	private IvoryTopicSubscriber subscriber;

	private static enum JMSprops {
		IvoryBrokerImplClass("broker.impl.class", "org.apache.activemq.ActiveMQConnectionFactory"), 
		IvoryBrokerUrl("broker.url", "tcp://localhost:61616?daemon=true"), 
		IvoryProcessTopic("process.topic", "IVORY.ENTITY.TOPIC");

		private String propName;
		private String defaultPropValue;

		private JMSprops(String propName, String defaultPropValue) {
			this.propName = propName;
			this.defaultPropValue = defaultPropValue;
		}

	}

	// @Override
	public String getName() {
		return ProcessSubscriberService.class.getSimpleName();
	}

	// @Override
	public void init() throws IvoryException {
		this.ivoryBrokerImplClass = getPropertyValue(JMSprops.IvoryBrokerImplClass);
		this.ivoryBrokerUrl = getPropertyValue(JMSprops.IvoryBrokerUrl);
		this.ivoryProcessTopic = getPropertyValue(JMSprops.IvoryProcessTopic);

		subscriber = new IvoryTopicSubscriber(ivoryBrokerImplClass, "", "",
				ivoryBrokerUrl, ivoryProcessTopic);
		subscriber.startSubscriber();
	}

	private String getPropertyValue(JMSprops prop) {
		return StartupProperties.get().getProperty(prop.propName,
				prop.defaultPropValue);
	}

	// @Override
	public void destroy() throws IvoryException {
		subscriber.closeSubscriber();
	}
}

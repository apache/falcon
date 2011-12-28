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

package org.apache.ivory.messaging;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.log4j.Logger;
import org.springframework.jms.core.MessageCreator;

/**
 * Ivory JMS message creator It just wraps a TextMessage
 */
public class ProcessMessageCreator implements MessageCreator {

	private static final Logger LOG = Logger
			.getLogger(ProcessMessageCreator.class);

	private final String entityName;
	private final String feedName;
	private final String message;

	public ProcessMessageCreator(String entityName, String feedName,
			String message) {
		this.entityName = entityName;
		this.feedName = feedName;
		this.message = message;
	}

	@Override
	public Message createMessage(Session session) throws JMSException {
		MapMessage mapMessage = session.createMapMessage();
		mapMessage.setString("entityName", this.entityName);
		mapMessage.setString("feedName", this.feedName);
		mapMessage.setString("message", this.message);
		
		LOG.debug("Sending "
				+ ((ActiveMQMapMessage) mapMessage).getContentMap());

		return mapMessage;
	}

}

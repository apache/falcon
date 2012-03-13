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
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.ivory.messaging.EntityInstanceMessage;

import org.apache.log4j.Logger;

/**
 * Ivory JMS message creator- creates JMS TextMessage
 */
public class EntityInstanceMessageCreator  {

	private static final Logger LOG = Logger
			.getLogger(EntityInstanceMessageCreator.class);

	private TextMessage textMessage;

	private final EntityInstanceMessage args;

	public EntityInstanceMessageCreator(EntityInstanceMessage args) {
		this.args = args;
	}

	public TextMessage createMessage(Session session) throws JMSException {
		this.textMessage = session.createTextMessage();
		this.textMessage.setText(this.args.toString());
		LOG.debug("Sending Message: " + this.textMessage.getText());
		// System.out.println("Sending Message: " + this.textMessage);
		return this.textMessage;
	}

	@Override
	public String toString() {
		try {
			return this.textMessage.getText();
		} catch (JMSException e) {
			return e.getMessage();
		}

	}

}

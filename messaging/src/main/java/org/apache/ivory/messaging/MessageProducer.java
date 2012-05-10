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

import java.lang.reflect.InvocationTargetException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.log4j.Logger;

/**
 * Default Ivory Message Producer The configuration are loaded from
 * jms-beans.xml
 */
public class MessageProducer {

	private Connection connection;
	private static final Logger LOG = Logger.getLogger(MessageProducer.class);
	private static final long DEFAULT_TTL = 3 * 24 * 60 * 60 * 1000;

	/**
	 * 
	 * @param arguments
	 *            - Accepts a Message to be send to JMS topic, creates a new
	 *            Topic based on topic name if it does not exist or else
	 *            existing topic with the same name is used to send the message.
	 * @throws JMSException
	 */
	protected void sendMessage(EntityInstanceMessage entityInstanceMessage)
			throws JMSException {

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Topic entityTopic = session.createTopic(entityInstanceMessage
				.getTopicName());
		javax.jms.MessageProducer producer = session
				.createProducer(entityTopic);
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		long messageTTL = DEFAULT_TTL;
		try {
			long messageTTLinMins = Long.parseLong(entityInstanceMessage
					.getBrokerTTL());
			messageTTL = messageTTLinMins * 60 * 1000;
		} catch (NumberFormatException e) {
			LOG.error("Error in parsing broker.ttl, setting TTL to:"
					+ DEFAULT_TTL+ " milli-seconds");
		}
		producer.setTimeToLive(messageTTL);
		producer.send(new EntityInstanceMessageCreator(entityInstanceMessage)
				.createMessage(session));

	}

	/**
	 * 
	 * @param args
	 *            - array of Strings, which will be used to create TextMessage
	 */
	public static void main(String[] args) {
		debug(args);
		EntityInstanceMessage[] entityInstanceMessage = EntityInstanceMessage
				.argsToMessage(args);
		if (entityInstanceMessage.length == 0) {
			LOG.warn("No operation on output feed");
			return;
		}

		MessageProducer ivoryMessageProducer = new MessageProducer();
		try {
			ivoryMessageProducer.createAndStartConnection(
					args[EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.ORDER()],
					"", "", entityInstanceMessage[0].getBrokerUrl());
			for (EntityInstanceMessage processMessage : entityInstanceMessage) {
				ivoryMessageProducer.sendMessage(processMessage);
			}
		} catch (JMSException e) {
			LOG.error(e);
			e.printStackTrace();
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
		} finally {
			try {
				ivoryMessageProducer.connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

	}

	private static void debug(String[] args) {
		if (LOG.isDebugEnabled()) {
			for (int i = 0; i < args.length; i++) {
				LOG.debug(args[i] + "::");
			}
		}

	}

	private void createAndStartConnection(String implementation,
			String userName, String password, String url) throws JMSException,
			ClassNotFoundException, IllegalArgumentException,
			SecurityException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		Class<ConnectionFactory> clazz = (Class<ConnectionFactory>) MessageProducer.class
				.getClassLoader().loadClass(implementation);

		ConnectionFactory connectionFactory = clazz.getConstructor(
				String.class, String.class, String.class).newInstance(userName,
				password, url);

		connection = connectionFactory.createConnection();
		connection.start();
	}

}

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

import java.lang.reflect.InvocationTargetException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.ivory.IvoryException;
import org.apache.ivory.messaging.EntityInstanceMessage.ARG;
import org.apache.ivory.resource.AbstractProcessInstanceManager;
import org.apache.log4j.Logger;

public class IvoryTopicSubscriber implements MessageListener, ExceptionListener {
	private static final Logger LOG = Logger
			.getLogger(IvoryTopicSubscriber.class);

	private TopicSubscriber subscriber;
	private String implementation;
	private String userName;
	private String password;
	private String url;
	private String topicName;
	private Connection connection;
	private AbstractProcessInstanceManager processInstanceManager = new AbstractProcessInstanceManager() {
		@Override
		public String getName() {
			return "Test";
		}
	};

	public IvoryTopicSubscriber(String implementation, String userName,
			String password, String url, String topicName) {
		this.implementation = implementation;
		this.userName = userName;
		this.password = password;
		this.url = url;
		this.topicName = topicName;
	}

	public void startSubscriber() throws IvoryException {
		try {
			connection = createAndGetConnection(implementation, userName,
					password, url);
			TopicSession session = (TopicSession) connection.createSession(
					false, Session.AUTO_ACKNOWLEDGE);
			Topic destination = session.createTopic(topicName);
			subscriber = session.createSubscriber(destination);
			subscriber.setMessageListener(this);
			connection.setExceptionListener(this);
			connection.start();
		} catch (Exception e) {
			LOG.error("Error starting subscriber of topic: " + this.toString(),
					e);
			throw new IvoryException(e);
		}
	}

	// @Override
	public void onMessage(Message message) {
		MapMessage mapMessage = (MapMessage) message;
		try {
			debug(mapMessage);
			String processName = mapMessage.getString(ARG.entityName
					.getArgName());
			String feedName = mapMessage.getString(ARG.feedNames.getArgName());
			String feedpath = mapMessage.getString(ARG.feedInstancePaths
					.getArgName());
			String workflowId = mapMessage.getString(ARG.workflowId
					.getArgName());
			String runId = mapMessage.getString(ARG.runId.getArgName());
			String nominalTime = mapMessage.getString(ARG.nominalTime
					.getArgName());
			String timeStamp = mapMessage.getString(ARG.timeStamp.getArgName());
			String status = mapMessage.getString(ARG.status.getArgName());

			try {
				processInstanceManager.instrumentWithAspect(processName,
						feedName, feedpath, nominalTime, timeStamp, status,
						workflowId, runId, System.currentTimeMillis());
			} catch (Exception ignore) {
				// mocked exception
			}

		} catch (Exception ignore) {
			LOG.info(
					"Error in onMessage for subscriber of topic: "
							+ this.toString(), ignore);
		}

	}

	private void debug(MapMessage mapMessage) throws JMSException {
		StringBuffer buff = new StringBuffer();
		buff.append("Received:{");
		for (ARG arg : ARG.values()) {
			buff.append(
					arg.getArgName() + "="
							+ mapMessage.getString(arg.getArgName())).append(
					", ");
		}
		buff.append("}");
		LOG.debug(buff);
	}

	// @Override
	public void onException(JMSException ignore) {
		LOG.info(
				"Error in onException for subscriber of topic: "
						+ this.toString(), ignore);
	}

	public void closeSubscriber() throws IvoryException {
		try {
			LOG.info("Closing subscriber on topic : " + this.topicName);
			subscriber.close();
			connection.close();
		} catch (JMSException e) {
			LOG.error("Error closing subscriber of topic: " + this.toString(),
					e);
			throw new IvoryException(e);
		}
	}

	private static Connection createAndGetConnection(String implementation,
			String userName, String password, String url) throws JMSException,
			ClassNotFoundException, IllegalArgumentException,
			SecurityException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		@SuppressWarnings("unchecked")
		Class<ConnectionFactory> clazz = (Class<ConnectionFactory>) IvoryTopicSubscriber.class
				.getClassLoader().loadClass(implementation);

		ConnectionFactory connectionFactory = clazz.getConstructor(
				String.class, String.class, String.class).newInstance(userName,
				password, url);

		Connection connection = connectionFactory.createConnection();
		return connection;
	}

	@Override
	public String toString() {
		return topicName;
	}

}

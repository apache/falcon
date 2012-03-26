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
import java.net.InetAddress;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.ivory.IvoryException;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.log4j.Logger;

public class IvoryTopicSubscriber implements MessageListener, ExceptionListener {
	private static final Logger LOG = Logger
			.getLogger(IvoryTopicSubscriber.class);

	private static final String MSG_SEPERATOR = "\\$";

	private static  String IVORY_PROCESS_TOPIC_CLIENT = "IVORY.PROCESS.CLIENT";

	private TopicSubscriber subscriber;
	private String implementation;
	private String userName;
	private String password;
	private String url;
	private String topicName;

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
			Connection connection = createAndGetConnection(
					implementation, userName, password, url);
			connection.setClientID(IVORY_PROCESS_TOPIC_CLIENT+"-"+InetAddress.getLocalHost().getHostName());
			TopicSession session = (TopicSession) connection.createSession(
					false, Session.AUTO_ACKNOWLEDGE);
			Topic destination = session.createTopic(topicName);
			subscriber = session.createDurableSubscriber(destination,IVORY_PROCESS_TOPIC_CLIENT+"-"+InetAddress.getLocalHost().getHostName());
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
		TextMessage textmessage = (TextMessage) message;
		try {
			LOG.debug("Received: "+textmessage.getText());
			String[] items = textmessage.getText().split(MSG_SEPERATOR);
			String processName = items[0];
			String feedName = items[1];
			String feedpath = items[2];
			String workflowId = items[3];
			String runId = items[4];
			String nominalTime = items[5];
			String timeStamp = items[6];
			String status = items[7];

			try {
				instrumentWithAspect(processName, feedName, feedpath,
						nominalTime, timeStamp, status);
			} catch (Exception ignore) {
				// mocked exception
			}

		} catch (Exception ignore) {
			LOG.info(
					"Error in onMessage for subscriber of topic: "
							+ this.toString(), ignore);
		}

	}

	/*
	 * Below method is a mock and gets automatically invoked by Aspect
	 */
	// TODO capture execution time
	@Monitored(event = "process-instance")
	private String instrumentWithAspect(
			@Dimension(value = "process") String process,
			@Dimension(value = "feed") String feedName,
			@Dimension(value = "feedPath") String feedpath,
			@Dimension(value = "nominalTime") String nominalTime,
			@Dimension(value = "timeStamp") String timeStamp,@Dimension(value = "status") String status)
					throws Exception {
		if(status.equalsIgnoreCase("FAILED")){
			throw new Exception(process+":"+nominalTime+" Failed");
		}
		return "DONE";
		
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
		return this.topicName;
	}
}

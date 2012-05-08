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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.ivory.IvoryException;
import org.apache.ivory.aspect.instances.IvoryTopicSubscriber;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.mortbay.log.Log;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class IvoryTopicSubscriberTest {

	private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
	// private static final String BROKER_URL =
	// "tcp://localhost:61616?daemon=true";
	private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
	private static final String TOPIC_NAME = "IVORY.PROCESS.TOPIC";
	private BrokerService broker;

	@BeforeClass
	public void setup() throws Exception {
		broker = new BrokerService();
		broker.setUseJmx(true);
		broker.addConnector(BROKER_URL);
		broker.start();
	}

	public void sendMessages() throws JMSException {
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				BROKER_URL);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createTopic(TOPIC_NAME);
		javax.jms.MessageProducer producer = session
				.createProducer(destination);
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		for (int i = 0; i < 10; i++) {
			String ivoryMessage = getMockIvoryMessage(i).toString();
			TextMessage message = session.createTextMessage(ivoryMessage);
			Log.debug("Sending:"+message);
			producer.send(message);
		}

		EntityInstanceMessage message = getMockIvoryMessage(15);
		message.setStatus("FAILED");
		TextMessage textMessage = session.createTextMessage(message.toString());
		producer.send(textMessage);

	}

	private EntityInstanceMessage getMockIvoryMessage(int i) {
		EntityInstanceMessage message = new EntityInstanceMessage();
		message.setBrokerImplClass(BROKER_IMPL_CLASS);
		message.setBrokerUrl(BROKER_URL);
		message.setProcessName("process1");
		message.setEntityType("PROCESS");
		message.setFeedInstancePath("/clicks/hour/00/0" + i);
		message.setFeedName("clicks");
		message.setLogFile("/logfile");
		message.setNominalTime("2012-10-10-10-10");
		message.setOperation("GENERATE");
		message.setRunId("0");
		message.setTimeStamp("2012-10-10-10-1" + i);
		message.setWorkflowId("workflow-" + i);
		message.setTopicName(TOPIC_NAME);
		message.setStatus("SUCCEEDED");
		return message;
	}

	@Test
	public void testSubscriber() throws IvoryException, JMSException {
		IvoryTopicSubscriber subscriber1 = new IvoryTopicSubscriber(
				BROKER_IMPL_CLASS, "", "", BROKER_URL, TOPIC_NAME);

		subscriber1.startSubscriber();
		sendMessages();
		subscriber1.closeSubscriber();

	}

	@AfterClass
	public void tearDown() throws Exception {
		broker.stop();
	}

}

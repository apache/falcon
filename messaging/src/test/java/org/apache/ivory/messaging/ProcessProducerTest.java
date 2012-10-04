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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.ivory.messaging.EntityInstanceMessage.ARG;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProcessProducerTest {

	private String[] args;
	private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
	// private static final String BROKER_URL =
	// "tcp://localhost:61616?daemon=true";
	private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
	private static final String TOPIC_NAME = "IVORY.PROCESS";
	private BrokerService broker;

	private volatile AssertionError error;

	@BeforeClass
	public void setup() throws Exception {
		args = new String[] { "-" + ARG.entityName.getArgName(), TOPIC_NAME,
				"-" + ARG.feedNames.getArgName(), "click-logs,raw-logs",
				"-" + ARG.feedInstancePaths.getArgName(),
				"/click-logs/10/05/05/00/20,/raw-logs/10/05/05/00/20",
				"-" + ARG.workflowId.getArgName(), "workflow-01-00",
				"-" + ARG.runId.getArgName(), "1",
				"-" + ARG.nominalTime.getArgName(), "2011-01-01-01-00",
				"-" + ARG.timeStamp.getArgName(), "2012-01-01-01-00",
				"-" + ARG.brokerUrl.getArgName(), BROKER_URL,
				"-" + ARG.brokerImplClass.getArgName(), (BROKER_IMPL_CLASS),
				"-" + ARG.entityType.getArgName(), ("process"),
				"-" + ARG.operation.getArgName(), ("GENERATE"),
				"-" + ARG.logFile.getArgName(), ("/logFile"),
				"-" + ARG.topicName.getArgName(), (TOPIC_NAME),
				"-" + ARG.status.getArgName(), ("SUCCEEDED"),
				"-" + ARG.brokerTTL.getArgName(), "10",
				"-" + ARG.cluster.getArgName(), "corp" };
		broker = new BrokerService();
		broker.addConnector(BROKER_URL);
		broker.setDataDirectory("target/activemq");
		broker.setBrokerName("localhost");
		broker.setSchedulerSupport(true);
		broker.start();
	}

	@AfterClass
	public void tearDown() throws Exception {
		broker.deleteAllMessages();
		broker.stop();
	}

	@Test
	public void testProcessMessageCreator() throws Exception {

		Thread t = new Thread() {
			@Override
			public void run() {
				try {
					consumer();
				} catch (AssertionError e) {
					error = e;
				} catch (JMSException ignore) {

				}
			}
		};
		t.start();
		Thread.sleep(1500);
		new MessageProducer().run(this.args);
		t.join();
		if (error != null) {
			throw error;
		}
	}

	private void consumer() throws JMSException {
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				BROKER_URL);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createTopic(TOPIC_NAME);
		MessageConsumer consumer = session.createConsumer(destination);

		// wait till you get atleast one message
		MapMessage m;
		for (m = null; m == null;)
			m = (MapMessage) consumer.receive();
		System.out.println("Consumed: " + m.toString());
		assertMessage(m);
		Assert.assertEquals(m.getString(ARG.feedNames.getArgName()),
				"click-logs");
		Assert.assertEquals(m.getString(ARG.feedInstancePaths.getArgName()),
				"/click-logs/10/05/05/00/20");

		for (m = null; m == null;)
			m = (MapMessage) consumer.receive();
		System.out.println("Consumed: " + m.toString());
		assertMessage(m);
		Assert.assertEquals(m.getString(ARG.feedNames.getArgName()), "raw-logs");
		Assert.assertEquals(m.getString(ARG.feedInstancePaths.getArgName()),
				"/raw-logs/10/05/05/00/20");
		connection.close();
	}

	private void assertMessage(MapMessage m) throws JMSException {
		Assert.assertEquals(m.getString(ARG.entityName.getArgName()),
				TOPIC_NAME);
		Assert.assertEquals(m.getString(ARG.workflowId.getArgName()),
				"workflow-01-00");
		Assert.assertEquals(m.getString(ARG.runId.getArgName()), "1");
		Assert.assertEquals(m.getString(ARG.nominalTime.getArgName()),
				"2011-01-01T01:00Z");
		Assert.assertEquals(m.getString(ARG.timeStamp.getArgName()),
				"2012-01-01T01:00Z");
		Assert.assertEquals(m.getString(ARG.status.getArgName()), "SUCCEEDED");
	}
}

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
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProcessProducerTest {

	private EntityInstanceMessage msgArgs;
	private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
	//private static final String BROKER_URL = "tcp://localhost:61616?daemon=true";
	private static final String BROKER_IMPL_CLASS="org.apache.activemq.ActiveMQConnectionFactory";	
	private static final String TOPIC_NAME = "Ivory.process1.click-logs";
	private BrokerService broker;
	
	private volatile AssertionError error;

	@BeforeClass
	public void setup() throws Exception {
		this.msgArgs = new EntityInstanceMessage();
		this.msgArgs.setEntityTopicName(TOPIC_NAME);
		this.msgArgs.setFeedName("click-logs,raw-logs,");
		this.msgArgs.setFeedInstancePath("/click-logs/10/05/05/00/20,/raw-logs/10/05/05/00/20,");
		this.msgArgs.setWorkflowId("workflow-01-00");
		this.msgArgs.setRunId("1");
		this.msgArgs.setNominalTime("2011-01-01");
		this.msgArgs.setTimeStamp("2012-01-01");
		this.msgArgs.setBrokerUrl(BROKER_URL);
		this.msgArgs.setBrokerImplClass(BROKER_IMPL_CLASS);
		this.msgArgs.setEntityType("PROCESS");
		this.msgArgs.setOperation("GENERATE");

		broker = new BrokerService();
		broker.setUseJmx(true);
		broker.addConnector(BROKER_URL);
		broker.start();
	}

	@AfterClass
	public void tearDown() throws Exception {
		broker.stop();
	}

	@Test
	public void testProcessMessageCreator() throws JMSException,
			InterruptedException {

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
		Thread.sleep(1000);
		MessageProducer.main(EntityInstanceMessage
				.messageToArgs(new EntityInstanceMessage[] { this.msgArgs }));
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
		TextMessage m;
		for (m = null; m == null;)
			m = (TextMessage)consumer.receive();
		System.out.println("Consumed: " + m.getText());
		String[] items = m.getText().split(",");
		assertMessage(items);
		Assert.assertEquals(items[1], "click-logs");
		Assert.assertEquals(items[2], "/click-logs/10/05/05/00/20");
		
		for (m = null; m == null;)
			m = (TextMessage)consumer.receive();
		items = m.getText().split(",");
		assertMessage(items);
		Assert.assertEquals(items[1], "raw-logs");
		Assert.assertEquals(items[2], "/raw-logs/10/05/05/00/20");


		connection.close();
	}
	
	private void assertMessage(String [] items) throws JMSException {
		Assert.assertEquals(items.length, 7);
		Assert.assertEquals(items[0], TOPIC_NAME);
		Assert.assertEquals(items[3], "workflow-01-00");
		Assert.assertEquals(items[4],"1");
		Assert.assertEquals(items[5],"2011-01-01");
		Assert.assertEquals(items[6],"2012-01-01");
	}
}

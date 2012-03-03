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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MessageProducerTest {

	private ProcessMessage msgArgs;
	private static final String TEST_CONN_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
	private static final String TOPIC_NAME = "Ivory.process1.click-logs";
	private volatile AssertionError error;

	@BeforeClass
	public void setArgs() {
		this.msgArgs = new ProcessMessage();
		this.msgArgs.setProcessTopicName(TOPIC_NAME);
		this.msgArgs.setFeedName("click-logs,");
		this.msgArgs.setFeedInstancePath("/click-logs/10/05/05/00/20,");
		this.msgArgs.setWorkflowId("workflow-01-00");
		this.msgArgs.setRunId("1");
		this.msgArgs.setNominalTime("2011-01-01");
		this.msgArgs.setTimeStamp("2012-01-01");
		this.msgArgs.setBrokerUrl(TEST_CONN_URL);
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
					error=e;
				} catch (JMSException ignore) {
					
				}
			}
		};
		t.start();
		Thread.sleep(1500);
		MessageProducer.main(ArgumentsResolver
				.resolveToStringArray(new ProcessMessage[] { this.msgArgs }));
		if(error!=null){
			throw error;
		}
	}

	private void consumer() throws JMSException {
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				TEST_CONN_URL);
		Connection connection = connectionFactory.createConnection();
		connection.start();

		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createTopic(TOPIC_NAME);
		MessageConsumer consumer = session.createConsumer(destination);
		Message m = consumer.receive();

		if (m != null) {
			TextMessage textMessage = (TextMessage) m;
			String[] items = textMessage.getText().split(",");
			Assert.assertEquals(items[0], TOPIC_NAME);
			Assert.assertEquals(items[1], "click-logs");
			Assert.assertEquals(items[2], "/click-logs/10/05/05/00/20");
			Assert.assertEquals(items[3], "workflow-01-00");
		}
		connection.close();
	}
}

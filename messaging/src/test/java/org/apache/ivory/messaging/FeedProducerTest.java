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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.messaging.EntityInstanceMessage.ARG;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.jms.*;
import java.io.InputStream;
import java.io.OutputStream;

public class FeedProducerTest {

	private String[] args;
	private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
	// private static final String BROKER_URL =
	// "tcp://localhost:61616?daemon=true";
	private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
	private static final String TOPIC_NAME = "Ivory.process1.click-logs";
	private BrokerService broker;

	private Path logFile;

	private volatile AssertionError error;
	private EmbeddedCluster dfsCluster;
	private Configuration conf ;

	@BeforeClass
	public void setup() throws Exception {

		this.dfsCluster = EmbeddedCluster.newCluster("testCluster", false);
        conf = dfsCluster.getConf();
		logFile = new Path(conf.get("fs.default.name"),
				"/ivory/feed/agg-logs/instance-2012-01-01-10-00.csv");

		args = new String[] { "-" + ARG.entityName.getArgName(), TOPIC_NAME,
				"-" + ARG.feedNames.getArgName(), "click-logs",
				"-" + ARG.feedInstancePaths.getArgName(),
				"/click-logs/10/05/05/00/20",
				"-" + ARG.workflowId.getArgName(), "workflow-01-00",
				"-" + ARG.runId.getArgName(), "1",
				"-" + ARG.nominalTime.getArgName(), "2011-01-01-01-00",
				"-" + ARG.timeStamp.getArgName(), "2012-01-01-01-00",
				"-" + ARG.brokerUrl.getArgName(), BROKER_URL,
				"-" + ARG.brokerImplClass.getArgName(), (BROKER_IMPL_CLASS),
				"-" + ARG.entityType.getArgName(), ("FEED"),
				"-" + ARG.operation.getArgName(), ("DELETE"),
				"-" + ARG.logFile.getArgName(), (logFile.toString()),
				"-" + ARG.topicName.getArgName(), (TOPIC_NAME),
				"-" + ARG.status.getArgName(), ("SUCCEEDED"),
				"-" + ARG.brokerTTL.getArgName(), "10",
				"-" + ARG.cluster.getArgName(), "corp" };

		broker = new BrokerService();
		broker.addConnector(BROKER_URL);
		broker.setDataDirectory("target/activemq");
		broker.start();
	}

	@AfterClass
	public void tearDown() throws Exception {
		broker.deleteAllMessages();
		broker.stop();
		this.dfsCluster.shutdown();
	}

	@Test
	public void testLogFile() throws Exception {
		FileSystem fs = dfsCluster.getFileSystem();
		OutputStream out = fs.create(logFile);
		InputStream in = new ByteArrayInputStream(
				("instancePaths=/ivory/feed/agg-logs/path1/2010/10/10/20,"
						+ "/ivory/feed/agg-logs/path1/2010/10/10/21,"
						+ "/ivory/feed/agg-logs/path1/2010/10/10/22,"
						+ "/ivory/feed/agg-logs/path1/2010/10/10/23")
						.getBytes());
		IOUtils.copyBytes(in, out, conf);
		testProcessMessageCreator();
	}

	@Test
	public void testEmptyLogFile() throws Exception {
		FileSystem fs = dfsCluster.getFileSystem();
		OutputStream out = fs.create(logFile);
		InputStream in = new ByteArrayInputStream(("instancePaths=").getBytes());
		IOUtils.copyBytes(in, out, conf);

		new MessageProducer().run(this.args);
	}

	private void testProcessMessageCreator() throws Exception {

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
		Assert.assertEquals(m.getString(ARG.feedInstancePaths.getArgName()),
				"/ivory/feed/agg-logs/path1/2010/10/10/20");

		for (m = null; m == null;)
			m = (MapMessage) consumer.receive();
		System.out.println("Consumed: " + m.toString());
		assertMessage(m);
		Assert.assertEquals(m.getString(ARG.feedInstancePaths.getArgName()),
				"/ivory/feed/agg-logs/path1/2010/10/10/21");

		for (m = null; m == null;)
			m = (MapMessage) consumer.receive();
		System.out.println("Consumed: " + m.toString());
		assertMessage(m);
		Assert.assertEquals(m.getString(ARG.feedInstancePaths.getArgName()),
				"/ivory/feed/agg-logs/path1/2010/10/10/22");

		for (m = null; m == null;)
			m = (MapMessage) consumer.receive();
		System.out.println("Consumed: " + m.toString());
		assertMessage(m);
		Assert.assertEquals(m.getString(ARG.feedInstancePaths.getArgName()),
				"/ivory/feed/agg-logs/path1/2010/10/10/23");

		connection.close();
	}

	private void assertMessage(MapMessage m) throws JMSException {
		Assert.assertEquals(m.getString(ARG.entityName.getArgName()),
				TOPIC_NAME);
		Assert.assertEquals(m.getString(ARG.operation.getArgName()), "DELETE");
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

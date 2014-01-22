/**
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
package org.apache.falcon.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.jms.*;

/**
 * Test for falcon topic message producer.
 */
public class FalconTopicProducerTest {

    private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
    // private static final String BROKER_URL =
    // "tcp://localhost:61616?daemon=true";
    private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String TOPIC_NAME = "FALCON.ENTITY.TOPIC";
    private static final String SECONDARY_TOPIC_NAME = "FALCON.ENTITY.SEC.TOPIC";
    private BrokerService broker;
    private List<MapMessage> mapMessages;

    private volatile AssertionError error;

    @BeforeClass
    public void setup() throws Exception {
        broker = new BrokerService();
        broker.addConnector(BROKER_URL);
        broker.setDataDirectory("target/activemq");
        broker.setBrokerName("localhost");
        broker.start();
    }

    @AfterClass
    public void tearDown() throws Exception {
        broker.deleteAllMessages();
        broker.stop();
    }

    @Test
    public void testWithFeedOutputPaths() throws Exception {
        List<String> args = createCommonArgs();
        List<String> newArgs = new ArrayList<String>(Arrays.asList(
                "-" + ARG.entityName.getArgName(), "agg-coord",
                "-" + ARG.feedNames.getArgName(), "click-logs,raw-logs",
                "-" + ARG.feedInstancePaths.getArgName(),
                "/click-logs/10/05/05/00/20,/raw-logs/10/05/05/00/20",
                "-" + ARG.topicName.getArgName(), TOPIC_NAME));
        args.addAll(newArgs);
        List<String[]> messages = new ArrayList<String[]>();
        messages.add(args.toArray(new String[args.size()]));
        testProcessMessageCreator(messages, TOPIC_NAME);
        for (MapMessage m : mapMessages) {
            assertMessage(m);
            Assert.assertTrue((m.getString(ARG.feedNames.getArgName())
                    .equals("click-logs,raw-logs")));
            Assert.assertTrue(m
                    .getString(ARG.feedInstancePaths.getArgName())
                    .equals("/click-logs/10/05/05/00/20,/raw-logs/10/05/05/00/20"));
        }
    }

    @Test
    public void testWithEmptyFeedOutputPaths() throws Exception {
        List<String> args = createCommonArgs();
        List<String> newArgs = new ArrayList<String>(Arrays.asList(
                "-" + ARG.entityName.getArgName(), "agg-coord",
                "-" + ARG.feedNames.getArgName(), "null",
                "-" + ARG.feedInstancePaths.getArgName(), "null",
                "-" + ARG.topicName.getArgName(), TOPIC_NAME));
        args.addAll(newArgs);
        List<String[]> messages = new ArrayList<String[]>();
        messages.add(args.toArray(new String[args.size()]));
        testProcessMessageCreator(messages, TOPIC_NAME);
        for (MapMessage m : mapMessages) {
            assertMessage(m);
            assertMessage(m);
            Assert.assertTrue(m.getString(ARG.feedNames.getArgName()).equals(
                    "null"));
            Assert.assertTrue(m.getString(ARG.feedInstancePaths.getArgName())
                    .equals("null"));
        }
    }

    @Test
    public void testConsumerWithMultipleTopics() throws Exception {
        List<String[]> messages = new ArrayList<String[]>();
        List<String> args = createCommonArgs();
        List<String> newArgs = new ArrayList<String>(Arrays.asList(
                "-" + ARG.entityName.getArgName(), "agg-coord",
                "-" + ARG.feedNames.getArgName(), "raw-logs",
                "-" + ARG.feedInstancePaths.getArgName(),
                "/raw-logs/10/05/05/00/20",
                "-" + ARG.topicName.getArgName(), TOPIC_NAME));
        args.addAll(newArgs);
        messages.add(args.toArray(new String[args.size()]));

        args = createCommonArgs();
        newArgs = new ArrayList<String>(Arrays.asList(
                "-" + ARG.entityName.getArgName(), "agg-coord",
                "-" + ARG.feedNames.getArgName(), "click-logs",
                "-" + ARG.feedInstancePaths.getArgName(),
                "/click-logs/10/05/05/00/20",
                "-" + ARG.topicName.getArgName(), SECONDARY_TOPIC_NAME));
        args.addAll(newArgs);
        messages.add(args.toArray(new String[args.size()]));

        testProcessMessageCreator(messages, TOPIC_NAME+","+SECONDARY_TOPIC_NAME);
        Assert.assertEquals(mapMessages.size(), 2);
        for (MapMessage m : mapMessages) {
            assertMessage(m);
        }
    }

    private List<String> createCommonArgs() {
        return new ArrayList<String>(Arrays.asList(
                "-" + ARG.workflowId.getArgName(), "workflow-01-00",
                "-" + ARG.runId.getArgName(), "1",
                "-" + ARG.nominalTime.getArgName(), "2011-01-01-01-00",
                "-" + ARG.timeStamp.getArgName(), "2012-01-01-01-00",
                "-" + ARG.brokerUrl.getArgName(), BROKER_URL,
                "-" + ARG.brokerImplClass.getArgName(), (BROKER_IMPL_CLASS),
                "-" + ARG.entityType.getArgName(), ("process"),
                "-" + ARG.operation.getArgName(), ("GENERATE"),
                "-" + ARG.logFile.getArgName(), ("/logFile"),
                "-" + ARG.status.getArgName(), ("SUCCEEDED"),
                "-" + ARG.brokerTTL.getArgName(), "10",
                "-" + ARG.cluster.getArgName(), "corp"));
    }

    private void testProcessMessageCreator(final List<String[]> messages,
             final String topicsToListen) throws Exception {

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    consumer(messages.size(), topicsToListen);
                } catch (AssertionError e) {
                    error = e;
                } catch (Exception ignore) {
                    error = null;
                }
            }
        };
        t.start();
        for (String[] message : messages) {
            new MessageProducer().run(message);
        }
        t.join();
        if (error != null) {
            throw error;
        }
    }

    private void consumer(int size, String topicsToListen) throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(topicsToListen);
        MessageConsumer consumer = session.createConsumer(destination);
        mapMessages = new ArrayList<MapMessage>();
        for (int i=0; i<size; i++) {
            MapMessage m = (MapMessage) consumer.receive();
            mapMessages.add(m);
            System.out.println("Consumed: " + m.toString());
        }

        connection.close();
    }

    private void assertMessage(MapMessage m) throws JMSException {
        Assert.assertEquals(m.getString(ARG.entityName.getArgName()),
                "agg-coord");
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

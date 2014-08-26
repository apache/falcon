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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test for falcon topic message producer.
 */
public class JMSMessageProducerTest {

    private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
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
                "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), "agg-coord",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), "click-logs,raw-logs",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
                "/click-logs/10/05/05/00/20,/raw-logs/10/05/05/00/20"));
        args.addAll(newArgs);
        List<String[]> messages = new ArrayList<String[]>();
        messages.add(args.toArray(new String[args.size()]));
        testProcessMessageCreator(messages, TOPIC_NAME);
        for (MapMessage m : mapMessages) {
            assertMessage(m);
            Assert.assertTrue((m.getString(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName())
                    .equals("click-logs,raw-logs")));
            Assert.assertTrue(m
                    .getString(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName())
                    .equals("/click-logs/10/05/05/00/20,/raw-logs/10/05/05/00/20"));
        }
    }

    @Test
    public void testWithEmptyFeedOutputPaths() throws Exception {
        List<String> args = createCommonArgs();
        List<String> newArgs = new ArrayList<String>(Arrays.asList(
                "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), "agg-coord",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), "null",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), "null"));
        args.addAll(newArgs);
        List<String[]> messages = new ArrayList<String[]>();
        messages.add(args.toArray(new String[args.size()]));
        testProcessMessageCreator(messages, TOPIC_NAME);
        for (MapMessage m : mapMessages) {
            assertMessage(m);
            assertMessage(m);
            Assert.assertTrue(m.getString(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName()).equals(
                    "null"));
            Assert.assertTrue(m.getString(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName())
                    .equals("null"));
        }
    }

    @Test
    public void testConsumerWithMultipleTopics() throws Exception {
        List<String[]> messages = new ArrayList<String[]>();
        List<String> args = createCommonArgs();
        List<String> newArgs = new ArrayList<String>(Arrays.asList(
                "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), "agg-coord",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), "raw-logs",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
                "/raw-logs/10/05/05/00/20"));
        args.addAll(newArgs);
        messages.add(args.toArray(new String[args.size()]));

        args = createCommonArgs();
        newArgs = new ArrayList<String>(Arrays.asList(
                "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), "agg-coord",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), "click-logs",
                "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
                "/click-logs/10/05/05/00/20"));
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
                "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
                "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), "falcon",
                "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
                "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), "2011-01-01-01-00",
                "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), "2012-01-01-01-00",
                "-" + WorkflowExecutionArgs.BRKR_URL.getName(), BROKER_URL,
                "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), (BROKER_IMPL_CLASS),
                "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), ("process"),
                "-" + WorkflowExecutionArgs.OPERATION.getName(), ("GENERATE"),
                "-" + WorkflowExecutionArgs.LOG_FILE.getName(), ("/logFile"),
                "-" + WorkflowExecutionArgs.LOG_DIR.getName(), ("/tmp"),
                "-" + WorkflowExecutionArgs.STATUS.getName(), ("SUCCEEDED"),
                "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "10",
                "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), "corp"));
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
        Thread.sleep(100);

        for (String[] message : messages) {
            WorkflowExecutionContext context = WorkflowExecutionContext.create(
                    message, WorkflowExecutionContext.Type.POST_PROCESSING);
            JMSMessageProducer jmsMessageProducer = JMSMessageProducer.builder(context)
                    .type(JMSMessageProducer.MessageType.FALCON).build();
            jmsMessageProducer.sendMessage();
        }

        t.join();
        if (error != null) {
            throw error;
        }
    }

    private void consumer(int size, String topicsToListen) throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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

    private void assertMessage(MapMessage message) throws JMSException {
        Assert.assertEquals(message.getString(WorkflowExecutionArgs.ENTITY_NAME.getName()),
                "agg-coord");
        Assert.assertEquals(message.getString(WorkflowExecutionArgs.NOMINAL_TIME.getName()),
                "2011-01-01-01-00");
        Assert.assertEquals(message.getString(WorkflowExecutionArgs.TIMESTAMP.getName()),
                "2012-01-01-01-00");
        Assert.assertEquals(message.getString(WorkflowExecutionArgs.STATUS.getName()), "SUCCEEDED");
    }
}

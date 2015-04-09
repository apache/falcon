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
import org.apache.commons.lang3.StringUtils;
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
import java.util.concurrent.CountDownLatch;

/**
 * Test for process message producer.
 */
public class ProcessProducerTest {

    private String[] args;
    private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
    private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String ENTITY_NAME = "process1";
    private BrokerService broker;

    private volatile AssertionError error;
    private CountDownLatch latch = new CountDownLatch(1);
    private String[] outputFeedNames = {"click-logs", "raw-logs"};
    private String[] outputFeedPaths = {"/click-logs/10/05/05/00/20", "/raw-logs/10/05/05/00/20"};

    @BeforeClass
    public void setup() throws Exception {
        args = new String[] {
            "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), ENTITY_NAME,
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), StringUtils.join(outputFeedNames, ","),
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), StringUtils.join(outputFeedPaths, ","),
            "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
            "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), "falcon",
            "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
            "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), "2011-01-01-01-00",
            "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), "2012-01-01-01-00",
            "-" + WorkflowExecutionArgs.BRKR_URL.getName(), BROKER_URL,
            "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS,
            "-" + WorkflowExecutionArgs.USER_BRKR_URL.getName(), BROKER_URL,
            "-" + WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS,
            "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), "process",
            "-" + WorkflowExecutionArgs.OPERATION.getName(), "GENERATE",
            "-" + WorkflowExecutionArgs.LOG_FILE.getName(), "/logFile",
            "-" + WorkflowExecutionArgs.LOG_DIR.getName(), "/falcon/feed/agg-logs/",
            "-" + WorkflowExecutionArgs.STATUS.getName(), "SUCCEEDED",
            "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "10",
            "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), "corp",
        };

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
                    error = null;
                }
            }
        };
        t.start();
        latch.await();

        WorkflowExecutionContext context = WorkflowExecutionContext.create(
                args, WorkflowExecutionContext.Type.POST_PROCESSING);
        JMSMessageProducer jmsMessageProducer = JMSMessageProducer.builder(context)
                .type(JMSMessageProducer.MessageType.USER).build();
        jmsMessageProducer.sendMessage();

        t.join();
        if (error != null) {
            throw error;
        }
    }

    private void consumer() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(getTopicName());
        MessageConsumer consumer = session.createConsumer(destination);

        latch.countDown();

        for(int index = 0; index < outputFeedNames.length; ++index) {
            MapMessage m = (MapMessage) consumer.receive();
            System.out.println("Consumed: " + m.toString());
            assertMessage(m);
            Assert.assertEquals(m.getString(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName()),
                    outputFeedNames[index]);
            Assert.assertEquals(m.getString(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName()),
                    outputFeedPaths[index]);
        }
        connection.close();
    }

    private String getTopicName() {
        return JMSMessageProducer.FALCON_TOPIC_PREFIX + ENTITY_NAME;
    }

    private void assertMessage(MapMessage m) throws JMSException {
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.ENTITY_NAME.getName()), ENTITY_NAME);
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.WORKFLOW_ID.getName()),
                "workflow-01-00");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.WORKFLOW_USER.getName()),
                "falcon");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.RUN_ID.getName()), "1");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.NOMINAL_TIME.getName()),
                "2011-01-01-01-00");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.TIMESTAMP.getName()),
                "2012-01-01-01-00");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.STATUS.getName()), "SUCCEEDED");
    }
}

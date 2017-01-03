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
package org.apache.falcon.oozie.workflow;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.workflow.FalconPostProcessing;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
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
 * Test for validating the falcon post processing utility.
 */
public class FalconPostProcessingTest {

    private String[] args;
    private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
    private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String ENTITY_NAME = "agg-coord";
    private String userBrokerUrl = BROKER_URL;
    private BrokerService broker;

    private volatile AssertionError error;
    private CountDownLatch latch = new CountDownLatch(1);
    private String[] outputFeedNames = {"out-click-logs", "out-raw-logs"};
    private String[] outputFeedPaths = {"/out-click-logs/10/05/05/00/20", "/out-raw-logs/10/05/05/00/20"};
    private String userNotification = "true";
    private String systemNotification = "true";

    @BeforeClass
    public void setup() throws Exception {
        args = new String[]{
            "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), ENTITY_NAME,
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), StringUtils.join(outputFeedNames, ","),
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), StringUtils.join(outputFeedPaths, ","),
            "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
            "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), FalconTestUtil.TEST_USER_1,
            "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
            "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), "2011-01-01-01-00",
            "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), "2012-01-01-01-00",
            "-" + WorkflowExecutionArgs.BRKR_URL.getName(), BROKER_URL,
            "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS,
            "-" + WorkflowExecutionArgs.USER_BRKR_URL.getName(), userBrokerUrl,
            "-" + WorkflowExecutionArgs.USER_JMS_NOTIFICATION_ENABLED, userNotification,
            "-" + WorkflowExecutionArgs.SYSTEM_JMS_NOTIFICATION_ENABLED, systemNotification,
            "-" + WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS,
            "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), "process",
            "-" + WorkflowExecutionArgs.OPERATION.getName(), "GENERATE",
            "-" + WorkflowExecutionArgs.LOG_FILE.getName(), "/logFile",
            "-" + WorkflowExecutionArgs.STATUS.getName(), "SUCCEEDED",
            "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "10",
            "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), "corp",
            "-" + WorkflowExecutionArgs.WF_ENGINE_URL.getName(), "http://localhost:11000/oozie/",
            "-" + WorkflowExecutionArgs.LOG_DIR.getName(), "target/log",
            "-" + WorkflowExecutionArgs.USER_SUBFLOW_ID.getName(), "userflow@wf-id" + "test",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(), "oozie",
            "-" + WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), "in-click-logs,in-raw-logs",
            "-" + WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(),
            "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(), "test-workflow",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_VERSION.getName(), "1.0.0",
        };

        broker = new BrokerService();
        broker.addConnector(BROKER_URL);
        broker.setDataDirectory("target/activemq");
        broker.setBrokerName("localhost");
        broker.start();
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (broker.isStopped()) {
            broker.start(true);
        }
        broker.deleteAllMessages();
        broker.stop();
    }

    @Test
    public void testProcessMessageCreator() throws Exception {

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    // falcon message [FALCON_TOPIC_NAME] and user message ["FALCON." + ENTITY_NAME]
                    consumer(BROKER_URL, "FALCON.>", true);
                } catch (AssertionError e) {
                    error = e;
                } catch (JMSException ignore) {
                    error = null;
                }
            }
        };
        t.start();

        userBrokerUrl = BROKER_URL;

        latch.await();
        new FalconPostProcessing().run(args);
        t.join();
        if (error != null) {
            throw error;
        }
    }

    @Test
    public void testNoUserMessage() throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    // falcon message [FALCON_TOPIC_NAME] and user message ["FALCON." + ENTITY_NAME]
                    consumer(BROKER_URL, "FALCON.>", false);
                } catch (AssertionError e) {
                    error = e;
                } catch (JMSException ignore) {
                    error = null;
                }
            }
        };
        t.start();

        userNotification = "false";
        latch.await();
        new FalconPostProcessing().run(this.args);
        t.join();

        userNotification = "true";
        userBrokerUrl = ClusterHelper.NO_USER_BROKER_URL;
        latch.await();
        new FalconPostProcessing().run(this.args);
        t.join();

        if (error != null) {
            throw error;
        }
    }

    @Test
    public void testSystemMessage() throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    // falcon message [FALCON_TOPIC_NAME]
                    consumer(BROKER_URL, "FALCON.>", false);
                } catch (AssertionError e) {
                    error = e;
                } catch (JMSException ignore) {
                    error = null;
                }
            }
        };
        t.start();

        systemNotification = "false";
        latch.await();
        new FalconPostProcessing().run(this.args);
        t.join();

        systemNotification = "true";
        latch.await();
        new FalconPostProcessing().run(this.args);
        t.join();

        if (error != null) {
            throw error;
        }
    }

    private void consumer(String brokerUrl, String topic, boolean checkUserMessage) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(topic);
        MessageConsumer consumer = session.createConsumer(destination);

        latch.countDown();

        // Verify user message
        if (checkUserMessage) {
            verifyMessage(consumer);
        }

        connection.close();
    }

    private void verifyMessage(MessageConsumer consumer) throws JMSException {

        String[] actualFeedNames = new String[outputFeedPaths.length];
        String[] actualFeedPaths = new String[outputFeedPaths.length];
        for (int index = 0; index < outputFeedPaths.length; ++index) {
            // receive call is blocking
            MapMessage m = (MapMessage) consumer.receive();

            System.out.println("Received JMS message {}" + m.toString());
            System.out.println("Consumed: " + m.toString());
            assertMessage(m);
            actualFeedNames[index] = m.getString(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName());
            actualFeedPaths[index] = m.getString(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName());
        }
        Assert.assertEqualsNoOrder(actualFeedNames, outputFeedNames);
        Assert.assertEqualsNoOrder(actualFeedPaths, outputFeedPaths);
    }

    private void assertMessage(MapMessage m) throws JMSException {
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.ENTITY_NAME.getName()), "agg-coord");
        String workflowUser = m.getString(WorkflowExecutionArgs.WORKFLOW_USER.getName());
        if (workflowUser != null) { // in case of user message, its NULL
            Assert.assertEquals(workflowUser, FalconTestUtil.TEST_USER_1);
        }
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.NOMINAL_TIME.getName()), "2011-01-01-01-00");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.TIMESTAMP.getName()), "2012-01-01-01-00");
        Assert.assertEquals(m.getString(WorkflowExecutionArgs.STATUS.getName()), "SUCCEEDED");
    }

    @Test (expectedExceptions = JMSException.class)
    public void testFailuresInSendMessagesAreNotMasked() throws Exception {
        try {
            broker.stop();
        } catch (Exception ignored) {
            // ignore
        } finally {
            new FalconPostProcessing().run(getMessageArgs());
        }
    }

    private String[] getMessageArgs() {
        return new String[]{
            "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), ENTITY_NAME,
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), "out-click-logs,out-raw-logs",
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
            "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20",
            "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
            "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), FalconTestUtil.TEST_USER_1,
            "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
            "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), "2011-01-01-01-00",
            "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), "2012-01-01-01-00",
            "-" + WorkflowExecutionArgs.BRKR_URL.getName(), "error",
            "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS,
            "-" + WorkflowExecutionArgs.USER_BRKR_URL.getName(), "error",
            "-" + WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS,
            "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), "process",
            "-" + WorkflowExecutionArgs.OPERATION.getName(), "GENERATE",
            "-" + WorkflowExecutionArgs.LOG_FILE.getName(), "/logFile",
            "-" + WorkflowExecutionArgs.STATUS.getName(), "SUCCEEDED",
            "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "10",
            "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), "corp",
            "-" + WorkflowExecutionArgs.WF_ENGINE_URL.getName(), "http://localhost:11000/oozie/",
            "-" + WorkflowExecutionArgs.LOG_DIR.getName(), "target/log",
            "-" + WorkflowExecutionArgs.USER_SUBFLOW_ID.getName(), "userflow@wf-id" + "test",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(), "oozie",
            "-" + WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), "in-click-logs,in-raw-logs",
            "-" + WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(),
            "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(), "test-workflow",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_VERSION.getName(), "1.0.0",
        };
    }
}

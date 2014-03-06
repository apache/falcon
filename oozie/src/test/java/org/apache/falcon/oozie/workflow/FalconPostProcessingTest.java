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
import org.apache.falcon.workflow.FalconPostProcessing;
import org.apache.falcon.workflow.FalconPostProcessing.Arg;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.jms.*;

/**
 * Test for validating the falcon post processing utility.
 */
public class FalconPostProcessingTest {

    private String[] args;
    private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
    private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String FALCON_TOPIC_NAME = "FALCON.ENTITY.TOPIC";
    private static final String ENTITY_NAME = "agg-coord";
    private BrokerService broker;

    private volatile AssertionError error;

    @BeforeClass
    public void setup() throws Exception {
        args = new String[]{
            "-" + Arg.ENTITY_NAME.getOptionName(), ENTITY_NAME,
            "-" + Arg.FEED_NAMES.getOptionName(), "out-click-logs,out-raw-logs",
            "-" + Arg.FEED_INSTANCE_PATHS.getOptionName(),
            "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20",
            "-" + Arg.WORKFLOW_ID.getOptionName(), "workflow-01-00",
            "-" + Arg.WORKFLOW_USER.getOptionName(), "falcon",
            "-" + Arg.RUN_ID.getOptionName(), "1",
            "-" + Arg.NOMINAL_TIME.getOptionName(), "2011-01-01-01-00",
            "-" + Arg.TIMESTAMP.getOptionName(), "2012-01-01-01-00",
            "-" + Arg.BRKR_URL.getOptionName(), BROKER_URL,
            "-" + Arg.BRKR_IMPL_CLASS.getOptionName(), (BROKER_IMPL_CLASS),
            "-" + Arg.USER_BRKR_URL.getOptionName(), BROKER_URL,
            "-" + Arg.USER_BRKR_IMPL_CLASS.getOptionName(), (BROKER_IMPL_CLASS),
            "-" + Arg.ENTITY_TYPE.getOptionName(), ("process"),
            "-" + Arg.OPERATION.getOptionName(), ("GENERATE"),
            "-" + Arg.LOG_FILE.getOptionName(), ("/logFile"),
            "-" + Arg.STATUS.getOptionName(), ("SUCCEEDED"),
            "-" + Arg.BRKR_TTL.getOptionName(), "10",
            "-" + Arg.CLUSTER.getOptionName(), "corp",
            "-" + Arg.WF_ENGINE_URL.getOptionName(), "http://localhost:11000/oozie/",
            "-" + Arg.LOG_DIR.getOptionName(), "target/log",
            "-" + Arg.USER_SUBFLOW_ID.getOptionName(), "userflow@wf-id" + "test",
            "-" + Arg.USER_WORKFLOW_ENGINE.getOptionName(), "oozie",
            "-" + Arg.INPUT_FEED_NAMES.getOptionName(), "in-click-logs,in-raw-logs",
            "-" + Arg.INPUT_FEED_PATHS.getOptionName(),
            "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20",
            "-" + Arg.USER_WORKFLOW_NAME.getOptionName(), "test-workflow",
            "-" + Arg.USER_WORKFLOW_VERSION.getOptionName(), "1.0.0",
        };

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
    public void testProcessMessageCreator() throws Exception {

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    consumer(BROKER_URL, "FALCON." + ENTITY_NAME);
                    consumer(BROKER_URL, FALCON_TOPIC_NAME);
                } catch (AssertionError e) {
                    error = e;
                } catch (JMSException ignore) {
                    error = null;
                }
            }
        };
        t.start();
        Thread.sleep(1500);
        new FalconPostProcessing().run(this.args);
        t.join();
        if (error != null) {
            throw error;
        }
    }

    private void consumer(String brokerUrl, String topic) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(topic);
        MessageConsumer consumer = session.createConsumer(destination);

        // wait till you get atleast one message
        MapMessage m;
        for (m = null; m == null;) {
            m = (MapMessage) consumer.receive();
        }
        System.out.println("Consumed: " + m.toString());

        assertMessage(m);
        if (topic.equals(FALCON_TOPIC_NAME)) {
            Assert.assertEquals(m.getString(Arg.FEED_NAMES.getOptionName()),
                    "out-click-logs,out-raw-logs");
            Assert.assertEquals(m.getString(Arg.FEED_INSTANCE_PATHS.getOptionName()),
                    "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20");
        } else {
            Assert.assertEquals(m.getString(Arg.FEED_NAMES.getOptionName()), "out-click-logs");
            Assert.assertEquals(m.getString(Arg.FEED_INSTANCE_PATHS.getOptionName()),
                    "/out-click-logs/10/05/05/00/20");
        }

        connection.close();
    }

    private void assertMessage(MapMessage m) throws JMSException {
        Assert.assertEquals(m.getString(Arg.ENTITY_NAME.getOptionName()), "agg-coord");
        Assert.assertEquals(m.getString(Arg.WORKFLOW_ID.getOptionName()), "workflow-01-00");
        String workflowUser = m.getString(Arg.WORKFLOW_USER.getOptionName());
        if (workflowUser != null) { // in case of user message, its NULL
            Assert.assertEquals(workflowUser, "falcon");
        }
        Assert.assertEquals(m.getString(Arg.RUN_ID.getOptionName()), "1");
        Assert.assertEquals(m.getString(Arg.NOMINAL_TIME.getOptionName()), "2011-01-01T01:00Z");
        Assert.assertEquals(m.getString(Arg.TIMESTAMP.getOptionName()), "2012-01-01T01:00Z");
        Assert.assertEquals(m.getString(Arg.STATUS.getOptionName()), "SUCCEEDED");
    }
}

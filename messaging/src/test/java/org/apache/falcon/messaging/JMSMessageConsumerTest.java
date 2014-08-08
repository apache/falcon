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
import org.apache.falcon.FalconException;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.mortbay.log.Log;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for FalconTopicSubscriber.
 */
public class JMSMessageConsumerTest {

    private static final String BROKER_URL = "vm://localhost";
    private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    private static final String TOPIC_NAME = "FALCON.ENTITY.TOPIC";
    private static final String SECONDARY_TOPIC_NAME = "FALCON.ENTITY.SEC.TOPIC";
    private BrokerService broker;

    @BeforeClass
    public void setup() throws Exception {
        broker = new BrokerService();
        broker.addConnector(BROKER_URL);
        broker.setDataDirectory("target/activemq");
        broker.setBrokerName("localhost");
        broker.start();
    }

    public void sendMessages(String topic) throws JMSException, FalconException, IOException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(topic);
        javax.jms.MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 0; i < 3; i++) {
            WorkflowExecutionContext context = WorkflowExecutionContext.create(
                    getMockFalconMessage(i), WorkflowExecutionContext.Type.POST_PROCESSING);
            context.serialize(WorkflowExecutionContext.getFilePath("/tmp/log", "process1"));

            MapMessage message = session.createMapMessage();
            for (Map.Entry<WorkflowExecutionArgs, String> entry : context.entrySet()) {
                message.setString(entry.getKey().getName(), entry.getValue());
            }

            Log.debug("Sending:" + message);
            producer.send(message);
        }

        WorkflowExecutionContext context = WorkflowExecutionContext.create(
                getMockFalconMessage(15), WorkflowExecutionContext.Type.POST_PROCESSING);
        context.serialize(WorkflowExecutionContext.getFilePath("/tmp/log", "process1"));

        MapMessage mapMessage = session.createMapMessage();
        for (Map.Entry<WorkflowExecutionArgs, String> entry : context.entrySet()) {
            mapMessage.setString(entry.getKey().getName(), entry.getValue());
        }

        Log.debug("Sending:" + mapMessage);
        producer.send(mapMessage);
    }

    private String[] getMockFalconMessage(int i) {
        Map<String, String> message = new HashMap<String, String>();
        message.put(WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS);
        message.put(WorkflowExecutionArgs.BRKR_URL.getName(), BROKER_URL);
        message.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), "cluster1");
        message.put(WorkflowExecutionArgs.ENTITY_NAME.getName(), "process1");
        message.put(WorkflowExecutionArgs.ENTITY_TYPE.getName(), "PROCESS");
        message.put(WorkflowExecutionArgs.FEED_INSTANCE_PATHS.getName(),
                "/clicks/hour/00/0" + i);
        message.put(WorkflowExecutionArgs.FEED_NAMES.getName(), "clicks");
        message.put(WorkflowExecutionArgs.LOG_FILE.getName(), "/logfile");
        message.put(WorkflowExecutionArgs.LOG_DIR.getName(), "/tmp/log");
        message.put(WorkflowExecutionArgs.NOMINAL_TIME.getName(), "2012-10-10-10-10");
        message.put(WorkflowExecutionArgs.OPERATION.getName(), "GENERATE");
        message.put(WorkflowExecutionArgs.RUN_ID.getName(), "0");
        message.put(WorkflowExecutionArgs.TIMESTAMP.getName(), "2012-10-10-10-1" + i);
        message.put(WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-" + i);
        message.put(WorkflowExecutionArgs.TOPIC_NAME.getName(), TOPIC_NAME);
        message.put(WorkflowExecutionArgs.STATUS.getName(), i != 15 ? "SUCCEEDED" : "FAILED");
        message.put(WorkflowExecutionArgs.WORKFLOW_USER.getName(), "falcon");

        String[] args = new String[message.size() * 2];
        int index = 0;
        for (Map.Entry<String, String> entry : message.entrySet()) {
            args[index++] = "-" + entry.getKey();
            args[index++] = entry.getValue();
        }

        return args;
    }

    @Test
    public void testSubscriber() {
        try {
            //Comma separated topics are supported in startup properties
            JMSMessageConsumer subscriber = new JMSMessageConsumer(BROKER_IMPL_CLASS, "", "",
                    BROKER_URL, TOPIC_NAME+","+SECONDARY_TOPIC_NAME, new WorkflowJobEndNotificationService());
            subscriber.startSubscriber();
            sendMessages(TOPIC_NAME);
            Assert.assertEquals(broker.getAdminView().getTotalEnqueueCount(), 9);

            sendMessages(SECONDARY_TOPIC_NAME);
            Assert.assertEquals(broker.getAdminView().getTotalEnqueueCount(), 17);
            Assert.assertEquals(broker.getAdminView().getTotalConsumerCount(), 2);
            subscriber.closeSubscriber();
        } catch (Exception e) {
            Assert.fail("This should not have thrown an exception.", e);
        }
    }

    @AfterClass
    public void tearDown() throws Exception {
        broker.deleteAllMessages();
        broker.stop();
    }
}

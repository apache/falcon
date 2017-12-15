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

import java.io.File;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.mockito.Mockito;
import org.mortbay.log.Log;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
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
    private static final String DATA_DIRECTORY = "target/activemq";
    private BrokerService broker;

    private JMSMessageConsumer subscriber;
    private WorkflowJobEndNotificationService jobEndService;

    @BeforeMethod
    public void setup() throws Exception {
        FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
        broker = new BrokerService();
        broker.addConnector(BROKER_URL);
        broker.setDataDirectory(DATA_DIRECTORY);
        broker.setBrokerName("localhost");
        jobEndService = Mockito.mock(WorkflowJobEndNotificationService.class);
        broker.start();
        //Comma separated topics are supported in startup properties
        subscriber = new JMSMessageConsumer(BROKER_IMPL_CLASS, "", "",
                BROKER_URL, TOPIC_NAME + "," + SECONDARY_TOPIC_NAME, jobEndService);

        subscriber.startSubscriber();
        Process mockProcess = new Process();
        mockProcess.setName("process1");
        ConfigurationStore.get().publish(EntityType.PROCESS, mockProcess);
    }

    public void sendMessages(String topic, WorkflowExecutionContext.Type type)
        throws JMSException, FalconException, IOException {
        sendMessages(topic, type, true);
    }

    public void sendMessages(String topic, WorkflowExecutionContext.Type type, boolean isFalconWF)
        throws JMSException, FalconException, IOException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic(topic);
        javax.jms.MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 0; i < 5; i++) {
            Message message = null;

            switch(type) {
            case POST_PROCESSING:
                message = getMockFalconMessage(i, session);
                break;
            case WORKFLOW_JOB:
                message = getMockOozieMessage(i, session, isFalconWF);
                break;
            case COORDINATOR_ACTION:
                message = getMockOozieCoordMessage(i, session, isFalconWF);
            default:
                break;
            }
            Log.debug("Sending:" + message);
            producer.send(message);
        }
    }

    private Message getMockOozieMessage(int i, Session session, boolean isFalconWF)
        throws FalconException, JMSException {
        TextMessage message = session.createTextMessage();
        message.setStringProperty("appType", "WORKFLOW_JOB");
        if (isFalconWF) {
            message.setStringProperty("appName", "FALCON_PROCESS_DEFAULT_process1");
        } else {
            message.setStringProperty("appName", "OozieSampleShellWF");
        }
        message.setStringProperty("user", "falcon");
        switch(i % 4) {
        case 0:
            message.setText("{\"status\":\"RUNNING\",\"id\":\"0000042-130618221729631-oozie-oozi-W\""
                    + ",\"startTime\":1342915200000}");
            break;
        case 1:
            message.setText("{\"status\":\"FAILED\",\"errorCode\":\"EL_ERROR\","
                    + "\"errorMessage\":\"variable [dummyvalue] cannot be resolved\","
                    + "\"id\":\"0000042-130618221729631-oozie-oozi-W\",\"startTime\":1342915200000,"
                    + "\"endTime\":1366672183543}");
            break;
        case 2:
            message.setText("{\"status\":\"SUCCEEDED\",\"id\":\"0000039-130618221729631-oozie-oozi-W\""
                    + ",\"startTime\":1342915200000,"
                    + "\"parentId\":\"0000025-130618221729631-oozie-oozi-C@1\",\"endTime\":1366676224154}");
            break;
        case 3:
            message.setText("{\"status\":\"SUSPENDED\",\"id\":\"0000039-130618221729631-oozie-oozi-W\","
                    + "\"startTime\":1342915200000,\"parentId\":\"0000025-130618221729631-oozie-oozi-C@1\"}");
            break;
        default:
        }
        return message;
    }

    private Message getMockOozieCoordMessage(int i, Session session, boolean isFalconWF)
        throws FalconException, JMSException {
        TextMessage message = session.createTextMessage();
        message.setStringProperty("appType", "COORDINATOR_ACTION");
        if (isFalconWF) {
            message.setStringProperty("appName", "FALCON_PROCESS_DEFAULT_process1");
        } else {
            message.setStringProperty("appName", "OozieSampleShellWF");
        }
        message.setStringProperty("user", "falcon");
        switch(i % 5) {
        case 0:
            message.setText("{\"status\":\"WAITING\",\"nominalTime\":1310342400000,\"missingDependency\""
                    + ":\"hdfs://gsbl90107.blue.com:8020/user/john/dir1/file1\","
                    + "\"id\":\"0000025-130618221729631-oozie-oozi-C@1\",\"startTime\":1342915200000,"
                    + "\"parentId\":\"0000025-130618221729631-oozie-oozi-C\"}");
            message.setStringProperty("eventStatus", "WAITING");
            break;
        case 1:
            message.setText("{\"status\":\"RUNNING\",\"nominalTime\":1310342400000,"
                    + "\"id\":\"0000025-130618221729631-oozie-oozi-C@1\","
                    + "\"startTime\":1342915200000,\"parentId\":\"0000025-130618221729631-oozie-oozi-C\"}");
            message.setStringProperty("eventStatus", "STARTED");
            break;
        case 2:
            message.setText("{\"status\":\"SUCCEEDED\",\"nominalTime\":1310342400000,"
                    + "\"id\":\"0000025-130618221729631-oozie-oozi-C@1\","
                    + "\"startTime\":1342915200000,\"parentId\":\"0000025-130618221729631-oozie-oozi-C\","
                    + "\"endTime\":1366677082799}");
            message.setStringProperty("eventStatus", "SUCCESS");
            break;
        case 3:
            message.setText("{\"status\":\"FAILED\",\"errorCode\":\"E0101\",\"errorMessage\":"
                    + "\"dummyError\",\"nominalTime\":1310342400000,"
                    + "\"id\":\"0000025-130618221729631-oozie-oozi-C@1\",\"startTime\":1342915200000,"
                    + "\"parentId\":\"0000025-130618221729631-oozie-oozi-C\",\"endTime\":1366677140818}");
            message.setStringProperty("eventStatus", "FAILURE");
            break;
        case 4:
            message.setText("{\"status\":\"TIMEDOUT\",\"nominalTime\":1310342400000,"
                    + "\"id\":\"0000025-130618221729631-oozie-oozi-C@1\",\"startTime\":1342915200000,"
                    + "\"parentId\":\"0000025-130618221729631-oozie-oozi-C\",\"endTime\":1366677140818}");
            message.setStringProperty("eventStatus", "FAILURE");
        default:
        }
        return message;
    }

    private Message getMockFalconMessage(int i, Session session) throws FalconException, JMSException {
        Map<String, String> message = new HashMap<String, String>();
        message.put(WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER_IMPL_CLASS);
        message.put(WorkflowExecutionArgs.BRKR_URL.getName(), BROKER_URL);
        message.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), "cluster1");
        message.put(WorkflowExecutionArgs.ENTITY_NAME.getName(), "process1");
        message.put(WorkflowExecutionArgs.ENTITY_TYPE.getName(), "PROCESS");
        message.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
                "/clicks/hour/00/0" + i);
        message.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), "clicks");
        message.put(WorkflowExecutionArgs.LOG_FILE.getName(), "/logfile");
        message.put(WorkflowExecutionArgs.LOG_DIR.getName(), "/tmp/falcon-log");
        message.put(WorkflowExecutionArgs.NOMINAL_TIME.getName(), "2012-10-10-10-10");
        message.put(WorkflowExecutionArgs.OPERATION.getName(), "GENERATE");
        message.put(WorkflowExecutionArgs.RUN_ID.getName(), "0");
        message.put(WorkflowExecutionArgs.TIMESTAMP.getName(), "2012-10-10-10-1" + i);
        message.put(WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-" + i);
        message.put(WorkflowExecutionArgs.TOPIC_NAME.getName(), TOPIC_NAME);
        message.put(WorkflowExecutionArgs.STATUS.getName(), i != 15 ? "SUCCEEDED" : "FAILED");
        message.put(WorkflowExecutionArgs.WORKFLOW_USER.getName(), FalconTestUtil.TEST_USER_1);

        String[] args = new String[message.size() * 2];
        int index = 0;
        for (Map.Entry<String, String> entry : message.entrySet()) {
            args[index++] = "-" + entry.getKey();
            args[index++] = entry.getValue();
        }

        WorkflowExecutionContext context = WorkflowExecutionContext.create(
                args, WorkflowExecutionContext.Type.POST_PROCESSING);

        MapMessage jmsMessage = session.createMapMessage();
        for (Map.Entry<WorkflowExecutionArgs, String> entry : context.entrySet()) {
            jmsMessage.setString(entry.getKey().getName(), entry.getValue());
        }

        return jmsMessage;
    }

    @Test
    public void testSubscriber() {
        try {
            sendMessages(TOPIC_NAME, WorkflowExecutionContext.Type.POST_PROCESSING);

            final BrokerView adminView = broker.getAdminView();
            Assert.assertEquals(adminView.getTotalConsumerCount(), 2);

            sendMessages(SECONDARY_TOPIC_NAME, WorkflowExecutionContext.Type.POST_PROCESSING);

            Assert.assertEquals(adminView.getTotalConsumerCount(), 3);
        } catch (Exception e) {
            Assert.fail("This should not have thrown an exception.", e);
        }
    }

    @Test
    public void testJMSMessagesFromOozie() throws Exception {
        sendMessages(TOPIC_NAME, WorkflowExecutionContext.Type.WORKFLOW_JOB);

        final BrokerView adminView = broker.getAdminView();
        Assert.assertEquals(adminView.getTotalConsumerCount(), 2);

        // Async operations. Give some time for messages to be processed.
        Thread.sleep(100);
        Mockito.verify(jobEndService, Mockito.times(2)).notifyStart(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService).notifyFailure(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService).notifySuccess(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService).notifySuspend(Mockito.any(WorkflowExecutionContext.class));
    }

    @Test
    public void testJMSMessagesForOozieCoord() throws Exception {
        sendMessages(TOPIC_NAME, WorkflowExecutionContext.Type.COORDINATOR_ACTION);

        final BrokerView adminView = broker.getAdminView();
        Assert.assertEquals(adminView.getTotalConsumerCount(), 2);

        // Async operations. Give some time for messages to be processed.
        Thread.sleep(100);
        Mockito.verify(jobEndService, Mockito.never()).notifyStart(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.never()).notifySuccess(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.never()).notifySuspend(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.times(1)).notifyWait(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.times(1)).notifyFailure(Mockito.any(WorkflowExecutionContext.class));
    }

    @AfterMethod
    public void tearDown() throws Exception{
        ConfigurationStore.get().remove(EntityType.PROCESS, "process1");
        broker.stop();
        subscriber.closeSubscriber();
        FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    }

    @Test
    public void testJMSMessagesFromOozieForNonFalconWF() throws Exception {
        sendMessages(TOPIC_NAME, WorkflowExecutionContext.Type.WORKFLOW_JOB, false /* isFalconWF */);

        final BrokerView adminView = broker.getAdminView();
        Assert.assertEquals(adminView.getTotalConsumerCount(), 2);

        Thread.sleep(100);
        Mockito.verify(jobEndService, Mockito.never()).notifyStart(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.never()).notifySuccess(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.never()).notifySuspend(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.never()).notifyWait(Mockito.any(WorkflowExecutionContext.class));
        Mockito.verify(jobEndService, Mockito.never()).notifyFailure(Mockito.any(WorkflowExecutionContext.class));
    }

}

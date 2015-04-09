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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.messaging.util.MessagingUtil;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Subscribes to the falcon topic for handling retries and alerts.
 */
public class JMSMessageConsumer implements MessageListener, ExceptionListener {
    private static final Logger LOG = LoggerFactory.getLogger(JMSMessageConsumer.class);

    private static final String FALCON_CLIENT_ID = "falcon-server";

    private final String implementation;
    private final String userName;
    private final String password;
    private final String url;
    private final String topicName;
    private final WorkflowJobEndNotificationService jobEndNotificationService;

    private Connection connection;
    private TopicSession topicSession;
    private TopicSubscriber topicSubscriber;

    public JMSMessageConsumer(String implementation, String userName,
                              String password, String url, String topicName,
                              WorkflowJobEndNotificationService jobEndNotificationService) {
        this.implementation = implementation;
        this.userName = userName;
        this.password = password;
        this.url = url;
        this.topicName = topicName;
        this.jobEndNotificationService = jobEndNotificationService;
    }

    public void startSubscriber() throws FalconException {
        try {
            connection = createAndGetConnection(implementation, userName, password, url);
            connection.setClientID(FALCON_CLIENT_ID);

            topicSession = (TopicSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = topicSession.createTopic(topicName);
            topicSubscriber = topicSession.createDurableSubscriber(destination, FALCON_CLIENT_ID);
            topicSubscriber.setMessageListener(this);

            connection.setExceptionListener(this);
            connection.start();
        } catch (Exception e) {
            LOG.error("Error starting topicSubscriber of topic: " + this.toString(), e);
            throw new FalconException(e);
        }
    }

    @Override
    public void onMessage(Message message) {
        MapMessage mapMessage = (MapMessage) message;
        LOG.info("Received JMS message {}", message.toString());

        try {
            WorkflowExecutionContext context = createContext(mapMessage);
            LOG.info("Created context from JMS message {}", context);

            // Login the user so listeners can access FS and WfEngine as this user
            CurrentUser.authenticate(context.getWorkflowUser());

            if (context.hasWorkflowFailed()) {
                onFailure(context);
            } else if (context.hasWorkflowSucceeded()) {
                onSuccess(context);
            }
        } catch (JMSException e) {
            String errorMessage = "Error in onMessage for topicSubscriber of topic: "
                    + topicName + ", Message: " + message.toString();
            LOG.info(errorMessage, e);
            GenericAlert.alertJMSMessageConsumerFailed(errorMessage, e);
        }
    }

    private WorkflowExecutionContext createContext(MapMessage mapMessage) throws JMSException {
        // for backwards compatibility, read all args from message
        Map<WorkflowExecutionArgs, String> wfProperties = new HashMap<WorkflowExecutionArgs, String>();
        for (WorkflowExecutionArgs arg : WorkflowExecutionArgs.values()) {
            String optionValue = mapMessage.getString(arg.getName());
            if (StringUtils.isNotEmpty(optionValue)) {
                wfProperties.put(arg, optionValue);
            }
        }

        return WorkflowExecutionContext.create(wfProperties);
    }

    public void onFailure(WorkflowExecutionContext context) {
        jobEndNotificationService.notifyFailure(context);
    }

    public void onSuccess(WorkflowExecutionContext context) {
        jobEndNotificationService.notifySuccess(context);
    }

    @Override
    public void onException(JMSException ignore) {
        String errorMessage = "Error in onException for topicSubscriber of topic: " + topicName;
        LOG.info(errorMessage, ignore);
        GenericAlert.alertJMSMessageConsumerFailed(errorMessage, ignore);
    }

    public void closeSubscriber() {
        LOG.info("Closing topicSubscriber on topic : " + this.topicName);
        // closing each quietly so client id can be unsubscribed
        MessagingUtil.closeQuietly(topicSubscriber);
        MessagingUtil.closeQuietly(topicSession, FALCON_CLIENT_ID);
        MessagingUtil.closeQuietly(connection);
    }

    private static Connection createAndGetConnection(String implementation,
                                                     String userName, String password, String url)
        throws JMSException, ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        @SuppressWarnings("unchecked")
        Class<ConnectionFactory> clazz = (Class<ConnectionFactory>)
                JMSMessageConsumer.class.getClassLoader().loadClass(implementation);

        ConnectionFactory connectionFactory = clazz.getConstructor(
                String.class, String.class, String.class).newInstance(userName,
                password, url);

        return connectionFactory.createConnection();
    }

    @Override
    public String toString() {
        return topicName;
    }
}

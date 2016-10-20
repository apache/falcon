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
import org.apache.falcon.Pair;
import org.apache.falcon.Tag;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.WorkflowNameBuilder;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.messaging.util.MessagingUtil;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.json.JSONException;
import org.json.JSONObject;
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
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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
        LOG.info("Received JMS message {}", message.toString());
        try {
            if (message instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) message;
                WorkflowExecutionContext context = createContext(mapMessage);
                LOG.info("Created context from Falcon JMS message {}", context);
                invokeListener(context);
            // Due to backward compatibility, need to handle messages from post processing too.
            // Hence cannot use JMS selectors.
            } else if (shouldHandle(message)) {
                TextMessage textMessage = (TextMessage) message;
                WorkflowExecutionContext context = createContext(textMessage);
                LOG.info("Created context from Oozie JMS message {}", context);
                invokeListener(context);
            }
        } catch (Exception e) {
            String errorMessage = "Error in onMessage for topicSubscriber of topic: "
                    + topicName + ", Message: " + message.toString();
            LOG.info(errorMessage, e);
            GenericAlert.alertJMSMessageConsumerFailed(errorMessage, e);
        }
    }

    // Creates context from the JMS notification of the workflow engine
    private WorkflowExecutionContext createContext(TextMessage message) throws JMSException, FalconException {
        try {
            // Example Workflow Job in FAILED state:
            // {"status":"FAILED","errorCode":"EL_ERROR","errorMessage":"variable [dummyvalue] cannot be resolved",
            //        "id":"0000042-130618221729631-oozie-oozi-W","startTime":1342915200000,"endTime":1366672183543}
            JSONObject json = new JSONObject(message.getText());
            long currentTime = System.currentTimeMillis();
            Map<WorkflowExecutionArgs, String> wfProperties = new HashMap<>();
            wfProperties.put(WorkflowExecutionArgs.STATUS, json.getString("status"));
            wfProperties.put(WorkflowExecutionArgs.WORKFLOW_ID, json.getString("id"));
            wfProperties.put(WorkflowExecutionArgs.WF_START_TIME, json.isNull("startTime")? Long.toString(currentTime)
                    : json.getString("startTime"));
            wfProperties.put(WorkflowExecutionArgs.WF_END_TIME, json.isNull("endTime")? Long.toString(currentTime)
                    : json.getString("endTime"));
            if (!json.isNull("nominalTime")) {
                wfProperties.put(WorkflowExecutionArgs.NOMINAL_TIME,
                        getNominalTimeString(Long.parseLong(json.getString("nominalTime"))));
            }
            if (!json.isNull("parentId")) {
                wfProperties.put(WorkflowExecutionArgs.PARENT_ID, json.getString("parentId"));
            }
            String appName = message.getStringProperty("appName");
            Pair<String, EntityType> entityTypePair = WorkflowNameBuilder.WorkflowName.getEntityNameAndType(appName);
            wfProperties.put(WorkflowExecutionArgs.ENTITY_NAME, entityTypePair.first);
            wfProperties.put(WorkflowExecutionArgs.ENTITY_TYPE, entityTypePair.second.name());
            wfProperties.put(WorkflowExecutionArgs.WORKFLOW_USER, message.getStringProperty("user"));
            wfProperties.put(WorkflowExecutionArgs.OPERATION, getOperation(appName).name());
            wfProperties.put(WorkflowExecutionArgs.USER_SUBFLOW_ID,
                    json.getString("id").concat("@user-action"));
            String appType = message.getStringProperty("appType");
            return WorkflowExecutionContext.create(wfProperties, WorkflowExecutionContext.Type.valueOf(appType));

        } catch (JSONException e) {
            throw new FalconException("Unable to build a context from the JMS message.", e);
        }
    }

    // Retrieves EntityOperation from the workflow name
    private WorkflowExecutionContext.EntityOperations getOperation(String appName) {
        Tag tag = WorkflowNameBuilder.WorkflowName.getTagAndSuffixes(appName).first;
        switch(tag) {
        case REPLICATION:
            return WorkflowExecutionContext.EntityOperations.REPLICATE;
        case RETENTION:
            return WorkflowExecutionContext.EntityOperations.DELETE;
        case IMPORT:
            return WorkflowExecutionContext.EntityOperations.IMPORT;
        case EXPORT:
            return WorkflowExecutionContext.EntityOperations.EXPORT;
        case DEFAULT:
            return WorkflowExecutionContext.EntityOperations.GENERATE;
        default:
            throw new IllegalArgumentException("Invalid tag - " + tag);
        }
    }

    private String getNominalTimeString(long timeInMillis) {
        Date time = new Date(timeInMillis);
        final String format = "yyyy-MM-dd-HH-mm";
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(time);
    }

    private void invokeListener(WorkflowExecutionContext context) throws FalconException {
        // Login the user so listeners can access FS and WfEngine as this user
        CurrentUser.authenticate(context.getWorkflowUser());

        WorkflowExecutionContext.Status status = WorkflowExecutionContext.Status.valueOf(
                context.getValue(WorkflowExecutionArgs.STATUS));

        // Handle only timeout and wait notifications of coord
        if (context.getContextType() == WorkflowExecutionContext.Type.COORDINATOR_ACTION) {
            switch(status) {
            case TIMEDOUT:
                jobEndNotificationService.notifyFailure(context);
                break;
            case WAITING:
                jobEndNotificationService.notifyWait(context);
                break;
            default:
                break;
            }
        } else {
            switch(status) {
            case KILLED:
            case FAILED:
                jobEndNotificationService.notifyFailure(context);
                break;
            case SUCCEEDED:
                jobEndNotificationService.notifySuccess(context);
                break;
            case SUSPENDED:
                jobEndNotificationService.notifySuspend(context);
                break;
            case RUNNING:
                jobEndNotificationService.notifyStart(context);
                break;
            default :
                throw new IllegalArgumentException("Not valid Status of workflow");
            }
        }
    }

    // Since Oozie has a system level JMS connection info, Falcon should ensure it is handling notifications
    // of Falcon entities only.
    private boolean shouldHandle(Message message) {
        try {
            String appType = message.getStringProperty("appType");
            // Handle all workflow job notifications for falcon workflows
            if (appType != null
                    && WorkflowExecutionContext.Type.WORKFLOW_JOB == WorkflowExecutionContext.Type.valueOf(appType)
                    && WorkflowNameBuilder.WorkflowName.getEntityNameAndType(
                        message.getStringProperty("appName")) != null) {
                return true;
            }

            // Handle coord notification for falcon workflows only for WAITING and TIMED_OUT.
            if (appType != null
                    && WorkflowExecutionContext.Type.COORDINATOR_ACTION
                        == WorkflowExecutionContext.Type.valueOf(appType)
                    && WorkflowNameBuilder.WorkflowName.getEntityNameAndType(
                        message.getStringProperty("appName")) != null) {
                String status = message.getStringProperty("eventStatus");
                if (status != null && ("WAITING".equals(status) || "FAILURE".equals(status))) {
                    return true;
                }
            }
        } catch (JMSException e) {
            LOG.error("Error while parsing the message header", e);
        }
        return false;
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

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
package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.metadata.MetadataMappingService;
import org.apache.falcon.rerun.event.RerunEvent.RerunType;
import org.apache.falcon.rerun.handler.AbstractRerunHandler;
import org.apache.falcon.rerun.handler.RerunHandlerFactory;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.log4j.Logger;

import javax.jms.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;

/**
 * Subscribes to the falcon topic for handling retries and alerts.
 */
public class FalconTopicSubscriber implements MessageListener, ExceptionListener {
    private static final Logger LOG = Logger.getLogger(FalconTopicSubscriber.class);

    private final String implementation;
    private final String userName;
    private final String password;
    private final String url;
    private final String topicName;

    private Connection connection;
    private TopicSubscriber subscriber;

    private AbstractRerunHandler retryHandler = RerunHandlerFactory.getRerunHandler(RerunType.RETRY);
    private AbstractRerunHandler latedataHandler = RerunHandlerFactory.getRerunHandler(RerunType.LATE);

    public FalconTopicSubscriber(String implementation, String userName,
                                 String password, String url, String topicName) {
        this.implementation = implementation;
        this.userName = userName;
        this.password = password;
        this.url = url;
        this.topicName = topicName;
    }

    public void startSubscriber() throws FalconException {
        try {
            connection = createAndGetConnection(implementation, userName, password, url);
            TopicSession session = (TopicSession) connection.createSession(
                    false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic(topicName);
            subscriber = session.createSubscriber(destination);
            subscriber.setMessageListener(this);
            connection.setExceptionListener(this);
            connection.start();
        } catch (Exception e) {
            LOG.error("Error starting subscriber of topic: " + this.toString(), e);
            throw new FalconException(e);
        }
    }

    @Override
    public void onMessage(Message message) {
        MapMessage mapMessage = (MapMessage) message;
        try {
            if (LOG.isDebugEnabled()) {debug(mapMessage); }
            String cluster = mapMessage.getString(ARG.cluster.getArgName());
            String entityName = mapMessage.getString(ARG.entityName.getArgName());
            String entityType = mapMessage.getString(ARG.entityType.getArgName());
            String workflowId = mapMessage.getString(ARG.workflowId.getArgName());
            String workflowUser = mapMessage.getString(ARG.workflowUser.getArgName());
            String runId = mapMessage.getString(ARG.runId.getArgName());
            String nominalTime = mapMessage.getString(ARG.nominalTime.getArgName());
            String status = mapMessage.getString(ARG.status.getArgName());
            String operation = mapMessage.getString(ARG.operation.getArgName());

            CurrentUser.authenticate(workflowUser);
            AbstractWorkflowEngine wfEngine = WorkflowEngineFactory.getWorkflowEngine();
            InstancesResult result = wfEngine.getJobDetails(cluster, workflowId);
            Date startTime = result.getInstances()[0].startTime;
            Date endTime = result.getInstances()[0].endTime;
            Long duration = (endTime.getTime() - startTime.getTime()) * 1000000;

            if (status.equalsIgnoreCase("FAILED")) {
                retryHandler.handleRerun(cluster, entityType, entityName,
                        nominalTime, runId, workflowId, workflowUser,
                        System.currentTimeMillis());

                GenericAlert.instrumentFailedInstance(cluster, entityType,
                        entityName, nominalTime, workflowId, workflowUser, runId, operation,
                        SchemaHelper.formatDateUTC(startTime), "", "", duration);

            } else if (status.equalsIgnoreCase("SUCCEEDED")) {
                Entity entity = EntityUtil.getEntity(entityType, entityName);
                //late data handling not applicable for feed retention action
                if (!operation.equalsIgnoreCase("DELETE") && EntityUtil.getLateProcess(entity) != null) {
                    latedataHandler.handleRerun(cluster, entityType, entityName,
                        nominalTime, runId, workflowId, workflowUser,
                        System.currentTimeMillis());
                } else {
                    LOG.info("Late date handling not applicable for entityType: " + entityType + ", entityName: "
                        + entityName + " operation: " + operation);
                }
                GenericAlert.instrumentSucceededInstance(cluster, entityType,
                    entityName, nominalTime, workflowId, workflowUser, runId, operation,
                    SchemaHelper.formatDateUTC(startTime), duration);

                notifySLAService(cluster, entityName, entityType, nominalTime, duration);
                notifyMetadataMappingService(entityName, operation, mapMessage.getString(ARG.logDir.getArgName()));
            }
        } catch (JMSException e) {
            LOG.info("Error in onMessage for subscriber of topic: " + this.toString(), e);
        } catch (FalconException e) {
            LOG.info("Error in onMessage for subscriber of topic: " + this.toString(), e);
        } catch (Exception e) {
            LOG.info("Error in onMessage for subscriber of topic: " + this.toString(), e);
        }
    }

    private void notifySLAService(String cluster, String entityName,
                                  String entityType, String nominalTime, Long duration) {
        try {
            getSLAMonitoringService().notifyCompletion(EntityUtil.getEntity(entityType, entityName),
                    cluster, SchemaHelper.parseDateUTC(nominalTime), duration);
        } catch (Throwable e) {
            LOG.warn("Unable to notify SLA Service", e);
        }
    }

    private SLAMonitoringService getSLAMonitoringService() {
        return Services.get().getService(SLAMonitoringService.SERVICE_NAME);
    }

    private void notifyMetadataMappingService(String entityName, String operation,
                                              String logDir) throws FalconException {
        MetadataMappingService service = Services.get().getService(MetadataMappingService.SERVICE_NAME);
        service.onSuccessfulWorkflowCompletion(entityName, operation, logDir);
    }

    private void debug(MapMessage mapMessage) throws JMSException {
        StringBuilder buff = new StringBuilder();
        buff.append("Received:{");
        for (ARG arg : ARG.values()) {
            buff.append(arg.getArgName()).append('=')
                .append(mapMessage.getString(arg.getArgName())).append(", ");
        }
        buff.append("}");
        LOG.debug(buff);
    }

    @Override
    public void onException(JMSException ignore) {
        LOG.info("Error in onException for subscriber of topic: " + this.toString(), ignore);
    }

    public void closeSubscriber() throws FalconException {
        try {
            LOG.info("Closing subscriber on topic : " + this.topicName);
            if (subscriber != null) {
                subscriber.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            LOG.error("Error closing subscriber of topic: " + this.toString(), e);
            throw new FalconException(e);
        }
    }

    private static Connection createAndGetConnection(String implementation,
                                                     String userName, String password, String url)
        throws JMSException, ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        @SuppressWarnings("unchecked")
        Class<ConnectionFactory> clazz = (Class<ConnectionFactory>) FalconTopicSubscriber.class
                .getClassLoader().loadClass(implementation);

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

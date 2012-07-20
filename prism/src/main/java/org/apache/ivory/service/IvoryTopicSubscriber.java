/*
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
package org.apache.ivory.service;

import org.apache.ivory.IvoryException;
import org.apache.ivory.aspect.GenericAlert;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.messaging.EntityInstanceMessage.ARG;
import org.apache.ivory.rerun.event.RerunEvent.RerunType;
import org.apache.ivory.rerun.handler.AbstractRerunHandler;
import org.apache.ivory.rerun.handler.RerunHandlerFactory;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.workflow.WorkflowEngineFactory;
import org.apache.ivory.workflow.engine.AbstractWorkflowEngine;
import org.apache.log4j.Logger;

import javax.jms.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;

public class IvoryTopicSubscriber implements MessageListener, ExceptionListener {
	private static final Logger LOG = Logger
			.getLogger(IvoryTopicSubscriber.class);

	private TopicSubscriber subscriber;
	private String implementation;
	private String userName;
	private String password;
	private String url;
	private String topicName;
	private Connection connection;

	private AbstractRerunHandler retryHandler = RerunHandlerFactory
			.getRerunHandler(RerunType.RETRY);
	private AbstractRerunHandler latedataHandler = RerunHandlerFactory
			.getRerunHandler(RerunType.LATE);

	public IvoryTopicSubscriber(String implementation, String userName,
			String password, String url, String topicName) {
		this.implementation = implementation;
		this.userName = userName;
		this.password = password;
		this.url = url;
		this.topicName = topicName;
	}

	public void startSubscriber() throws IvoryException {
		try {
			connection = createAndGetConnection(implementation, userName,
					password, url);
			TopicSession session = (TopicSession) connection.createSession(
					false, Session.AUTO_ACKNOWLEDGE);
			Topic destination = session.createTopic(topicName);
			subscriber = session.createSubscriber(destination);
			subscriber.setMessageListener(this);
			connection.setExceptionListener(this);
			connection.start();
		} catch (Exception e) {
			LOG.error("Error starting subscriber of topic: " + this.toString(),
					e);
			throw new IvoryException(e);
		}
	}

	@Override
	public void onMessage(Message message) {
		MapMessage mapMessage = (MapMessage) message;
		try {
			debug(mapMessage);
			String cluster = mapMessage.getString(ARG.cluster.getArgName());
			String entityName = mapMessage.getString(ARG.entityName
					.getArgName());
			String entityType = mapMessage.getString(ARG.entityType
					.getArgName());
			String workflowId = mapMessage.getString(ARG.workflowId
					.getArgName());
			String runId = mapMessage.getString(ARG.runId.getArgName());
			String nominalTime = mapMessage.getString(ARG.nominalTime
					.getArgName());
			String status = mapMessage.getString(ARG.status.getArgName());
			String operation = mapMessage.getString(ARG.operation.getArgName());

			AbstractWorkflowEngine wfEngine = WorkflowEngineFactory.getWorkflowEngine();
			InstancesResult result = wfEngine
					.getJobDetails(cluster, workflowId);
            Date startTime = result.getInstances()[0].startTime;
            Date endTime = result.getInstances()[0].endTime;
            Long duration = (endTime.getTime() - startTime.getTime()) * 1000000;
			if (status.equalsIgnoreCase("FAILED")) {
				retryHandler.handleRerun(cluster, entityType, entityName,
						nominalTime, runId, workflowId,
						System.currentTimeMillis());
				GenericAlert.instrumentFailedInstance(cluster, entityType,
						entityName, nominalTime, workflowId, runId, operation,
                        SchemaHelper.formatDateUTC(startTime),
                        "", "", duration);
			} else if (status.equalsIgnoreCase("SUCCEEDED")) {
				latedataHandler.handleRerun(cluster, entityType, entityName,
						nominalTime, runId, workflowId,
						System.currentTimeMillis());
				GenericAlert.instrumentSucceededInstance(cluster, entityType,
                        entityName, nominalTime, workflowId, runId, operation,
                        SchemaHelper.formatDateUTC(startTime),
                        duration);
                notifySLAService(cluster, entityName, entityType, nominalTime, duration);
            }

		} catch (Exception ignore) {
			LOG.info(
					"Error in onMessage for subscriber of topic: "
							+ this.toString(), ignore);
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
        return (SLAMonitoringService) Services.get().getService(SLAMonitoringService.SERVICE_NAME);
    }

    private void debug(MapMessage mapMessage) throws JMSException {
        if (LOG.isDebugEnabled()) {
            StringBuffer buff = new StringBuffer();
            buff.append("Received:{");
            for (ARG arg : ARG.values()) {
                buff.append(arg.getArgName()).append('=').
                        append(mapMessage.getString(arg.getArgName())).
                        append(", ");
            }
            buff.append("}");
            LOG.debug(buff);
        }
    }

	@Override
	public void onException(JMSException ignore) {
		LOG.info(
				"Error in onException for subscriber of topic: "
						+ this.toString(), ignore);
	}

	public void closeSubscriber() throws IvoryException {
		try {
			LOG.info("Closing subscriber on topic : " + this.topicName);
			subscriber.close();
			connection.close();
		} catch (JMSException e) {
			LOG.error("Error closing subscriber of topic: " + this.toString(),
					e);
			throw new IvoryException(e);
		}
	}

	private static Connection createAndGetConnection(String implementation,
			String userName, String password, String url) throws JMSException,
			ClassNotFoundException, IllegalArgumentException,
			SecurityException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {

		@SuppressWarnings("unchecked")
		Class<ConnectionFactory> clazz = (Class<ConnectionFactory>) IvoryTopicSubscriber.class
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

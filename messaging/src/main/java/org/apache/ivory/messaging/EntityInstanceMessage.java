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

package org.apache.ivory.messaging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Value Object which is stored in JMS Topic as TextMessage
 * 
 */
public class EntityInstanceMessage {

	private final Map<ARG, String> msgs = new HashMap<ARG, String>();
	private static final String MSG_SEPERATOR = "$";
	private static final int ARG_LENGTH = EntityInstanceMessage.ARG.values().length;
	private static final Logger LOG = Logger
			.getLogger(EntityInstanceMessage.class);
	private static final String IVORY_PROCESS_TOPIC_NAME = "IVORY.PROCESS.TOPIC";

	/**
	 * Enum for arguments that are used in coordinators to pass arguments to
	 * parent workflow
	 */

	public enum entityOperation {
		GENERATE, DELETE, ARCHIVE, REPLICATE, CHMOD
	}

	public enum ARG {
		PROCESS_NAME(0, "entityName"), FEED_NAME(1, "feedNames"), FEED_INSTANCE_PATH(
				2, "feedInstancePaths"), WORKFLOW_ID(3, "workflowId"), RUN_ID(
				4, "runId"), NOMINAL_TIME(5, "nominalTime"), TIME_STAMP(6,
				"timeStamp"), BROKER_URL(7, "brokerUrl"), BROKER_IMPL_CLASS(8,
				"brokerImplClass"), ENTITY_TYPE(9, "entityType"), OPERATION(10,
				"operation"), LOG_FILE(11, "logFile"), TOPIC_NAME(12,
				"topicName"), STATUS(13, "status"), BROKER_TTL(14, "broker.ttlInMins");

		private int argOrder;
		private String argName;

		private ARG(int argOrder, String argName) {
			this.argOrder = argOrder;
			this.argName = argName;
		}

		public int ORDER() {
			return this.argOrder;
		}

		public String NAME() {
			return this.argName;
		}
	}

	public String getProcessName() {
		return this.msgs.get(ARG.PROCESS_NAME);
	}

	public void setProcessName(String processName) {
		this.msgs.put(ARG.PROCESS_NAME, processName);
	}

	public String getTopicName() {
		return this.msgs.get(ARG.TOPIC_NAME);
	}

	public void setTopicName(String topicName) {
		this.msgs.put(ARG.TOPIC_NAME, topicName);
	}

	public String getFeedName() {
		return this.msgs.get(ARG.FEED_NAME);
	}

	public void setFeedName(String feedName) {
		this.msgs.put(ARG.FEED_NAME, feedName);
	}

	public String getFeedInstancePath() {
		return this.msgs.get(ARG.FEED_INSTANCE_PATH);
	}

	public void setFeedInstancePath(String feedInstancePath) {
		this.msgs.put(ARG.FEED_INSTANCE_PATH, feedInstancePath);
	}

	public String getWorkflowId() {
		return this.msgs.get(ARG.WORKFLOW_ID);
	}

	public void setWorkflowId(String workflowId) {
		this.msgs.put(ARG.WORKFLOW_ID, workflowId);
	}

	public String getRunId() {
		return this.msgs.get(ARG.RUN_ID);
	}

	public void setRunId(String runId) {
		this.msgs.put(ARG.RUN_ID, runId);
	}

	public String getNominalTime() {
		return this.msgs.get(ARG.NOMINAL_TIME);
	}

	public void setNominalTime(String nominalTime) {
		this.msgs.put(ARG.NOMINAL_TIME, nominalTime);
	}

	public String getTimeStamp() {
		return this.msgs.get(ARG.TIME_STAMP);
	}

	public void setTimeStamp(String timeStamp) {
		this.msgs.put(ARG.TIME_STAMP, timeStamp);
	}

	public String getBrokerUrl() {
		return this.msgs.get(ARG.BROKER_URL);
	}

	public void setBrokerUrl(String brokerUrl) {
		this.msgs.put(ARG.BROKER_URL, brokerUrl);
	}

	public String getBrokerImplClass() {
		return this.msgs.get(ARG.BROKER_IMPL_CLASS);
	}

	public void setBrokerImplClass(String brokerImplClass) {
		this.msgs.put(ARG.BROKER_IMPL_CLASS, brokerImplClass);
	}

	public String getEntityType() {
		return this.msgs.get(ARG.ENTITY_TYPE);
	}

	public void setEntityType(String entityType) {
		this.msgs.put(ARG.ENTITY_TYPE, entityType);
	}

	public String getOperation() {
		return this.msgs.get(ARG.OPERATION);
	}

	public void setOperation(String operation) {
		this.msgs.put(ARG.OPERATION, operation);
	}

	public String getLogFile() {
		return this.msgs.get(ARG.LOG_FILE);
	}

	public void setLogFile(String logFile) {
		this.msgs.put(ARG.LOG_FILE, logFile);
	}

	public String getStatus() {
		return this.msgs.get(ARG.STATUS);
	}

	public void setStatus(String status) {
		this.msgs.put(ARG.STATUS, status);
	}
	
	public String getBrokerTTL() {
		return this.msgs.get(ARG.BROKER_TTL);
	}

	public void setBrokerTTL(String brokerTTL) {
		this.msgs.put(ARG.BROKER_TTL, brokerTTL);
	}

	@Override
	public String toString() {
		if (getEntityType().equalsIgnoreCase("PROCESS")
				&& getTopicName().equals(IVORY_PROCESS_TOPIC_NAME)) {
			return getIvoryMessage();
		}
		if (getEntityType().equalsIgnoreCase("FEED")) {
			return getFeedMessage();
		}
		return getProcessMessage();

	}

	private String getProcessMessage() {
		return getProcessName() + MSG_SEPERATOR + getFeedName() + MSG_SEPERATOR
				+ getFeedInstancePath() + MSG_SEPERATOR + getWorkflowId()
				+ MSG_SEPERATOR + getRunId() + MSG_SEPERATOR + getNominalTime()
				+ MSG_SEPERATOR + getTimeStamp();
	}

	private String getIvoryMessage() {
		return getProcessName() + MSG_SEPERATOR + getFeedName() + MSG_SEPERATOR
				+ getFeedInstancePath() + MSG_SEPERATOR + getWorkflowId()
				+ MSG_SEPERATOR + getRunId() + MSG_SEPERATOR + getNominalTime()
				+ MSG_SEPERATOR + getTimeStamp() + MSG_SEPERATOR + getStatus();
	}

	private String getFeedMessage() {
		return getFeedName() + MSG_SEPERATOR
				+ getFeedInstancePath() + MSG_SEPERATOR + getOperation()
				+ MSG_SEPERATOR + getWorkflowId() + MSG_SEPERATOR + getRunId()
				+ MSG_SEPERATOR + getNominalTime() + MSG_SEPERATOR
				+ getTimeStamp();
	}

	/**
	 * 
	 * @param args
	 *            - String array passed from oozie action for jms messaging
	 * @return ProcessMessage - Value object which is stored in JMS topic
	 */
	public static EntityInstanceMessage[] argsToMessage(String[] args) {

		assert args.length == ARG_LENGTH : "Required number of arguments: "
				+ ARG_LENGTH;

		String[] feedNames = getFeedNames(args);

		String[] feedPaths;
		try {
			feedPaths = getFeedPaths(args);
		} catch (IOException e) {
			LOG.error("Error getting instance paths: ", e);
			throw new RuntimeException(e);
		}

		EntityInstanceMessage[] processMessages = new EntityInstanceMessage[feedPaths.length];
		for (int i = 0; i < feedPaths.length; i++) {
			EntityInstanceMessage instanceMessage = new EntityInstanceMessage();
			instanceMessage
					.setProcessName(args[EntityInstanceMessage.ARG.PROCESS_NAME
							.ORDER()]);
			if (args[EntityInstanceMessage.ARG.ENTITY_TYPE.ORDER()]
					.equalsIgnoreCase("PROCESS")) {
				instanceMessage.setFeedName(feedNames[i]);
			} else {
				instanceMessage
						.setFeedName(args[EntityInstanceMessage.ARG.FEED_NAME
								.ORDER()]);
			}
			instanceMessage.setFeedInstancePath(feedPaths[i]);
			instanceMessage
					.setWorkflowId(args[EntityInstanceMessage.ARG.WORKFLOW_ID
							.ORDER()]);
			instanceMessage.setRunId(args[EntityInstanceMessage.ARG.RUN_ID
					.ORDER()]);
			instanceMessage
					.setNominalTime(args[EntityInstanceMessage.ARG.NOMINAL_TIME
							.ORDER()]);
			instanceMessage
					.setTimeStamp(args[EntityInstanceMessage.ARG.TIME_STAMP
							.ORDER()]);
			instanceMessage
					.setBrokerUrl(args[EntityInstanceMessage.ARG.BROKER_URL
							.ORDER()]);
			instanceMessage
					.setBrokerImplClass(args[EntityInstanceMessage.ARG.BROKER_IMPL_CLASS
							.ORDER()]);
			instanceMessage
					.setEntityType(args[EntityInstanceMessage.ARG.ENTITY_TYPE
							.ORDER()]);
			instanceMessage
					.setOperation(args[EntityInstanceMessage.ARG.OPERATION
							.ORDER()]);
			instanceMessage.setLogFile(args[EntityInstanceMessage.ARG.LOG_FILE
					.ORDER()]);
			instanceMessage
					.setTopicName(args[EntityInstanceMessage.ARG.TOPIC_NAME
							.ORDER()]);
			instanceMessage.setStatus(args[EntityInstanceMessage.ARG.STATUS
					.ORDER()]);
			instanceMessage.setBrokerTTL(args[EntityInstanceMessage.ARG.BROKER_TTL
			           					.ORDER()]);

			processMessages[i] = instanceMessage;
		}
		return processMessages;
	}

	private static String[] getFeedNames(String[] args) {
		String topicName = args[ARG.TOPIC_NAME.argOrder];
		if (topicName.equals(IVORY_PROCESS_TOPIC_NAME)) {
			return new String[] { args[EntityInstanceMessage.ARG.FEED_NAME
					.ORDER()] };
		}
		return args[EntityInstanceMessage.ARG.FEED_NAME.ORDER()].split(",");
	}

	private static String[] getFeedPaths(String[] args) throws IOException {
		String entityType = args[EntityInstanceMessage.ARG.ENTITY_TYPE.ORDER()];
		String topicName = args[EntityInstanceMessage.ARG.TOPIC_NAME.ORDER()];

		if (entityType.equalsIgnoreCase("PROCESS")
				&& topicName.equals(IVORY_PROCESS_TOPIC_NAME)) {
			LOG.debug("Returning instance paths for Ivory Topic: "
					+ args[EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.ORDER()]);
			return new String[] { args[EntityInstanceMessage.ARG.FEED_INSTANCE_PATH
					.ORDER()] };
		}

		if (entityType.equalsIgnoreCase("PROCESS")) {
			LOG.debug("Returning instance paths for process: "
					+ args[EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.ORDER()]);
			return args[EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.ORDER()]
					.split(",");
		}
		//
		Path logFile = new Path(
				args[EntityInstanceMessage.ARG.LOG_FILE.ORDER()]);
		FileSystem fs = FileSystem.get(logFile.toUri(), new Configuration());
		ByteArrayOutputStream writer = new ByteArrayOutputStream();
		InputStream instance = fs.open(logFile);
		IOUtils.copyBytes(instance, writer, 4096, true);
		String[] instancePaths = writer.toString().split("=");
		if (instancePaths.length == 1) {
			LOG.debug("Returning 0 instance paths for feed ");
			return new String[0];
		} else {
			LOG.debug("Returning instance paths for feed " + instancePaths[1]);
			return instancePaths[1].split(",");
		}

	}

	/**
	 * 
	 * @param instanceMessages
	 *            - value object which is stored in JMS topic as TextMessage
	 * @return - String array.
	 */
	public static String[] messageToArgs(
			EntityInstanceMessage[] instanceMessages) {
		String[] args = new String[ARG_LENGTH];

		args[EntityInstanceMessage.ARG.PROCESS_NAME.ORDER()] = instanceMessages[0]
				.getProcessName();
		StringBuilder feedNames = new StringBuilder();
		StringBuilder feedPaths = new StringBuilder();

		for (EntityInstanceMessage instanceMessage : instanceMessages) {
			feedNames.append(instanceMessage.getFeedName()).append(",");
			feedPaths.append(instanceMessage.getFeedInstancePath()).append(",");
		}
		if (instanceMessages[0].getEntityType().equalsIgnoreCase("PROCESS")) {
			args[EntityInstanceMessage.ARG.FEED_NAME.ORDER()] = feedNames
					.toString();

		} else {
			args[EntityInstanceMessage.ARG.FEED_NAME.ORDER()] = instanceMessages[0]
					.getFeedName();
		}
		args[EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.ORDER()] = feedPaths
				.toString();
		args[EntityInstanceMessage.ARG.WORKFLOW_ID.ORDER()] = instanceMessages[0]
				.getWorkflowId();
		args[EntityInstanceMessage.ARG.RUN_ID.ORDER()] = instanceMessages[0]
				.getRunId();
		args[EntityInstanceMessage.ARG.NOMINAL_TIME.ORDER()] = instanceMessages[0]
				.getNominalTime();
		args[EntityInstanceMessage.ARG.TIME_STAMP.ORDER()] = instanceMessages[0]
				.getTimeStamp();
		args[EntityInstanceMessage.ARG.BROKER_URL.ORDER()] = instanceMessages[0]
				.getBrokerUrl();
		args[EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.ORDER()] = instanceMessages[0]
				.getBrokerImplClass();
		args[EntityInstanceMessage.ARG.ENTITY_TYPE.ORDER()] = instanceMessages[0]
				.getEntityType();
		args[EntityInstanceMessage.ARG.OPERATION.ORDER()] = instanceMessages[0]
				.getOperation();
		args[EntityInstanceMessage.ARG.LOG_FILE.ORDER()] = instanceMessages[0]
				.getLogFile();
		args[EntityInstanceMessage.ARG.TOPIC_NAME.ORDER()] = instanceMessages[0]
				.getTopicName();
		args[EntityInstanceMessage.ARG.STATUS.ORDER()] = instanceMessages[0]
				.getStatus();
		args[EntityInstanceMessage.ARG.BROKER_TTL.ORDER()] = instanceMessages[0]
				.getBrokerTTL();

		return args;
	}

}

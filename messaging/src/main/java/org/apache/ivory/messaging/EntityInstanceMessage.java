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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Value Object which is stored in JMS Topic as MapMessage
 * 
 */
public class EntityInstanceMessage {

	private final Map<ARG, String> keyValueMap = new LinkedHashMap<ARG, String>();
	private static final Logger LOG = Logger
			.getLogger(EntityInstanceMessage.class);
	private static final String IVORY_PROCESS_TOPIC_NAME = "IVORY.ENTITY.TOPIC";

	public enum EntityOps {
		GENERATE, DELETE, ARCHIVE, REPLICATE, CHMOD
	}

	public enum ARG {
		entityName("entityName"), feedNames("feedNames"), feedInstancePaths(
				"feedInstancePaths"), workflowId("workflowId"), runId("runId"), nominalTime(
				"nominalTime"), timeStamp("timeStamp"), brokerUrl("broker.url"), brokerImplClass(
				"broker.impl.class"), entityType("entityType"), operation(
				"operation"), logFile("logFile"), topicName("topicName"), status(
				"status"), brokerTTL("broker.ttlInMins"),cluster("cluster");

		private String propName;

		private ARG(String propName) {
			this.propName = propName;
		}

		/**
		 * 
		 * @return Name of the Argument used in the parent workflow to pass
		 *         arguments to MessageProducer Main class.
		 */
		public String getArgName() {
			return this.name();
		}

		/**
		 * 
		 * @return Name of the property used in the startup.properties,
		 *         coordinator and parent workflow.
		 */
		public String getPropName() {
			return this.propName;
		}
	}

	public Map<ARG, String> getKeyValueMap() {
		return this.keyValueMap;
	}

	public String getTopicName() {
		return this.keyValueMap.get(ARG.topicName);
	}

	public String getFeedName() {
		return this.keyValueMap.get(ARG.feedNames);
	}

	public void setFeedName(String feedName) {
		this.keyValueMap.remove(ARG.feedNames);
		this.keyValueMap.put(ARG.feedNames, feedName);
	}

	public String getFeedInstancePath() {
		return this.keyValueMap.get(ARG.feedInstancePaths);
	}

	public void setFeedInstancePath(String feedInstancePath) {
		this.keyValueMap.remove(ARG.feedInstancePaths);
		this.keyValueMap.put(ARG.feedInstancePaths, feedInstancePath);
	}

	public String getEntityType() {
		return this.keyValueMap.get(ARG.entityType);
	}

	public String getBrokerTTL() {
		return this.keyValueMap.get(ARG.brokerTTL);
	}

	public void convertDateFormat() throws ParseException {
		String date = this.keyValueMap.remove(ARG.nominalTime);
		this.keyValueMap.put(ARG.nominalTime, getIvoryDate(date));
		date = this.keyValueMap.remove(ARG.timeStamp);
		this.keyValueMap.put(ARG.timeStamp, getIvoryDate(date));
	}

	public static EntityInstanceMessage[] getMessages(CommandLine cmd)
			throws ParseException {

		String[] feedNames = getFeedNames(cmd);
		String[] feedPaths;
		try {
			feedPaths = getFeedPaths(cmd);
		} catch (IOException e) {
			LOG.error("Error getting instance paths: ", e);
			throw new RuntimeException(e);
		}

		EntityInstanceMessage[] messages = new EntityInstanceMessage[feedPaths.length];
		for (int i = 0; i < feedPaths.length; i++) {
			EntityInstanceMessage message = new EntityInstanceMessage();
			setDefaultValues(cmd, message);
			// override default values
			if (message.getEntityType().equalsIgnoreCase("PROCESS")) {
				message.setFeedName(feedNames[i]);
			} else {
				message.setFeedName(message.getFeedName());
			}
			message.setFeedInstancePath(feedPaths[i]);
			message.convertDateFormat();
			messages[i] = message;
		}

		return messages;
	}

	private static void setDefaultValues(CommandLine cmd,
			EntityInstanceMessage message) {
		for (ARG arg : ARG.values()) {
			message.keyValueMap.put(arg, cmd.getOptionValue(arg.name()));
		}
	}

	private static String[] getFeedNames(CommandLine cmd) {
		String topicName = cmd.getOptionValue(ARG.topicName.getArgName());
		if (topicName.equals(IVORY_PROCESS_TOPIC_NAME)) {
			return new String[] { cmd
					.getOptionValue(ARG.feedNames.getArgName()) };
		}
		return cmd.getOptionValue(ARG.feedNames.getArgName()).split(",");
	}

	private static String[] getFeedPaths(CommandLine cmd) throws IOException {
		String topicName = cmd.getOptionValue(ARG.topicName.getArgName());
		String operation = cmd.getOptionValue(ARG.operation.getArgName());

		if (topicName.equals(IVORY_PROCESS_TOPIC_NAME)) {
			LOG.debug("Returning instance paths for Ivory Topic: "
					+ cmd.getOptionValue(ARG.feedInstancePaths.getArgName()));
			return new String[] { cmd.getOptionValue(ARG.feedInstancePaths
					.getArgName()) };
		}

		if (operation.equals(EntityOps.GENERATE.name())
				|| operation.equals(EntityOps.REPLICATE.name())) {
			LOG.debug("Returning instance paths: "
					+ cmd.getOptionValue(ARG.feedInstancePaths.getArgName()));
			return cmd.getOptionValue(ARG.feedInstancePaths.getArgName())
					.split(",");
		}
		//else case of feed retention
		Path logFile = new Path(cmd.getOptionValue(ARG.logFile.getArgName()));
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

	public String getIvoryDate(String nominalTime) throws ParseException {
		DateFormat nominalFormat = new SimpleDateFormat(
				"yyyy'-'MM'-'dd'-'HH'-'mm");
		Date nominalDate = nominalFormat.parse(nominalTime);
		DateFormat ivoryFormat = new SimpleDateFormat(
				"yyyy'-'MM'-'dd'T'HH':'mm'Z'");
		return ivoryFormat.format(nominalDate);

	}

}

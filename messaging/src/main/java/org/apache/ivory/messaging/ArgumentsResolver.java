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

public class ArgumentsResolver {

	private static final int ARG_LENGTH = ProcessMessage.ARG.values().length;

	private ArgumentsResolver() {
		// Treated like util class
	}

	/**
	 * 
	 * @param args
	 *            - String array passed from oozie action for jms messaging
	 * @return ProcessMessage - Value object which is stored in JMS topic
	 */
	public static ProcessMessage[] resolveToMessage(String[] args) {

		assert args.length == ARG_LENGTH : "Required number of arguments: "
				+ ARG_LENGTH;
		
		String[] feedNames = args[ProcessMessage.ARG.FEED_NAME.ORDER()].split(",");
		String[] feedPaths = args[ProcessMessage.ARG.FEED_INSTANCE_PATH.ORDER()].split(",");

		assert feedNames.length == feedPaths.length : "Feed names: "
				+ feedNames + " and feed paths: " + feedPaths.length
				+ "passed to parent workflow are different";

		ProcessMessage[] processMessages = new ProcessMessage[feedNames.length];
		for (int i = 0; i < feedNames.length; i++) {
			ProcessMessage processMessage = new ProcessMessage();
			processMessage.setProcessTopicName(args[ProcessMessage.ARG.PROCESS_TOPIC_NAME.ORDER()]);
			processMessage.setFeedName(feedNames[i]);
			processMessage.setFeedInstancePath(feedPaths[i]);
			processMessage.setWorkflowId(args[ProcessMessage.ARG.WORKFLOW_ID.ORDER()]);
			processMessage.setRunId(args[ProcessMessage.ARG.RUN_ID.ORDER()]);
			processMessage.setNominalTime(args[ProcessMessage.ARG.NOMINAL_TIME.ORDER()]);
			processMessage.setTimeStamp(args[ProcessMessage.ARG.TIME_STAMP.ORDER()]);
			processMessage.setBrokerUrl(args[ProcessMessage.ARG.BROKER_URL.ORDER()]);
			processMessages[i] = processMessage;
		}
		return processMessages;
	}

	/**
	 * 
	 * @param processMessages
	 *            - value object which is stored in JMS topic as TextMessage
	 * @return - String array.
	 */
	public static String[] resolveToStringArray(ProcessMessage[] processMessages) {
		String[] args = new String[ARG_LENGTH];
		args[0] = processMessages[0].getProcessTopicName();
		StringBuilder feedNames=new StringBuilder();
		StringBuilder feedPaths = new StringBuilder();
		for(ProcessMessage processMessage: processMessages){
			feedNames.append(processMessage.getFeedName()).append(",");
			feedPaths.append(processMessage.getFeedInstancePath()).append(",");
		}
		args[ProcessMessage.ARG.FEED_NAME.ORDER()] = feedNames.toString();
		args[ProcessMessage.ARG.FEED_INSTANCE_PATH.ORDER()] = feedPaths.toString();
		args[ProcessMessage.ARG.WORKFLOW_ID.ORDER()] = processMessages[0].getWorkflowId();
		args[ProcessMessage.ARG.RUN_ID.ORDER()] = processMessages[0].getRunId();
		args[ProcessMessage.ARG.NOMINAL_TIME.ORDER()]= processMessages[0].getNominalTime();
		args[ProcessMessage.ARG.TIME_STAMP.ORDER()] = processMessages[0].getTimeStamp();
		args[ProcessMessage.ARG.BROKER_URL.ORDER()]=processMessages[0].getBrokerUrl();
		return args;
	}

}

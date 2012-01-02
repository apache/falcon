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

	private static final int ARG_LENGTH = 8;
	
	private ArgumentsResolver() {
		// Treated like util class
	}

	/**
	 * 
	 * @param args
	 *            - String array passed from oozie action for jms messaging
	 * @return ProcessMessage - Value object which is stored in JMS topic
	 */
	public static ProcessMessage resolveToMessage(String[] args) {

		ProcessMessage msgArguments = new ProcessMessage();
		if (args.length != ARG_LENGTH) {
			throw new IllegalArgumentException("Required number of arguments: "
					+ ARG_LENGTH);
		}

		msgArguments.setFeedTopicName(args[0]);
		msgArguments.setFeedInstance(args[1]);
		msgArguments.setProcessName(args[2]);
		msgArguments.setCoordinatorName(args[3]);
		msgArguments.setWorkflowId(args[4]);
		msgArguments.setExternalWorkflowId(args[5]);
		msgArguments.setTimeStamp(args[6]);
		msgArguments.setRunId(args[7]);

		return msgArguments;
	}

	/**
	 * 
	 * @param messageArguments
	 *            - value object which is stored in JMS topic as TextMessage
	 * @return - String array.
	 */
	public static String[] resolveToStringArray(ProcessMessage messageArguments) {
		String[] args = new String[ARG_LENGTH];
		args[0] = messageArguments.getFeedTopicName();
		args[1] = messageArguments.getFeedInstance();
		args[2] = messageArguments.getProcessName();
		args[3] = messageArguments.getCoordinatorName();
		args[4] = messageArguments.getWorkflowId();
		args[5] = messageArguments.getExternalWorkflowId();
		args[6] = messageArguments.getTimeStamp();
		args[7] = messageArguments.getRunId();

		return args;

	}


}

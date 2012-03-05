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

import java.util.HashMap;
import java.util.Map;

/**
 * Value Object which is stored in JMS Topic as TextMessage
 * 
 */
public class ProcessMessage {

	private final Map<ARG, String> msgs = new HashMap<ARG, String>();
	private static final String MSG_SEPERATOR=",";

	/**
	Enum for arguments that are used in coordinators
	to pass arguments to parent workflow
	 */
	public enum ARG {
		PROCESS_TOPIC_NAME(0,"processTopicName"),
		FEED_NAME(1,"feedNames"),
		FEED_INSTANCE_PATH(2,"feedInstancePaths"), 
		WORKFLOW_ID(3,"workflowId"), 
		RUN_ID(4,"runId"), 
		NOMINAL_TIME(5,"nominalTime"), 
		TIME_STAMP(6,"timeStamp"),
		BROKER_URL(7,"brokerUrl");

		private int argOrder;
		private String argName;

		private ARG(int argOrder, String argName){
			this.argOrder=argOrder;
			this.argName=argName;
		}

		public int ORDER(){
			return this.argOrder;
		}

		public String NAME(){
			return this.argName;
		}
	}

	public String getProcessTopicName() {
		return this.msgs.get(ARG.PROCESS_TOPIC_NAME);
	}

	public void setProcessTopicName(String processTopicName) {
		this.msgs.put(ARG.PROCESS_TOPIC_NAME, processTopicName);
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

	@Override
	public String toString() {
		return getProcessTopicName() + MSG_SEPERATOR + getFeedName() + MSG_SEPERATOR
				+ getFeedInstancePath() + MSG_SEPERATOR + getWorkflowId() + MSG_SEPERATOR
				+ getRunId() + MSG_SEPERATOR + getNominalTime() + MSG_SEPERATOR + getTimeStamp();
	}

}

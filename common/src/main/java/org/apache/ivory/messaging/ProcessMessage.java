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

/**
 * Value Object which is stored in JMS Topic as TextMessage
 * 
 */
public class ProcessMessage {

	private String feedTopicName;
	private String processName;
	private String coordinatorName;
	private String feedInstance;
	private String workflowId;
	private String externalWorkflowId;
	private String timeStamp;
	private String runId;

	public String getFeedTopicName() {
		return this.feedTopicName;
	}

	public void setFeedTopicName(String feedTopicName) {
		this.feedTopicName = feedTopicName;
	}

	public String getProcessName() {
		return this.processName;
	}

	public void setProcessName(String processName) {
		this.processName = processName;
	}

	public String getCoordinatorName() {
		return this.coordinatorName;
	}

	public void setCoordinatorName(String coordinatorName) {
		this.coordinatorName = coordinatorName;
	}

	public String getFeedInstance() {
		return this.feedInstance;
	}

	public void setFeedInstance(String feedInstance) {
		this.feedInstance = feedInstance;
	}

	public String getWorkflowId() {
		return this.workflowId;
	}

	public void setWorkflowId(String workflowId) {
		this.workflowId = workflowId;
	}

	public String getExternalWorkflowId() {
		return this.externalWorkflowId;
	}

	public void setExternalWorkflowId(String externalWorkflowId) {
		this.externalWorkflowId = externalWorkflowId;
	}

	public String getTimeStamp() {
		return this.timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getRunId() {
		return this.runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
	}

	@Override
	public String toString() {
		return this.feedTopicName + ":" + this.feedInstance + ":"
				+ this.processName + ":" + this.coordinatorName + ":"
				+ this.workflowId + ":" + this.externalWorkflowId + ":"
				+ this.timeStamp + ":" + this.runId;
	}

}

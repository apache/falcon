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
package org.apache.ivory.retry;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.ivory.workflow.engine.WorkflowEngine;

public class RetryEvent implements Delayed {

	public static final String SEP = "*";

	private WorkflowEngine wfEngine;
	private String clusterName;
	private String wfId;
	private long queueInsertTime;
	private long endOfDelay;
	private String processName;
	private String processInstance;
	private int runId;
	private int attempts;
	private int failRetryCount;

	public RetryEvent(WorkflowEngine wfEngine, String clusterName, String wfId,
			long queueInsertTime, long endOfDelay, String processName,
			String processInstance, int runId, int attempts, int failRetryCount) {
		this.wfEngine = wfEngine;
		this.clusterName = clusterName;
		this.wfId = wfId;
		this.queueInsertTime = queueInsertTime;
		this.endOfDelay = endOfDelay;
		this.processName = processName;
		this.processInstance = processInstance;
		this.runId = runId;
		this.attempts = attempts;
		this.failRetryCount = failRetryCount;
	}

	public WorkflowEngine getWfEngine() {
		return wfEngine;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getWfId() {
		return wfId;
	}

	public long getEndOfDelay() {
		return endOfDelay;
	}

	public String getProcessName() {
		return processName;
	}

	public String getProcessInstance() {
		return processInstance;
	}

	public int getRunId() {
		return runId;
	}

	public int getAttempts() {
		return attempts;
	}

	public int getFailRetryCount() {
		return failRetryCount;
	}

	public void setFailRetryCount(int failRetryCount) {
		this.failRetryCount = failRetryCount;
	}

	@Override
	public int compareTo(Delayed o) {
		int ret = 0;
		RetryEvent event = (RetryEvent) o;

		if (this.endOfDelay < event.endOfDelay)
			ret = -1;
		else if (this.endOfDelay > event.endOfDelay)
			ret = 1;
		else if (this.queueInsertTime == event.queueInsertTime)
			ret = 0;

		return ret;

	}

	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert((queueInsertTime - System.currentTimeMillis())
				+ endOfDelay, TimeUnit.MILLISECONDS);
	}

	@Override
	public String toString() {
		return clusterName + SEP + wfId + SEP + queueInsertTime + SEP
				+ endOfDelay + SEP + processName + SEP + processInstance + SEP
				+ runId + SEP + attempts + SEP + failRetryCount;
	}

	public static RetryEvent fromString(WorkflowEngine workflowEngine, String message) {
		String[] items = message.split("\\"+SEP);
		RetryEvent event = new RetryEvent(workflowEngine, items[0], items[1],
				Long.parseLong(items[2]), Long.parseLong(items[3]), items[4],
				items[5], Integer.parseInt(items[6]),
				Integer.parseInt(items[7]), Integer.parseInt(items[8]));
		return event;
	}
}

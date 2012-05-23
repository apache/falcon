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
package org.apache.ivory.rerun.event;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.ivory.workflow.engine.WorkflowEngine;

public class RerunEvent implements Delayed {

	protected static final String SEP = "*";
	
	public  enum RerunType{
		RETRY, LATE
	}

	protected WorkflowEngine wfEngine;
	protected String clusterName;
	protected String wfId;
	protected long msgInsertTime;
	protected long delayInMilliSec;
	protected String processName;
	protected String processInstance;
	protected int runId;

	public RerunEvent(WorkflowEngine wfEngine, String clusterName, String wfId,
			long msgInsertTime, long delay, String processName,
			String processInstance, int runId) {
		this.wfEngine = wfEngine;
		this.clusterName = clusterName;
		this.wfId = wfId;
		this.msgInsertTime = msgInsertTime;
		this.delayInMilliSec = delay;
		this.processName = processName;
		this.processInstance = processInstance;
		this.runId = runId;
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

	public long getDelayInMilliSec() {
		return delayInMilliSec;
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

	@Override
	public int compareTo(Delayed o) {
		int ret = 0;
		RerunEvent event = (RerunEvent) o;

		if (this.delayInMilliSec < event.delayInMilliSec)
			ret = -1;
		else if (this.delayInMilliSec > event.delayInMilliSec)
			ret = 1;
		else if (this.msgInsertTime == event.msgInsertTime)
			ret = 0;

		return ret;

	}

	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert((msgInsertTime - System.currentTimeMillis())
				+ delayInMilliSec, TimeUnit.MILLISECONDS);
	}

	public long getMsgInsertTime() {
		return msgInsertTime;
	}

	public void setMsgInsertTime(long msgInsertTime) {
		this.msgInsertTime = msgInsertTime;
	}

	public RerunEvent fromString(WorkflowEngine workflowEngine, String line) {
		// TODO Auto-generated method stub
		return null;
	}

	public RerunType getType() {
		if (this instanceof RetryEvent) {
			return RerunType.RETRY;
		} else if (this instanceof LaterunEvent) {
			return RerunType.LATE;
		} else {
			return null;
		}
	}

}

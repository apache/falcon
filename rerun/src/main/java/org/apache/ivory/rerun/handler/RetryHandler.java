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
package org.apache.ivory.rerun.handler;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.rerun.event.RetryEvent;
import org.apache.ivory.rerun.policy.AbstractRerunPolicy;
import org.apache.ivory.rerun.policy.RerunPolicyFactory;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.util.GenericAlert;
import org.apache.ivory.workflow.engine.WorkflowEngine;

public class RetryHandler<M extends DelayedQueue<RetryEvent>> extends
		AbstractRerunHandler<RetryEvent, M> {

	@Override
	public void handleRerun(String processName, String nominalTime,
			String runId, String wfId, WorkflowEngine wfEngine,
			long msgReceivedTime) throws IvoryException {
		try {
			Process processObj = getProcess(processName);
			if (!validate(processName, processObj)) {
				return;
			}

			int attempts = processObj.getRetry().getAttempts();
			int delay = processObj.getRetry().getDelay();
			String delayUnit = processObj.getRetry().getDelayUnit();
			String policy = processObj.getRetry().getPolicy();
			int intRunId = Integer.parseInt(runId);

			if (attempts > intRunId) {
				AbstractRerunPolicy rerunPolicy = RerunPolicyFactory
						.getRetryPolicy(policy);
				long delayTime = rerunPolicy.getDelay(delayUnit, delay,
						Integer.parseInt(runId));
				RetryEvent event = new RetryEvent(wfEngine, processObj
						.getCluster().getName(), wfId, msgReceivedTime,
						delayTime, processName, nominalTime, intRunId,
						attempts, 0);
				offerToQueue(event);
			} else {
				LOG.warn("All retry attempt failed out of configured: "
						+ attempts + " attempt for process instance::"
						+ processName + ":" + nominalTime + " And WorkflowId: "
						+ wfId);

				GenericAlert.alertWFfailed(processName, nominalTime);
			}
		} catch (Exception e) {
			LOG.error("Error during retry of processInstance " + processName
					+ ":" + nominalTime, e);
			GenericAlert.alertRetryFailed(processName, nominalTime,
					Integer.parseInt(runId), e.getMessage());
			throw new IvoryException(e);
		}

	}

	@Override
	public void init(M queue) throws IvoryException {
		super.init(queue);
		Thread daemon = new Thread(new RetryConsumer(this));
		daemon.setName("RetryHandler");
		daemon.setDaemon(true);
		daemon.start();
		LOG.info("RetryHandler  thread started");

	}

	protected boolean validate(String processName, Process processObj) {
		if (!super.validate(processName, processObj)) {
			return false;
		}
		if (processObj.getRetry() == null) {
			LOG.warn("Retry not configured for the process: " + processName);
			return false;
		}
		return true;
	}

}

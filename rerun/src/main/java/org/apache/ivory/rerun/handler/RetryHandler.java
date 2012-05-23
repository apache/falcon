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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.rerun.event.RetryEvent;
import org.apache.ivory.rerun.policy.AbstractRerunPolicy;
import org.apache.ivory.rerun.policy.RerunPolicyFactory;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.util.GenericAlert;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.WorkflowEngine;

public class RetryHandler<T extends RetryEvent, M extends DelayedQueue<RetryEvent>>
		extends AbstractRerunHandler<RetryEvent, M> {

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
		Thread daemon = new RetryHandler.Consumer();
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

	private final class Consumer extends Thread {
		@Override
		public void run() {
			while (true) {
				RetryEvent message = null;
				try {
					message = takeFromQueue();
				} catch (IvoryException e) {
					LOG.error("Error while reading message from the queue: ", e);
					continue;
				}
				try {
					Process processObj = getProcess(message.getProcessName());
					if (!validate(message.getProcessName(), processObj)) {
						continue;
					}
					String jobStatus = message.getWfEngine().instanceStatus(
							message.getClusterName(), message.getWfId());
					if (!jobStatus.equals("KILLED")) {
						LOG.debug("Re-enqueing message in RetryHandler for workflow with same delay as job status is running:"
								+ message.getWfId());
						message.setMsgInsertTime(System.currentTimeMillis());
						offerToQueue(message);
						continue;
					}
					LOG.info("Retrying attempt:" + (message.getRunId() + 1)
							+ " out of configured: " + message.getAttempts()
							+ " attempt for process instance::"
							+ message.getProcessName() + ":"
							+ message.getProcessInstance()
							+ " And WorkflowId: " + message.getWfId()
							+ " At time: "
							+ getTZdate(new Date(System.currentTimeMillis())));
					message.getWfEngine().reRun(message.getClusterName(),
							message.getWfId(), null);
				} catch (Throwable e) {
					int maxFailRetryCount = Integer.parseInt(StartupProperties
							.get().getProperty("max.retry.failure.count", "1"));
					if (message.getFailRetryCount() < maxFailRetryCount) {
						LOG.warn(
								"Retrying again for process instance "
										+ message.getProcessName()
										+ ":"
										+ message.getProcessInstance()
										+ " after "
										+ message.getDelayInMilliSec()
										+ " seconds as Retry failed with message:",
								e);
						message.setFailRetryCount(message.getFailRetryCount() + 1);
						offerToQueue(message);
					} else {
						LOG.warn(
								"Failure retry attempts exhausted for processInstance: "
										+ message.getProcessName() + ":"
										+ message.getProcessInstance(), e);
						GenericAlert.alertRetryFailed(message.getProcessName(),
								message.getProcessInstance(),
								message.getRunId(), e.getMessage());
					}

				}
			}
		}

		private String getTZdate(Date date) {
			DateFormat ivoryFormat = new SimpleDateFormat(
					"yyyy'-'MM'-'dd'T'HH':'mm'Z'");
			return ivoryFormat.format(date);
		}

	}
}

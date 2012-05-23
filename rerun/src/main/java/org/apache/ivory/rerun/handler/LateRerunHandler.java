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
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.rerun.event.LaterunEvent;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.WorkflowEngine;

public class LateRerunHandler<T extends LaterunEvent, M extends DelayedQueue<LaterunEvent>>
		extends AbstractRerunHandler<LaterunEvent, M> {

	@Override
	public void handleRerun(String processName, String nominalTime,
			String runId, String wfId, WorkflowEngine wfEngine,
			long msgReceivedTime) throws IvoryException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean offerToQueue(LaterunEvent event) {
		return false;
		// TODO Auto-generated method stub

	}

	@Override
	public LaterunEvent takeFromQueue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init(M delayQueue) throws IvoryException {
		super.init(delayQueue);
		Thread daemon = new LateRerunHandler.Consumer();
		daemon.setName("LaterunHandler");
		daemon.setDaemon(true);
		daemon.start();
		LOG.info("LaterunHandler  thread started");
	}

	public Process getProcess(String processName) throws IvoryException {
		return ConfigurationStore.get().get(EntityType.PROCESS, processName);
	}

	private final class Consumer extends Thread {
		@Override
		public void run() {
			while (true) {
				LaterunEvent message = null;
				message = takeFromQueue();
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
					// LOG.info("Retrying attempt:" + (message.getRunId() + 1)
					// + " out of configured: " + message.getAttempts()
					// + " attempt for process instance::"
					// + message.getProcessName() + ":"
					// + message.getProcessInstance()
					// + " And WorkflowId: " + message.getWfId()
					// + " At time: "
					// + getTZdate(new Date(System.currentTimeMillis())));
					message.getWfEngine().reRun(message.getClusterName(),
							message.getWfId(), null);
				} catch (Throwable e) {
					int maxFailRetryCount = Integer.parseInt(StartupProperties
							.get().getProperty("max.retry.failure.count", "1"));
					// if (message.getFailRetryCount() < maxFailRetryCount) {
					// LOG.warn(
					// "Retrying again for process instance "
					// + message.getProcessName()
					// + ":"
					// + message.getProcessInstance()
					// + " after "
					// + message.getDelayInMilliSec()
					// + " seconds as Retry failed with message:",
					// e);
					// message.setFailRetryCount(message.getFailRetryCount() +
					// 1);
					// offerToQueue(message);
					// } else {
					// LOG.warn(
					// "Failure retry attempts exhausted for processInstance: "
					// + message.getProcessName() + ":"
					// + message.getProcessInstance(), e);
					// GenericAlert.alertRetryFailed(message.getProcessName(),
					// message.getProcessInstance(),
					// message.getRunId(), e.getMessage());
					// }
				}
			}
		}
	}

}

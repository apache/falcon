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

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.LateInput;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.ivory.latedata.LateDataHandler;
import org.apache.ivory.rerun.event.LaterunEvent;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.util.GenericAlert;
import org.apache.ivory.workflow.engine.WorkflowEngine;
import org.apache.ivory.rerun.policy.AbstractRerunPolicy;
import org.apache.ivory.rerun.policy.RerunPolicyFactory;

public class LateRerunHandler<M extends DelayedQueue<LaterunEvent>>
		extends AbstractRerunHandler<LaterunEvent, M> {

	@Override
	public void handleRerun(String processName, String nominalTime,
			String runId, String wfId, WorkflowEngine wfEngine,
			long msgReceivedTime) throws IvoryException {

		Process processObj = getProcess(processName);
		int intRunId = Integer.parseInt(runId);
		Date msgInsertTime = EntityUtil.parseDateUTC(nominalTime);
		Long wait = getEventDelay(processObj, nominalTime);
		if (wait == -1)
			return;

		LOG.debug("Scheduling the late rerun for process instance : "
				+ processName + ":" + nominalTime + " And WorkflowId: " + wfId);
		LaterunEvent event = new LaterunEvent(wfEngine, processObj.getCluster()
				.getName(), wfId, msgInsertTime.getTime(), wait, processName,
				nominalTime, intRunId);
		offerToQueue(event);

	}

	public static long getEventDelay(Process processObj, String nominalTime)
			throws IvoryException {

		Date instanceDate = EntityUtil.parseDateUTC(nominalTime);
		if (processObj.getLateProcess() == null) {
			return -1;
		}

		String latePolicy = processObj.getLateProcess().getPolicy();
		Date cutOffTime = getCutOffTime(processObj, nominalTime);
		Date now = new Date(System.currentTimeMillis());
		Long wait = null;

		if (now.after(cutOffTime)) {
			LOG.warn("Feed Cut Off time: " + now.toString()
					+ " has expired, Late Rerun can not be scheduled");
			return -1;
		} else {
			AbstractRerunPolicy rerunPolicy = RerunPolicyFactory
					.getRetryPolicy(latePolicy);
			wait = rerunPolicy.getDelay(processObj.getLateProcess()
					.getDelayUnit(), processObj.getLateProcess().getDelay(),
					instanceDate, cutOffTime);
		}
		return wait;
	}

	public static Date addTime(Date date, int milliSecondsToAdd) {
		return new Date(date.getTime() + milliSecondsToAdd);
	}

	public static Date getCutOffTime(Process process, String nominalTime)
			throws IvoryException {
		ConfigurationStore store = ConfigurationStore.get();
		ExpressionHelper evaluator = ExpressionHelper.get();
		Date instanceStart = EntityUtil.parseDateUTC(nominalTime);
		ExpressionHelper.setReferenceDate(instanceStart);

		Date endTime = new Date();
		Date feedCutOff = new Date(0);
		for (LateInput lp : process.getLateProcess().getLateInput()) {
			Feed feed = null;
			String endInstanceTime = "";
			for (Input input : process.getInputs().getInput()) {
				if (input.getName().equals(lp.getFeed())) {
					endInstanceTime = input.getEndInstance();
					feed = store.get(EntityType.FEED, input.getFeed());
					break;
				}
			}
			String lateCutOff = feed.getLateArrival().getCutOff();
			endTime = evaluator.evaluate(endInstanceTime, Date.class);
			long feedCutOffPeriod = evaluator.evaluate(lateCutOff, Long.class);
			endTime = addTime(endTime, (int) feedCutOffPeriod);

			if (endTime.after(feedCutOff))
				feedCutOff = endTime;
		}
		return feedCutOff;
	}

	@Override
	public void init(M delayQueue) throws IvoryException {
		super.init(delayQueue);
		Thread daemon = new Thread(new LateRerunConsumer(this));
		daemon.setName("LaterunHandler");
		daemon.setDaemon(true);
		daemon.start();
		LOG.info("LaterunHandler  thread started");
	}

	public Process getProcess(String processName) throws IvoryException {
		return (Process) EntityUtil.getEntity(EntityType.PROCESS, processName);
	}
}
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

import java.util.Date;

import org.apache.ivory.IvoryException;
import org.apache.ivory.aspect.GenericAlert;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.LateInput;
import org.apache.ivory.entity.v0.process.LateProcess;
import org.apache.ivory.entity.v0.process.PolicyType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.ivory.rerun.event.LaterunEvent;
import org.apache.ivory.rerun.policy.AbstractRerunPolicy;
import org.apache.ivory.rerun.policy.RerunPolicyFactory;
import org.apache.ivory.rerun.queue.DelayedQueue;

public class LateRerunHandler<M extends DelayedQueue<LaterunEvent>> extends
		AbstractRerunHandler<LaterunEvent, M> {

	@Override
	public void handleRerun(String cluster, String entityType,
			String entityName, String nominalTime, String runId, String wfId,
			long msgReceivedTime) {

		try {
			Entity entity = EntityUtil.getEntity(entityType, entityName);
			int intRunId = Integer.parseInt(runId);
			Date msgInsertTime = EntityUtil.parseDateUTC(nominalTime);
			Long wait = getEventDelay(entity, nominalTime);
			if (wait == -1)
				return;

			LOG.debug("Scheduling the late rerun for entity instance : "
					+ entityType + "(" + entityName + ")" + ":" + nominalTime
					+ " And WorkflowId: " + wfId);
			LaterunEvent event = new LaterunEvent(cluster, wfId,
					msgInsertTime.getTime(), wait, entityType, entityName,
					nominalTime, intRunId);
			offerToQueue(event);
		} catch (Exception e) {
			LOG.error("Unable to schedule late rerun for entity instance : "
					+ entityType + "(" + entityName + ")" + ":" + nominalTime
					+ " And WorkflowId: " + wfId, e);
			GenericAlert.alertLateRerunFailed(entityType, entityName,
					nominalTime, wfId, runId, e.getMessage());
		}
	}

	private long getEventDelay(Entity entity, String nominalTime)
			throws IvoryException {

		Date instanceDate = EntityUtil.parseDateUTC(nominalTime);
		LateProcess lateProcess = EntityUtil.getLateProcess(entity);
		if (lateProcess == null) {
			LOG.warn("Late run not applicable for entity:"
					+ entity.getEntityType() + "(" + entity.getName() + ")");
			return -1;
		}
		PolicyType latePolicy = lateProcess.getPolicy();
		Date cutOffTime = getCutOffTime(entity, nominalTime);
		Date now = new Date();
		Long wait = null;

		if (now.after(cutOffTime)) {
			LOG.warn("Feed Cut Off time: "
					+ SchemaHelper.formatDateUTC(cutOffTime)
					+ " has expired, Late Rerun can not be scheduled");
			return -1;
		} else {
			AbstractRerunPolicy rerunPolicy = RerunPolicyFactory
					.getRetryPolicy(latePolicy);
			wait = rerunPolicy.getDelay(lateProcess.getDelay(), instanceDate,
					cutOffTime);
		}
		return wait;
	}

	public static Date addTime(Date date, int milliSecondsToAdd) {
		return new Date(date.getTime() + milliSecondsToAdd);
	}

	public static Date getCutOffTime(Entity entity, String nominalTime)
			throws IvoryException {

		ConfigurationStore store = ConfigurationStore.get();
		ExpressionHelper evaluator = ExpressionHelper.get();
		Date instanceStart = EntityUtil.parseDateUTC(nominalTime);
		ExpressionHelper.setReferenceDate(instanceStart);
		Date endTime = new Date();
		Date feedCutOff = new Date(0);
		if (entity.getEntityType() == EntityType.FEED) {
			String lateCutOff = ((Feed) entity).getLateArrival().getCutOff()
					.toString();
			endTime = EntityUtil.parseDateUTC(nominalTime);
			long feedCutOffPeriod = evaluator.evaluate(lateCutOff, Long.class);
			endTime = addTime(endTime, (int) feedCutOffPeriod);
			return endTime;
		} else if (entity.getEntityType() == EntityType.PROCESS) {
			Process process = (Process) entity;
			for (LateInput lp : process.getLateProcess().getLateInputs()) {
				Feed feed = null;
				String endInstanceTime = "";
				for (Input input : process.getInputs().getInputs()) {
					if (input.getName().equals(lp.getInput())) {
						endInstanceTime = input.getEnd();
						feed = store.get(EntityType.FEED, input.getFeed());
						break;
					}
				}
				String lateCutOff = feed.getLateArrival().getCutOff()
						.toString();
				endTime = evaluator.evaluate(endInstanceTime, Date.class);
				long feedCutOffPeriod = evaluator.evaluate(lateCutOff,
						Long.class);
				endTime = addTime(endTime, (int) feedCutOffPeriod);

				if (endTime.after(feedCutOff))
					feedCutOff = endTime;
			}
			return feedCutOff;
		} else {
			throw new IvoryException(
					"Invalid entity while getting cut-off time:"
							+ entity.getName());
		}
	}

	@Override
	public void init(M delayQueue) throws IvoryException {
		super.init(delayQueue);
		Thread daemon = new Thread(new LateRerunConsumer(this));
		daemon.setName("LaterunHandler");
		daemon.setDaemon(true);
		daemon.start();
		LOG.info("Laterun Handler  thread started");
	}

}

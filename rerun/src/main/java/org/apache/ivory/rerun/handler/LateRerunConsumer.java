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

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.aspect.GenericAlert;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.entity.v0.process.LateInput;
import org.apache.ivory.latedata.LateDataHandler;
import org.apache.ivory.rerun.event.LaterunEvent;
import org.apache.ivory.rerun.queue.DelayedQueue;
import org.apache.ivory.workflow.engine.WorkflowEngine;

public class LateRerunConsumer<T extends LateRerunHandler<DelayedQueue<LaterunEvent>>>
		extends AbstractRerunConsumer<LaterunEvent, T> {

	public LateRerunConsumer(T handler) {
		super(handler);
	}

	@Override
	protected void handleRerun(String cluster, String jobStatus,
			LaterunEvent message) {
		try {
			if (jobStatus.equals("RUNNING") || jobStatus.equals("PREP")
					|| jobStatus.equals("SUSPENDED")) {
				LOG.debug("Re-enqueing message in LateRerunHandler for workflow with same delay as job status is running:"
						+ message.getWfId());
				message.setMsgInsertTime(System.currentTimeMillis());
				handler.offerToQueue(message);
				return;
			}

			String detectLate = detectLate(message);

			if (detectLate.equals("")) {
				LOG.debug("No Late Data Detected, scheduling next late rerun for wf-id: "
						+ message.getWfId()
						+ " at "
						+ SchemaHelper.formatDateUTC(new Date()));
				handler.handleRerun(cluster, message.getEntityType(),
						message.getEntityName(), message.getInstance(),
						Integer.toString(message.getRunId()),
						message.getWfId(), System.currentTimeMillis());
				return;
			}

			LOG.info("Late changes detected in the following feeds: "
					+ detectLate);

			handler.getWfEngine().reRun(message.getClusterName(),
					message.getWfId(), null);
			LOG.info("Scheduled late rerun for wf-id: " + message.getWfId()
					+ " on cluster: " + message.getClusterName());
		} catch (Exception e) {
			LOG.warn(
					"Late Re-run failed for instance "
							+ message.getEntityName() + ":"
							+ message.getInstance() + " after "
							+ message.getDelayInMilliSec() + " with message:",
					e);
			GenericAlert.alertLateRerunFailed(message.getEntityType(),
					message.getEntityName(), message.getInstance(),
					message.getWfId(), Integer.toString(message.getRunId()),
					e.getMessage());
		}

	}

	public String detectLate(LaterunEvent message) throws Exception {
		LateDataHandler late = new LateDataHandler();
		String ivoryInputFeeds = handler.getWfEngine().getWorkflowProperty(
				message.getClusterName(), message.getWfId(), "ivoryInputFeeds");
		String logDir = handler.getWfEngine().getWorkflowProperty(
				message.getClusterName(), message.getWfId(), "logDir");
		String ivoryInPaths = handler.getWfEngine().getWorkflowProperty(
				message.getClusterName(), message.getWfId(), "ivoryInPaths");
		String nominalTime = handler.getWfEngine().getWorkflowProperty(
				message.getClusterName(), message.getWfId(), "nominalTime");
		String srcClusterName = handler.getWfEngine().getWorkflowProperty(
				message.getClusterName(), message.getWfId(), "srcClusterName");
		
		Configuration conf = new Configuration();
		conf.set(
				CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
				handler.getWfEngine().getWorkflowProperty(
						message.getClusterName(), message.getWfId(),
						WorkflowEngine.NAME_NODE));
		Path lateLogPath = getLateLogPath(logDir, nominalTime, srcClusterName);
		FileSystem fs = FileSystem.get(conf);
		if(!fs.exists(lateLogPath)){
			LOG.warn("Late log file:"+lateLogPath+" not found:");
			return "";
		}
		Map<String, Long> feedSizes = new LinkedHashMap<String, Long>();
		String[] pathGroups = ivoryInPaths.split("#");
		String[] inputFeeds = ivoryInputFeeds.split("#");
		Entity entity = EntityUtil.getEntity(message.getEntityType(),
				message.getEntityName());
		
		List<String> lateFeed = new ArrayList<String>();
		if (EntityUtil.getLateProcess(entity) != null) {
			for (LateInput li : EntityUtil.getLateProcess(entity)
					.getLateInputs()) {
				lateFeed.add(li.getInput());
			}
			for (int index = 0; index < pathGroups.length; index++) {
				if (lateFeed.contains(inputFeeds[index])) {
					long usage = 0;
					for (String pathElement : pathGroups[index].split(",")) {
						Path inPath = new Path(pathElement);
						usage += late.usage(inPath, conf);
					}
					feedSizes.put(inputFeeds[index], usage);
				}
			}
		} else {
			LOG.warn("Late process is not configured for entity: "
					+ message.getEntityType() + "(" + message.getEntityName()
					+ ")");
		}

		String detectLate = late.detectChanges(lateLogPath, feedSizes, conf);
		return detectLate;
	}

	private Path getLateLogPath(String logDir, String nominalTime,
			String srcClusterName) {
		//SrcClusterName valid only in case of feed
		return new Path(logDir + "/latedata/" + nominalTime + "/"
				+ (srcClusterName == null
				? "" : srcClusterName));

	}
}

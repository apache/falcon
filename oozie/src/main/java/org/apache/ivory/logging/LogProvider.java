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
package org.apache.ivory.logging;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.resource.InstancesResult.InstanceAction;
import org.apache.ivory.resource.InstancesResult.Instance;
import org.apache.ivory.resource.InstancesResult.WorkflowStatus;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;

public final class LogProvider {
	private static final Logger LOG = Logger.getLogger(LogProvider.class);

	public static Instance getLogUrl(Entity entity, Instance Instance,
			Tag type, String runId) throws IvoryException {
        Process process = (Process) entity;
        //TODO Fix it for multiple clusters just like for feed
		Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER,
				process.getClusters().getClusters().get(0).getName());
		ExternalId externalId = getExternalId(process.getName(), type,
				Instance.instance);
		try {
			// if parent wf is running/suspend return oozie's console url of
			// parent wf
			if (Instance.status.equals(WorkflowStatus.RUNNING)
					|| Instance.status.equals(WorkflowStatus.SUSPENDED)) {
				String parentWFUrl = getConsoleURL(
						ClusterHelper.getOozieUrl(cluster), externalId.getId());
				return new InstancesResult.Instance(
						Instance, parentWFUrl, null);
			}

			OozieClient client = new OozieClient(
					ClusterHelper.getOozieUrl(cluster));
			String parentJobId = client.getJobId(externalId.getId());
			WorkflowJob jobInfo = client.getJobInfo(parentJobId);
			List<WorkflowAction> actions = jobInfo.getActions();
			// if parent wf killed manually or ivory actions fail, return
			// oozie's console url of parent wf
			if (actions.size() < 4
					|| !actions.get(0).getStatus()
							.equals(WorkflowAction.Status.OK)
					|| !actions.get(2).getStatus()
							.equals(WorkflowAction.Status.OK)
					|| !actions.get(3).getStatus()
							.equals(WorkflowAction.Status.OK)
					|| (actions.size() == 5 && !actions.get(4).getStatus()
							.equals(WorkflowAction.Status.OK))) {
				return new InstancesResult.Instance(
						Instance, jobInfo.getConsoleUrl(), null);
			}

			return getActionsUrl(cluster, process, Instance, externalId,
					runId);
		} catch (Exception e) {
			LOG.error("Exception in LogProvider while getting job id", e);
			return new InstancesResult.Instance(Instance,
					"-", null);
		}

	}

	private static Instance getActionsUrl(Cluster cluster,
			Process process, Instance Instance,
			ExternalId externalId, String runId) throws IvoryException,
			OozieClientException {
		OozieClient client = new OozieClient(ClusterHelper.getOozieUrl(cluster));
		String parentJobId = client.getJobId(externalId.getId());
		WorkflowJob jobInfo = client.getJobInfo(parentJobId + "@user-workflow");
		String subflowId = jobInfo.getExternalId();
		WorkflowJob subflowJob = client.getJobInfo(subflowId);

		InstanceAction[] instanceActions = new InstanceAction[subflowJob
				.getActions().size()];
		for (int i = 0; i < subflowJob.getActions().size(); i++) {
			WorkflowAction action = subflowJob.getActions().get(i);
			InstanceAction instanceAction = new InstanceAction();
			instanceAction.action = action.getName();
			instanceAction.status = action.getStatus().name();
			if (action.getType().equals("pig")
					|| action.getType().equals("java")) {
				instanceAction.logFile = getDFSbrowserUrl(cluster, process,
						externalId, runId, action.getName());
			} else {
				instanceAction.logFile = action.getConsoleUrl();
			}
			instanceActions[i] = instanceAction;
		}
		String oozieLogFile = getDFSbrowserUrl(cluster, process, externalId,
				runId, "oozie");
		return new Instance(Instance, oozieLogFile,
				instanceActions);

	}

	private static String getConsoleURL(String oozieUrl, String externalId)
			throws OozieClientException {
		OozieClient client = new OozieClient(oozieUrl);
		String jobId = client.getJobId(externalId);
		WorkflowJob jobInfo = client.getJobInfo(jobId);
		return jobInfo.getConsoleUrl();
	}

	private static ExternalId getExternalId(String processName, Tag tag,
			String date) throws ValidationException {
        return new ExternalId(processName, tag, date);
	}

	private static String getDFSbrowserUrl(Cluster cluster, Process process,
			ExternalId externalId, String runId, String action)
			throws IvoryException {
		return (ClusterHelper.getReadOnlyHdfsUrl(cluster) + "/data/"
				+ process.getWorkflow().getPath() + "/log/"
				+ externalId.getDFSname() + "/" + runId + "/" + action + ".log")
				.replaceAll("//", "/");
	}

}

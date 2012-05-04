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

import java.util.List;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.resource.ProcessInstancesResult;
import org.apache.ivory.resource.ProcessInstancesResult.InstanceAction;
import org.apache.ivory.resource.ProcessInstancesResult.ProcessInstance;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

public final class LogProvider {
	private static final Logger LOG = Logger.getLogger(LogProvider.class);

	public static ProcessInstancesResult.ProcessInstance getLogUrl(
			Process process,
			ProcessInstancesResult.ProcessInstance processInstance,
			String type, String runId) throws IvoryException {
		Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER,
				process.getCluster().getName());
		ExternalId externalId = getExternalId(process.getName(), type,
				processInstance.instance);
		try {
			// if parent wf is running/suspend return oozie's console url of
			// parent wf
			if (processInstance.status.equals(WorkflowStatus.RUNNING)
					|| processInstance.status.equals(WorkflowStatus.SUSPENDED)) {
				String parentWFUrl = getConsoleURL(
						ClusterHelper.getOozieUrl(cluster), externalId.getId());
				return new ProcessInstancesResult.ProcessInstance(
						processInstance, parentWFUrl, null);
			}

			OozieClient client = new OozieClient(
					ClusterHelper.getOozieUrl(cluster));
			String parentJobId = client.getJobId(externalId.getId());
			WorkflowJob jobInfo = client.getJobInfo(parentJobId);
			List<WorkflowAction> actions = jobInfo.getActions();
			// if parent wf killed manually or ivory actions fail, return
			// oozie's console url of parent wf
			if (actions.size() != 4
					|| !actions.get(0).getStatus()
							.equals(WorkflowAction.Status.OK)
					|| !actions.get(2).getStatus()
							.equals(WorkflowAction.Status.OK)
					|| !actions.get(3).getStatus()
							.equals(WorkflowAction.Status.OK)) {
				return new ProcessInstancesResult.ProcessInstance(
						processInstance, jobInfo.getConsoleUrl(), null);
			}

			return getActionsUrl(cluster, process, processInstance, externalId,
					runId);
		} catch (OozieClientException e) {
			LOG.error(e);
			return new ProcessInstancesResult.ProcessInstance(processInstance,
					"-", new ProcessInstancesResult.InstanceAction[] {});
		}

	}

	private static ProcessInstance getActionsUrl(Cluster cluster,
			Process process, ProcessInstance processInstance,
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
		return new ProcessInstance(processInstance, oozieLogFile,
				instanceActions);

	}

	private static String getConsoleURL(String oozieUrl, String externalId)
			throws OozieClientException {
		OozieClient client = new OozieClient(oozieUrl);
		String jobId = client.getJobId(externalId);
		WorkflowJob jobInfo = client.getJobInfo(jobId);
		return jobInfo.getConsoleUrl();
	}

	private static ExternalId getExternalId(String processName, String type,
			String date) throws ValidationException {
		if (type == null || type.equalsIgnoreCase("DEFAULT")) {
			return new ExternalId(processName, "DEFAULT", date);
		} else if (type.equalsIgnoreCase("LATE1")) {
			return new ExternalId(processName, "LATE1", date);
		} else {
			throw new ValidationException("Query param type: " + type
					+ " is not valid");
		}

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

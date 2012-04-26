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

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.parser.ValidationException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.resource.ProcessInstancesResult;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

public final class LogProvider {
	private static final Logger LOG = Logger.getLogger(LogProvider.class);

	public static String getLogUrl(String processName,
			ProcessInstancesResult.ProcessInstance processInstance, String type)
			throws IvoryException {
		Process process = ConfigurationStore.get().get(EntityType.PROCESS,
				processName);
		Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER,
				process.getCluster().getName());
		if (processInstance.status.equals(WorkflowStatus.RUNNING)
				|| processInstance.status.equals(WorkflowStatus.SUSPENDED)) {
			try {
				String url = getConsoleURL(
						ClusterHelper.getOozieUrl(cluster),
						getExternalId(
								processName,
								type,
								EntityUtil
										.parseDateUTC(processInstance.instance))
								.getId());
				return url;
			} catch (OozieClientException e) {
				LOG.warn("Exception in getLogURL:", e);
				return "-";
			}
		}

		Date date = EntityUtil.parseDateUTC(processInstance.instance);
		String logPath = getExternalId(processName, type, date).getDFSname();
		String logLocation = getDFSbrowserUrl(
				ClusterHelper.getHdfsUrl(cluster), process.getWorkflow()
						.getPath(), logPath);
		return logLocation;
	}

	private static String getConsoleURL(String oozieUrl, String externalId)
			throws OozieClientException {
		OozieClient client = new OozieClient(oozieUrl);
		String jobId = client.getJobId(externalId);
		WorkflowJob jobInfo = client.getJobInfo(jobId);
		return jobInfo.getConsoleUrl();
	}

	private static ExternalId getExternalId(String processName, String type,
			Date date) throws ValidationException {
		if (type == null || type.equalsIgnoreCase("DEFAULT")) {
			return new ExternalId(processName, "DEFAULT", date);
		} else if (type.equalsIgnoreCase("LATE1")) {
			return new ExternalId(processName, "LATE1", date);
		} else {
			throw new ValidationException("Query param type: " + type
					+ " is not valid");
		}

	}

	private static String getDFSbrowserUrl(String nameNode,
			String workflowPath, String logFile) throws IvoryException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", nameNode);
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(nameNode
				+ "/" + workflowPath + "/log/" + logFile);
		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(path)) {
				return path.toString();
			} else {
				LOG.warn("Path: " + path + " does not exists");
				return "-";
			}
		} catch (IOException e) {
			throw new IvoryException(e);
		}
	}

}

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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.tools.ant.filters.StringInputStream;

public class LogMover extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(LogMover.class);

	private static class ARGS {
		String oozieUrl;
		String subflowId;
		String runId;
		String logDir;
		String externalId;
		String status;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new LogMover(), args);
	}

	@Override
	public int run(String[] arguments) throws Exception {
		if (arguments.length != 6) {
			throw new IllegalArgumentException("Expecting 6 arguments");
		}
		ARGS args = new ARGS();
		setupArgs(arguments, args);
		OozieClient client = new OozieClient(args.oozieUrl);
		WorkflowJob jobInfo = client.getJobInfo(args.subflowId);
		String subflowId = jobInfo.getExternalId();
		WorkflowJob subflowInfo = client.getJobInfo(subflowId);
		List<WorkflowAction> actions = subflowInfo.getActions();
		Path path = new Path(args.logDir + "/" + args.externalId + "/"
				+ args.runId);
		FileSystem fs = path.getFileSystem(getConf());
		for (WorkflowAction action : actions) {
			try {
				if (action.getType().equals("pig")
						|| action.getType().equals("java")) {
					String ttLogURL = getTTlogURL(action.getExternalId());
					if (ttLogURL != null) {
						LOG.info("Fetching log for action: "
								+ action.getExternalId() + " from url: "
								+ ttLogURL);
						InputStream in = getURLinputStream(new URL(ttLogURL));
						OutputStream out = fs.create(new Path(path, action
								.getName() + ".log"));
						IOUtils.copyBytes(in, out, 4096, true);
						LOG.info("Copied log to " + path);
					}
				} else {
					LOG.info("Ignoring hadoop TT log for non-pig and non-java action:"
							+ action.getName());
				}
			} catch (Exception e) {
				LOG.error("Exception while fetching TT log for action: "
						+ action.getName(), e);
			}
		}
		InputStream in = new StringInputStream(client.getJobLog(subflowId));
		OutputStream out = fs.create(new Path(path, "oozie.log"));
		IOUtils.copyBytes(in, out, 4096, true);
		LOG.info("Copied oozie log to " + path);

		return 0;
	}

	private void setupArgs(String[] arguments, ARGS args) {
		LOG.info("Arguments:");
		LOG.info("Oozie url:" + arguments[0]);
		args.oozieUrl = arguments[0];
		LOG.info("Subflow-id:" + arguments[1]);
		args.subflowId = arguments[1];
		LOG.info("Run id:" + arguments[2]);
		args.runId = arguments[2];
		LOG.info("Log dir path:" + arguments[3]);
		args.logDir = arguments[3];
		LOG.info("ExternalId:" + arguments[4]);
		args.externalId = arguments[4].replaceAll(":", "-");
		LOG.info("Status:" + arguments[5]);
		args.status = arguments[5];
	}

	private String getTTlogURL(String jobId) throws Exception {
		JobConf jobConf = new JobConf(getConf());
		JobClient jobClient = new JobClient(jobConf);
		RunningJob job = jobClient.getJob(JobID.forName(jobId));
		if (job == null) {
			LOG.warn("No running job for job id: " + jobId);
			return null;
		}
		TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0);
		// 0th even is setup, 1 event is launcher, 2 event is cleanup
		if (tasks != null && tasks.length == 3 && tasks[1] != null) {
			return tasks[1].getTaskTrackerHttp() + "/tasklog?attemptid="
					+ tasks[1].getTaskAttemptId() + "&all=true";
		} else {
			LOG.warn("No running task for job: " + jobId);
		}
		return null;
	}

	private InputStream getURLinputStream(URL url) throws IOException {
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		connection.connect();
		return connection.getInputStream();
	}

}

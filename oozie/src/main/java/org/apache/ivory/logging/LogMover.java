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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.ivory.entity.v0.EntityType;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
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
		String status;
		String entityType;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new LogMover(), args);
	}

	@Override
	public int run(String[] arguments) throws Exception {
		try {
			ARGS args = new ARGS();
			setupArgs(arguments, args);
			OozieClient client = new OozieClient(args.oozieUrl);
			WorkflowJob jobInfo = null;
			try {
				jobInfo = client.getJobInfo(args.subflowId);
			} catch (OozieClientException e) {
				LOG.error("Error getting jobinfo for: " + args.subflowId, e);
				return 0;
			}
			Path path = new Path(args.logDir + "/"
					+ String.format("%03d", Integer.parseInt(args.runId)));

			FileSystem fs = path.getFileSystem(getConf());

			if (args.entityType.equalsIgnoreCase(EntityType.FEED.name())) {
				// if replication wf
				copyTTlogs(args, fs, path, jobInfo.getActions().get(0));
				copyOozieLog(client, fs, path, jobInfo.getId());
			} else {
				// if process wf
				String subflowId = jobInfo.getExternalId();
				WorkflowJob subflowInfo = client.getJobInfo(subflowId);
				List<WorkflowAction> actions = subflowInfo.getActions();
				for (WorkflowAction action : actions) {

					if (action.getType().equals("pig")
							|| action.getType().equals("java")) {
						copyTTlogs(args, fs, path, action);
					} else {
						LOG.info("Ignoring hadoop TT log for non-pig and non-java action:"
								+ action.getName());
					}
				}
				copyOozieLog(client, fs, path, subflowId);
			}

		} catch (Exception e) {
			LOG.error("Exception in log mover:", e);
		}
		return 0;
	}

	private void copyOozieLog(OozieClient client, FileSystem fs, Path path,
			String id) throws OozieClientException, IOException {
		InputStream in = new StringInputStream(client.getJobLog(id));
		OutputStream out = fs.create(new Path(path, "oozie.log"));
		IOUtils.copyBytes(in, out, 4096, true);
		LOG.info("Copied oozie log to " + path);
	}

	private void copyTTlogs(ARGS args, FileSystem fs, Path path,
			WorkflowAction action) throws Exception {
		String ttLogURL = getTTlogURL(action.getExternalId());
		if (ttLogURL != null) {
			LOG.info("Fetching log for action: " + action.getExternalId()
					+ " from url: " + ttLogURL);
			InputStream in = getURLinputStream(new URL(ttLogURL));
			OutputStream out = fs.create(new Path(path, action.getName() + "_"
					+ action.getStatus() + ".log"));
			IOUtils.copyBytes(in, out, 4096, true);
			LOG.info("Copied log to " + path);
		}
	}

	private void setupArgs(String[] arguments, ARGS args) throws ParseException {
		Options options = new Options();
		Option opt;
		opt = new Option("workflowengineurl", true,
				"url of workflow engine, ex:oozie");
		opt.setRequired(true);
		options.addOption(opt);
		opt = new Option("subflowid", true, "external id of userworkflow");
		opt.setRequired(true);
		options.addOption(opt);
		opt = new Option("runid", true, "current workflow's runid");
		opt.setRequired(true);
		options.addOption(opt);
		opt = new Option("logdir", true, "log dir where job logs are stored");
		opt.setRequired(true);
		options.addOption(opt);
		opt = new Option("status", true, "user workflow status");
		opt.setRequired(true);
		options.addOption(opt);
		opt = new Option("entityType", true, "entity type feed or process");
		opt.setRequired(true);
		options.addOption(opt);

		CommandLine cmd = new GnuParser().parse(options, arguments);

		args.oozieUrl = cmd.getOptionValue("workflowengineurl");
		args.subflowId = cmd.getOptionValue("subflowid");
		args.runId = cmd.getOptionValue("runid");
		args.logDir = cmd.getOptionValue("logdir");
		args.status = cmd.getOptionValue("status");
		args.entityType = cmd.getOptionValue("entityType");

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

/**
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
package org.apache.falcon.logging;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

/**
 * Utitlity called in the post process of oozie workflow to move oozie action executor log.
 */
public class LogMover extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(LogMover.class);

    /**
     * Args to the command.
     */
    private static class ARGS {
        private String oozieUrl;
        private String subflowId;
        private String runId;
        private String logDir;
        private String entityType;
        private String userWorkflowEngine;
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
            WorkflowJob jobInfo;
            try {
                jobInfo = client.getJobInfo(args.subflowId);
            } catch (OozieClientException e) {
                LOG.error("Error getting jobinfo for: " + args.subflowId, e);
                return 0;
            }

            Path path = new Path(args.logDir + "/"
                    + String.format("%03d", Integer.parseInt(args.runId)));
            FileSystem fs = path.getFileSystem(getConf());

            if (args.entityType.equalsIgnoreCase(EntityType.FEED.name())
                    || notUserWorkflowEngineIsOozie(args.userWorkflowEngine)) {
                // if replication wf or PIG Process
                copyOozieLog(client, fs, path, jobInfo.getId());

                List<WorkflowAction> workflowActions = jobInfo.getActions();
                for (int i=0; i < workflowActions.size(); i++) {
                    if (workflowActions.get(i).getName().equals("replication")) {
                        copyTTlogs(fs, path, jobInfo.getActions().get(i));
                        break;
                    }
                }
            } else {
                // if process wf with oozie engine
                String subflowId = jobInfo.getExternalId();
                copyOozieLog(client, fs, path, subflowId);
                WorkflowJob subflowInfo = client.getJobInfo(subflowId);
                List<WorkflowAction> actions = subflowInfo.getActions();
                for (WorkflowAction action : actions) {
                    if (action.getType().equals("pig")
                            || action.getType().equals("java")) {
                        copyTTlogs(fs, path, action);
                    } else {
                        LOG.info("Ignoring hadoop TT log for non-pig and non-java action:"
                                + action.getName());
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("Exception in log mover:", e);
        }
        return 0;
    }

    private boolean notUserWorkflowEngineIsOozie(String userWorkflowEngine) {
        // userWorkflowEngine will be null for replication and "pig" for pig
        return userWorkflowEngine != null && EngineType.fromValue(userWorkflowEngine) != EngineType.OOZIE;
    }

    private void copyOozieLog(OozieClient client, FileSystem fs, Path path,
                              String id) throws OozieClientException, IOException {
        InputStream in = new ByteArrayInputStream(client.getJobLog(id).getBytes());
        OutputStream out = fs.create(new Path(path, "oozie.log"));
        IOUtils.copyBytes(in, out, 4096, true);
        LOG.info("Copied oozie log to " + path);
    }

    private void copyTTlogs(FileSystem fs, Path path,
                            WorkflowAction action) throws Exception {
        String ttLogURL = getTTlogURL(action.getExternalId());
        if (ttLogURL != null) {
            LOG.info("Fetching log for action: " + action.getExternalId()
                    + " from url: " + ttLogURL);
            InputStream in = getURLinputStream(new URL(ttLogURL));
            OutputStream out = fs.create(new Path(path, action.getName() + "_"
                    + getMappedStatus(action.getStatus()) + ".log"));
            IOUtils.copyBytes(in, out, 4096, true);
            LOG.info("Copied log to " + path);
        }
    }

    private String getMappedStatus(WorkflowAction.Status status) {
        if (status == WorkflowAction.Status.FAILED
                || status == WorkflowAction.Status.KILLED
                || status == WorkflowAction.Status.ERROR) {
            return "FAILED";
        } else {
            return "SUCCEEDED";
        }
    }

    private void setupArgs(String[] arguments, ARGS args) throws ParseException {
        Options options = new Options();

        Option opt = new Option("workflowEngineUrl", true, "url of workflow engine, ex:oozie");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("subflowId", true, "external id of userworkflow");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("userWorkflowEngine", true, "user workflow engine type");
        opt.setRequired(false);  // replication will NOT have this arg sent
        options.addOption(opt);

        opt = new Option("runId", true, "current workflow's runid");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("logDir", true, "log dir where job logs are stored");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("status", true, "user workflow status");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("entityType", true, "entity type feed or process");
        opt.setRequired(true);
        options.addOption(opt);

        CommandLine cmd = new GnuParser().parse(options, arguments);

        args.oozieUrl = cmd.getOptionValue("workflowEngineUrl");
        args.subflowId = cmd.getOptionValue("subflowId");
        args.userWorkflowEngine = cmd.getOptionValue("userWorkflowEngine");
        args.runId = cmd.getOptionValue("runId");
        args.logDir = cmd.getOptionValue("logDir");
        args.entityType = cmd.getOptionValue("entityType");
    }

    private String getTTlogURL(String jobId) throws Exception {
        TaskLogURLRetriever logRetriever = ReflectionUtils.newInstance(getLogRetrieverClassName(), getConf());
        return logRetriever.retrieveTaskLogURL(jobId);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends TaskLogURLRetriever> getLogRetrieverClassName() {
        try {
            return (Class<? extends TaskLogURLRetriever>)
                    Class.forName("org.apache.falcon.logging.v1.TaskLogRetrieverV1");
        } catch (ClassNotFoundException e) {
            LOG.warn("V1 Retriever missing, falling back to Default retriever");
            return DefaultTaskLogRetriever.class;
        }
    }

    private InputStream getURLinputStream(URL url) throws IOException {
        URLConnection connection = url.openConnection();
        connection.setDoOutput(true);
        connection.connect();
        return connection.getInputStream();
    }
}

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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.util.OozieActionConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility called in the post process of oozie workflow to move oozie action executor log.
 */
public class JobLogMover {

    private static final Logger LOG = LoggerFactory.getLogger(JobLogMover.class);
    private static final String YARN = "yarn";
    private static final String MAPREDUCE_FRAMEWORK = "mapreduce.framework.name";

    private static final Set<String> FALCON_ACTIONS =
        new HashSet<String>(Arrays.asList(new String[]{"eviction", "replication", }));

    private Configuration getConf() {
        Configuration conf = null;
        try {
            conf = OozieActionConfigurationHelper.createActionConf();
        } catch (IOException ioe) {
            LOG.warn("Cannot get Oozie configuration.  Returning default");
        }
        return conf == null ? new Configuration(): conf;
    }

    public void moveLog(WorkflowExecutionContext context){
        if (UserGroupInformation.isSecurityEnabled()) {
            LOG.info("Unable to move logs as security is enabled.");
            return;
        }
        try {
            run(context);
        } catch (Exception ignored) {
            // Mask exception, a failed log mover will not fail the user workflow
            LOG.error("Exception in job log mover:", ignored);
        }
    }

    public int run(WorkflowExecutionContext context) {
        try {
            String engineUrl = context.getWorkflowEngineUrl();
            if (StringUtils.isBlank(engineUrl)) {
                LOG.warn("Unable to retrieve workflow url for {} with status {} ",
                        context.getWorkflowId(), context.getWorkflowStatus());
                return 0;
            }
            String instanceOwner = context.getWorkflowUser();
            if (StringUtils.isNotBlank(instanceOwner)) {
                CurrentUser.authenticate(instanceOwner);
            } else {
                CurrentUser.authenticate(System.getProperty("user.name"));
            }
            OozieClient client = new OozieClient(engineUrl);
            WorkflowJob jobInfo;
            try {
                jobInfo = client.getJobInfo(context.getWorkflowId());
            } catch (OozieClientException e) {
                LOG.error("Error getting jobinfo for: {}", context.getUserSubflowId(), e);
                return 0;
            }
            //Assumption is - Each wf run will have a directory
            //the corresponding job logs are stored within the respective dir
            Path path = new Path(context.getLogDir() + "/" + context.getNominalTime() + "/"
                    + String.format("%03d", context.getWorkflowRunId()));
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri(), getConf());

            if (EntityType.FEED.name().equalsIgnoreCase(context.getEntityType())
                    || notUserWorkflowEngineIsOozie(context.getUserWorkflowEngine())) {
                // if replication wf, retention wf or PIG Process
                copyOozieLog(client, fs, path, jobInfo.getId());

                List<WorkflowAction> workflowActions = jobInfo.getActions();
                for (int i=0; i < workflowActions.size(); i++) {
                    if (FALCON_ACTIONS.contains(workflowActions.get(i).getName())) {
                        copyTTlogs(fs, path, jobInfo.getActions().get(i));
                        break;
                    }
                }
            } else {
                String flowId;
                // if process wf with pig, hive
                if (context.getUserWorkflowEngine().equals("pig")
                        ||context.getUserWorkflowEngine().equals("hive")) {
                    flowId = jobInfo.getId();
                } else {
                    jobInfo = client.getJobInfo(context.getUserSubflowId());
                    // if process wf with oozie engine
                    flowId = jobInfo.getExternalId();
                }
                copyOozieLog(client, fs, path, flowId);
                WorkflowJob subflowInfo = client.getJobInfo(flowId);
                List<WorkflowAction> actions = subflowInfo.getActions();
                for (WorkflowAction action : actions) {
                    if (isActionTypeSupported(action)) {
                        LOG.info("Copying hadoop TT log for action: {} of type: {}",
                                action.getName(), action.getType());
                        copyTTlogs(fs, path, action);
                    } else {
                        LOG.info("Ignoring hadoop TT log for non supported action: {} of type: {}",
                                action.getName(), action.getType());
                    }
                }
            }

        } catch (Exception e) {
            // JobLogMover doesn't throw exception, a failed log mover will not fail the user workflow
            LOG.error("Exception in log mover:", e);
        }
        return 0;
    }

    private boolean notUserWorkflowEngineIsOozie(String userWorkflowEngine) {
        // userWorkflowEngine will be null for replication and "not null" for pig, hive, oozie
        return userWorkflowEngine != null && EngineType.fromValue(userWorkflowEngine) == null;
    }

    private void copyOozieLog(OozieClient client, FileSystem fs, Path path,
                              String id) throws OozieClientException, IOException {
        InputStream in = new ByteArrayInputStream(client.getJobLog(id).getBytes());
        OutputStream out = fs.create(new Path(path, "oozie.log"));
        IOUtils.copyBytes(in, out, 4096, true);
        LOG.info("Copied oozie log to {}", path);
    }

    private void copyTTlogs(FileSystem fs, Path path,
                            WorkflowAction action) throws Exception {
        List<String> ttLogUrls = getTTlogURL(action.getExternalId());
        if (ttLogUrls != null) {
            int index = 1;
            for (String ttLogURL : ttLogUrls) {
                LOG.info("Fetching log for action: {} from url: {}", action.getExternalId(), ttLogURL);
                InputStream in = getURLinputStream(new URL(ttLogURL));
                OutputStream out = fs.create(new Path(path, action.getName() + "_" + action.getType() + "_"
                        + getMappedStatus(action.getStatus()) + "-" + index + ".log"));
                IOUtils.copyBytes(in, out, 4096, true);
                LOG.info("Copied log to {}", path);
                index++;
            }
        }
    }

    private boolean isActionTypeSupported(WorkflowAction action) {
        return action.getType().equals("pig")
                || action.getType().equals("hive")
                || action.getType().equals("java")
                || action.getType().equals("map-reduce");
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

    private List<String> getTTlogURL(String jobId) throws Exception {
        TaskLogURLRetriever logRetriever = ReflectionUtils
                .newInstance(getLogRetrieverClassName(getConf()), getConf());
        return logRetriever.retrieveTaskLogURL(jobId);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends TaskLogURLRetriever> getLogRetrieverClassName(Configuration conf) {
        if (YARN.equals(conf.get(MAPREDUCE_FRAMEWORK))) {
            return TaskLogRetrieverYarn.class;
        } else {
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

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
package org.apache.falcon.workflow.engine;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.exception.DAGEngineException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A DAG Engine that uses Oozie to execute the DAG.
 */
public class OozieDAGEngine implements DAGEngine {
    private static final Logger LOG = LoggerFactory.getLogger(OozieDAGEngine.class);
    private final OozieClient client;
    private static final int WORKFLOW_STATUS_RETRY_DELAY_MS = 100;

    private static final String WORKFLOW_STATUS_RETRY_COUNT = "workflow.status.retry.count";
    private static final List<String> PARENT_WF_ACTION_NAMES = Arrays.asList(
            "pre-processing",
            "recordsize",
            "succeeded-post-processing",
            "failed-post-processing"
    );

    public static final String INSTANCE_FORMAT = "yyyy-MM-dd-HH-mm";
    private final Cluster cluster;

    public OozieDAGEngine(Cluster cluster) throws DAGEngineException {
        try {
            client = OozieClientFactory.get(cluster);
            this.cluster = cluster;
        } catch (Exception e) {
            throw new DAGEngineException(e);
        }
    }

    public OozieDAGEngine(String clusterName) throws DAGEngineException {
        try {
            this.cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);
            client = OozieClientFactory.get(cluster);
        } catch (Exception e) {
            throw new DAGEngineException(e);
        }
    }

    @Override
    public String run(ExecutionInstance instance) throws DAGEngineException {
        try {
            Properties properties = getRunProperties(instance);
            Path buildPath = EntityUtil.getLatestStagingPath(cluster, instance.getEntity());
            switchUser();
            properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
            properties.setProperty(OozieClient.APP_PATH, buildPath.toString());
            return client.run(properties);
        } catch (OozieClientException e) {
            LOG.error("Oozie client exception:", e);
            throw new DAGEngineException(e);
        } catch (FalconException e1) {
            LOG.error("Falcon Exception : ", e1);
            throw new DAGEngineException(e1);
        }
    }

    private void prepareEntityBuildPath(Entity entity) throws FalconException {
        Path stagingPath = EntityUtil.getBaseStagingPath(cluster, entity);
        Path logPath = EntityUtil.getLogPath(cluster, entity);

        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                    ClusterHelper.getConfiguration(cluster));
            HadoopClientFactory.mkdirsWithDefaultPerms(fs, stagingPath);
            HadoopClientFactory.mkdirsWithDefaultPerms(fs, logPath);
        } catch (IOException e) {
            throw new FalconException("Error preparing base staging dirs: " + stagingPath, e);
        }
    }

    private void dryRunInternal(Properties properties, Path buildPath, Entity entity)
        throws OozieClientException, DAGEngineException {
        if (properties == null) {
            LOG.info("Entity {} is not scheduled on cluster {}", entity.getName(), cluster);
            throw new DAGEngineException("Properties for entity " + entity.getName() + " is empty");
        }

        switchUser();
        LOG.debug("Logged in user is " + CurrentUser.getUser());
        properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
        properties.setProperty(OozieClient.APP_PATH, buildPath.toString());
        properties.putAll(getDryRunProperties(entity));
        //Do dryrun before run as run is asynchronous
        LOG.info("Dry run with properties {}", properties);
        client.dryrun(properties);
    }

    private void switchUser() {
        String user = System.getProperty("user.name");
        CurrentUser.authenticate(user);
    }

    @Override
    public boolean isScheduled(ExecutionInstance instance) throws DAGEngineException {
        try {
            return statusEquals(client.getJobInfo(instance.getExternalID()).getStatus().name(),
                    Job.Status.PREP, Job.Status.RUNNING);
        } catch (OozieClientException e) {
            throw new DAGEngineException(e);
        }
    }

    // TODO : To be implemented. Currently hardcoded for process
    private Properties getRunProperties(ExecutionInstance instance) {
        Properties props = new Properties();
        DateTimeFormatter fmt = DateTimeFormat.forPattern(INSTANCE_FORMAT);
        String nominalTime = fmt.print(instance.getInstanceTime());
        props.put("nominalTime", nominalTime);
        props.put("timeStamp", nominalTime);
        props.put("feedNames", "NONE");
        props.put("feedInstancePaths", "NONE");
        props.put("falconInputFeeds", "NONE");
        props.put("falconInPaths", "NONE");
        props.put("feedNames", "NONE");
        props.put("feedInstancePaths", "NONE");
        props.put("userJMSNotificationEnabled", "true");
        props.put("systemJMSNotificationEnabled", "false");
        return props;
    }

    // TODO : To be implemented. Currently hardcoded for process
    private Properties getDryRunProperties(Entity entity) {
        Properties props = new Properties();
        DateTimeFormatter fmt = DateTimeFormat.forPattern(INSTANCE_FORMAT);
        String nominalTime = fmt.print(DateTime.now());
        props.put("nominalTime", nominalTime);
        props.put("timeStamp", nominalTime);
        props.put("feedNames", "NONE");
        props.put("feedInstancePaths", "NONE");
        props.put("falconInputFeeds", "NONE");
        props.put("falconInPaths", "NONE");
        props.put("feedNames", "NONE");
        props.put("feedInstancePaths", "NONE");
        props.put("userJMSNotificationEnabled", "true");
        props.put("systemJMSNotificationEnabled", "false");
        return props;
    }

    @Override
    public void suspend(ExecutionInstance instance) throws DAGEngineException {
        try {
            client.suspend(instance.getExternalID());
            assertStatus(instance.getExternalID(), Job.Status.PREPSUSPENDED, Job.Status.SUSPENDED, Job.Status.SUCCEEDED,
                    Job.Status.FAILED, Job.Status.KILLED);
            LOG.info("Suspended job {} on cluster {}", instance.getExternalID(), instance.getCluster());
        } catch (OozieClientException e) {
            throw new DAGEngineException(e);
        }
    }

    @Override
    public void resume(ExecutionInstance instance) throws DAGEngineException {
        try {
            client.resume(instance.getExternalID());
            assertStatus(instance.getExternalID(), Job.Status.PREP, Job.Status.RUNNING, Job.Status.SUCCEEDED,
                    Job.Status.FAILED, Job.Status.KILLED);
            LOG.info("Resumed job {} on cluster {}", instance.getExternalID(), instance.getCluster());
        } catch (OozieClientException e) {
            throw new DAGEngineException(e);
        }
    }

    @Override
    public void kill(ExecutionInstance instance) throws DAGEngineException {
        try {
            client.kill(instance.getExternalID());
            assertStatus(instance.getExternalID(), Job.Status.KILLED, Job.Status.SUCCEEDED, Job.Status.FAILED);
            LOG.info("Killed job {} on cluster {}", instance.getExternalID(), instance.getCluster());
        } catch (OozieClientException e) {
            throw new DAGEngineException(e);
        }
    }

    @Override
    public void reRun(ExecutionInstance instance) throws DAGEngineException {
        // TODO : Implement this
    }

    @Override
    public void submit(Entity entity) throws DAGEngineException {
        try {
            // TODO : remove hardcoded Tag value when feed support is added.
            OozieOrchestrationWorkflowBuilder builder =
                    OozieOrchestrationWorkflowBuilder.get(entity, cluster, Tag.DEFAULT);
            prepareEntityBuildPath(entity);
            Path buildPath = EntityUtil.getNewStagingPath(cluster, entity);
            Properties properties = builder.build(cluster, buildPath);
            dryRunInternal(properties, buildPath, entity);
        } catch (OozieClientException e) {
            LOG.error("Oozie client exception:", e);
            throw new DAGEngineException(e);
        } catch (FalconException e1) {
            LOG.error("Falcon Exception : ", e1);
            throw new DAGEngineException(e1);
        }
    }

    @Override
    public InstancesResult.Instance info(String externalID) throws DAGEngineException {
        InstancesResult.Instance instance = new InstancesResult.Instance();
        try {
            LOG.debug("Retrieving details for job {} ", externalID);
            WorkflowJob jobInfo = client.getJobInfo(externalID);
            instance.startTime = jobInfo.getStartTime();
            if (jobInfo.getStatus().name().equals(Job.Status.RUNNING.name())) {
                instance.endTime = new Date();
            } else {
                instance.endTime = jobInfo.getEndTime();
            }
            instance.cluster = cluster.getName();
            instance.runId = jobInfo.getRun();
            instance.status = InstancesResult.WorkflowStatus.valueOf(jobInfo.getStatus().name());
            instance.logFile = jobInfo.getConsoleUrl();
            instance.wfParams = getWFParams(jobInfo);
            return instance;
        } catch (Exception e) {
            LOG.error("Error when attempting to get info for " + externalID, e);
            throw new DAGEngineException(e);
        }
    }

    private InstancesResult.KeyValuePair[] getWFParams(WorkflowJob jobInfo) {
        Configuration conf = new Configuration(false);
        conf.addResource(new ByteArrayInputStream(jobInfo.getConf().getBytes()));
        InstancesResult.KeyValuePair[] wfParams = new InstancesResult.KeyValuePair[conf.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : conf) {
            wfParams[i++] = new InstancesResult.KeyValuePair(entry.getKey(), entry.getValue());
        }
        return wfParams;
    }

    @Override
    public List<InstancesResult.InstanceAction> getJobDetails(String externalID) throws DAGEngineException {
        List<InstancesResult.InstanceAction> instanceActions = new ArrayList<>();
        try {
            WorkflowJob wfJob = client.getJobInfo(externalID);
            List<WorkflowAction> wfActions = wfJob.getActions();
            // We wanna capture job urls for all user-actions & non succeeded actions of the main workflow
            for (WorkflowAction action : wfActions) {
                if (action.getType().equalsIgnoreCase("sub-workflow")
                        && StringUtils.isNotEmpty(action.getExternalId())) {
                    // if the action is sub-workflow, get job urls of all actions within the sub-workflow
                    List<WorkflowAction> subWorkFlowActions = client
                            .getJobInfo(action.getExternalId()).getActions();
                    for (WorkflowAction subWfAction : subWorkFlowActions) {
                        if (!subWfAction.getType().startsWith(":")) {
                            InstancesResult.InstanceAction instanceAction =
                                    new InstancesResult.InstanceAction(subWfAction.getName(),
                                            subWfAction.getExternalStatus(), subWfAction.getConsoleUrl());
                            instanceActions.add(instanceAction);
                        }
                    }
                } else if (!action.getType().startsWith(":")) {
                    // if the action is a transition node it starts with :, we don't need their statuses
                    if (PARENT_WF_ACTION_NAMES.contains(action.getName())
                            && !Job.Status.SUCCEEDED.toString().equals(action.getExternalStatus())) {
                        // falcon actions in the main workflow are defined in the list
                        // get job urls for all non succeeded actions of the main workflow
                        InstancesResult.InstanceAction instanceAction =
                                new InstancesResult.InstanceAction(action.getName(), action.getExternalStatus(),
                                        action.getConsoleUrl());
                        instanceActions.add(instanceAction);
                    } else if (!PARENT_WF_ACTION_NAMES.contains(action.getName())
                            && !StringUtils.equals(action.getExternalId(), "-")) {
                        // if user-action is pig/hive there is no sub-workflow, we wanna capture their urls as well
                        InstancesResult.InstanceAction instanceAction =
                                new InstancesResult.InstanceAction(action.getName(), action.getExternalStatus(),
                                        action.getConsoleUrl());
                        instanceActions.add(instanceAction);
                    }
                }
            }
            return instanceActions;
        } catch (OozieClientException oce) {
            throw new DAGEngineException(oce);
        }
    }

    @Override
    public boolean isAlive() throws DAGEngineException {
        try {
            return client.getSystemMode() == OozieClient.SYSTEM_MODE.NORMAL;
        } catch (OozieClientException e) {
            throw new DAGEngineException("Unable to reach Oozie server.", e);
        }
    }

    @Override
    public Properties getConfiguration(String externalID) throws DAGEngineException {
        Properties props = new Properties();
        try {
            WorkflowJob jobInfo = client.getJobInfo(externalID);
            Configuration conf = new Configuration(false);
            conf.addResource(new ByteArrayInputStream(jobInfo.getConf().getBytes()));

            for (Map.Entry<String, String> entry : conf) {
                props.put(entry.getKey(), entry.getValue());
            }
        } catch (OozieClientException e) {
            throw new DAGEngineException(e);
        }

        return props;
    }

    @Override
    public void touch(Entity entity, Boolean skipDryRun) throws DAGEngineException {
        // TODO : remove hardcoded Tag value when feed support is added.
        try {
            OozieOrchestrationWorkflowBuilder builder =
                    OozieOrchestrationWorkflowBuilder.get(entity, cluster, Tag.DEFAULT);
            if (!skipDryRun) {
                Path buildPath = new Path("/tmp", "falcon" + entity.getName() + System.currentTimeMillis());
                Properties props = builder.build(cluster, buildPath);
                dryRunInternal(props, buildPath, entity);
            }
            Path buildPath = EntityUtil.getNewStagingPath(cluster, entity);
            // build it and forget it. The next run will always pick up from the latest staging path.
            builder.build(cluster, buildPath);
        } catch (FalconException fe) {
            LOG.error("Falcon Exception : ", fe);
            throw new DAGEngineException(fe);
        } catch (OozieClientException e) {
            LOG.error("Oozie client exception:", e);
            throw new DAGEngineException(e);
        }
    }

    // Get status of a workflow (with retry) and ensure it is one of statuses requested.
    private void assertStatus(String jobID, Job.Status... statuses) throws DAGEngineException {
        String actualStatus = null;
        int retryCount;
        String retry = RuntimeProperties.get().getProperty(WORKFLOW_STATUS_RETRY_COUNT, "30");
        try {
            retryCount = Integer.valueOf(retry);
        } catch (NumberFormatException nfe) {
            throw new DAGEngineException("Invalid value provided for runtime property \""
                    + WORKFLOW_STATUS_RETRY_COUNT + "\". Please provide an integer value.");
        }
        for (int counter = 0; counter < retryCount; counter++) {
            try {
                actualStatus = client.getJobInfo(jobID).getStatus().name();
            } catch (OozieClientException e) {
                LOG.error("Unable to get status of workflow: " + jobID, e);
                throw new DAGEngineException(e);
            }
            if (!statusEquals(actualStatus, statuses)) {
                try {
                    Thread.sleep(WORKFLOW_STATUS_RETRY_DELAY_MS);
                } catch (InterruptedException ignore) {
                    //ignore
                }
            } else {
                return;
            }
        }
        throw new DAGEngineException("For Job" + jobID + ", actual statuses: " + actualStatus + ", expected statuses: "
                + Arrays.toString(statuses));
    }

    private boolean statusEquals(String left, Job.Status... right) {
        for (Job.Status rightElement : right) {
            if (left.equals(rightElement.name())) {
                return true;
            }
        }
        return false;
    }
}

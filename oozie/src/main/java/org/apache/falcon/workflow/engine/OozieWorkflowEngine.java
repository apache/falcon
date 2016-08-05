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
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityGraph;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieBundleBuilder;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.bundle.CONFIGURATION.Property;
import org.apache.falcon.oozie.bundle.COORDINATOR;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.InstancesSummaryResult.InstanceSummary;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.update.UpdateHelper;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.JMSConnectionInfo;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.RestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;

/**
 * Workflow engine which uses oozies APIs.
 */
public class OozieWorkflowEngine extends AbstractWorkflowEngine {

    private static final Logger LOG = LoggerFactory.getLogger(OozieWorkflowEngine.class);

    private static final BundleJob MISSING = new NullBundleJob();

    private static final List<WorkflowJob.Status> WF_KILL_PRECOND =
        Arrays.asList(WorkflowJob.Status.PREP, WorkflowJob.Status.RUNNING, WorkflowJob.Status.SUSPENDED,
                WorkflowJob.Status.FAILED);
    private static final List<WorkflowJob.Status> WF_SUSPEND_PRECOND = Arrays.asList(WorkflowJob.Status.RUNNING);
    private static final List<WorkflowJob.Status> WF_RESUME_PRECOND = Arrays.asList(WorkflowJob.Status.SUSPENDED);
    private static final List<CoordinatorAction.Status> COORD_RERUN_PRECOND =
        Arrays.asList(CoordinatorAction.Status.TIMEDOUT, CoordinatorAction.Status.FAILED,
                CoordinatorAction.Status.KILLED, CoordinatorAction.Status.SUCCEEDED);
    private static final List<Job.Status> BUNDLE_ACTIVE_STATUS =
        Arrays.asList(Job.Status.PREP, Job.Status.RUNNING, Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED,
            Job.Status.RUNNINGWITHERROR, Job.Status.PAUSED, Status.PREPPAUSED, Status.PAUSEDWITHERROR);
    private static final List<Job.Status> BUNDLE_SUSPENDED_STATUS =
        Arrays.asList(Job.Status.PREPSUSPENDED, Job.Status.SUSPENDED, Status.SUSPENDEDWITHERROR);
    private static final List<Job.Status> BUNDLE_RUNNING_STATUS = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING,
            Job.Status.RUNNINGWITHERROR);
    private static final List<Job.Status> BUNDLE_SUCCEEDED_STATUS = Arrays.asList(Job.Status.SUCCEEDED);
    private static final List<Job.Status> BUNDLE_FAILED_STATUS = Arrays.asList(Job.Status.FAILED,
            Job.Status.DONEWITHERROR);
    private static final List<Job.Status> BUNDLE_KILLED_STATUS = Arrays.asList(Job.Status.KILLED);

    private static final List<Job.Status> BUNDLE_SUSPEND_PRECOND =
        Arrays.asList(Job.Status.PREP, Job.Status.RUNNING, Job.Status.DONEWITHERROR);
    private static final List<Job.Status> BUNDLE_RESUME_PRECOND =
        Arrays.asList(Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED);
    private static final String FALCON_INSTANCE_ACTION_CLUSTERS = "falcon.instance.action.clusters";
    private static final String FALCON_INSTANCE_SOURCE_CLUSTERS = "falcon.instance.source.clusters";
    private static final String FALCON_SKIP_DRYRUN = "falcon.skip.dryrun";

    private static final int WORKFLOW_STATUS_RETRY_DELAY_MS = 100; // milliseconds
    private static final String WORKFLOW_STATUS_RETRY_COUNT = "workflow.status.retry.count";

    private static final List<String> PARENT_WF_ACTION_NAMES = Arrays.asList(
            "pre-processing",
            "recordsize",
            "succeeded-post-processing",
            "failed-post-processing"
    );

    private static final String[] BUNDLE_UPDATEABLE_PROPS =
        new String[]{"parallel", "clusters.clusters[\\d+].validity.end", };

    public static final ConfigurationStore STORE = ConfigurationStore.get();

    public OozieWorkflowEngine() {
        registerListener(new OozieHouseKeepingService());
    }

    @Override
    public boolean isAlive(Cluster cluster) throws FalconException {
        try {
            return OozieClientFactory.get(cluster).getSystemMode() == OozieClient.SYSTEM_MODE.NORMAL;
        } catch (OozieClientException e) {
            throw new FalconException("Unable to reach Oozie server.", e);
        }
    }

    @Override
    public void schedule(Entity entity, Boolean skipDryRun, Map<String, String> suppliedProps) throws FalconException {
        if (StartupProperties.isServerInSafeMode()) {
            throwSafemodeException("SCHEDULE");
        }
        Map<String, BundleJob> bundleMap = findLatestBundle(entity);
        List<String> schedClusters = new ArrayList<String>();
        for (Map.Entry<String, BundleJob> entry : bundleMap.entrySet()) {
            String cluster = entry.getKey();
            BundleJob bundleJob = entry.getValue();
            if (bundleJob == MISSING) {
                schedClusters.add(cluster);
            } else {
                LOG.debug("Entity {} is already scheduled on cluster {}", entity.getName(), cluster);
            }
        }

        if (!schedClusters.isEmpty()) {
            OozieEntityBuilder builder = OozieEntityBuilder.get(entity);
            for (String clusterName: schedClusters) {
                Cluster cluster = STORE.get(EntityType.CLUSTER, clusterName);
                prepareEntityBuildPath(entity, cluster);
                Path buildPath = EntityUtil.getNewStagingPath(cluster, entity);
                Properties properties = builder.build(cluster, buildPath, suppliedProps);
                if (properties == null) {
                    LOG.info("Entity {} is not scheduled on cluster {}", entity.getName(), cluster);
                    continue;
                }

                //Do dryRun of coords before schedule as schedule is asynchronous
                dryRunInternal(cluster, new Path(properties.getProperty(OozieEntityBuilder.ENTITY_PATH)), skipDryRun);
                scheduleEntity(clusterName, properties, entity);
            }
        }
    }

    private void throwSafemodeException(String operation) throws FalconException {
        String error = "Workflow Engine does not allow " + operation + " opeartion when Falcon server is in safemode";
        LOG.error(error);
        throw new FalconException(error);
    }

    /**
     * Prepare the staging and logs dir for this entity with default permissions.
     *
     * @param entity  entity
     * @param cluster cluster entity
     * @throws FalconException
     */
    private void prepareEntityBuildPath(Entity entity, Cluster cluster) throws FalconException {
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

    @Override
    public void dryRun(Entity entity, String clusterName, Boolean skipDryRun) throws FalconException {
        if (StartupProperties.isServerInSafeMode()) {
            throwSafemodeException("DRYRUN");
        }
        OozieEntityBuilder builder = OozieEntityBuilder.get(entity);
        Path buildPath = new Path("/tmp", "falcon" + entity.getName() + System.currentTimeMillis());
        Cluster cluster = STORE.get(EntityType.CLUSTER, clusterName);
        Properties props = builder.build(cluster, buildPath);
        if (props != null) {
            dryRunInternal(cluster, new Path(props.getProperty(OozieEntityBuilder.ENTITY_PATH)), skipDryRun);
        }
    }

    private void dryRunInternal(Cluster cluster, Path buildPath, Boolean skipDryRun) throws FalconException {
        if (null != skipDryRun && skipDryRun) {
            LOG.info("Skipping dryrun as directed by param in cli/RestApi.");
            return;
        } else {
            String skipDryRunStr = RuntimeProperties.get().getProperty(FALCON_SKIP_DRYRUN, "false").toLowerCase();
            if (Boolean.valueOf(skipDryRunStr)) {
                LOG.info("Skipping dryrun as directed by Runtime properties.");
                return;
            }
        }

        BUNDLEAPP bundle = OozieBundleBuilder.unmarshal(cluster, buildPath);
        OozieClient client = OozieClientFactory.get(cluster.getName());
        for (COORDINATOR coord : bundle.getCoordinator()) {
            Properties props = new Properties();
            props.setProperty(OozieClient.COORDINATOR_APP_PATH, coord.getAppPath());
            for (Property prop : coord.getConfiguration().getProperty()) {
                props.setProperty(prop.getName(), prop.getValue());
            }
            try {
                LOG.info("dryRun with properties {}", props);
                client.dryrun(props);
            } catch (OozieClientException e) {
                throw new FalconException(e);
            }
        }
    }

    @Override
    public boolean isActive(Entity entity) throws FalconException {
        return isBundleInState(findLatestBundle(entity), BundleStatus.ACTIVE);
    }

    @Override
    public boolean isSuspended(Entity entity) throws FalconException {
        return isBundleInState(findLatestBundle(entity), BundleStatus.SUSPENDED);
    }

    @Override
    public boolean isCompleted(Entity entity) throws FalconException {
        Map<String, BundleJob> bundles = findLatestBundle(entity);
        return (isBundleInState(bundles, BundleStatus.SUCCEEDED)
                || isBundleInState(bundles, BundleStatus.FAILED)
                || isBundleInState(bundles, BundleStatus.KILLED));
    }

    @Override
    public boolean isMissing(Entity entity) throws FalconException {
        List<String> bundlesToRemove = new ArrayList<>();
        Map<String, BundleJob> bundles = findLatestBundle(entity);
        for (Map.Entry<String, BundleJob> clusterBundle : bundles.entrySet()) {
            if (clusterBundle.getValue() == MISSING) { // There is no active bundle for this cluster
                bundlesToRemove.add(clusterBundle.getKey());
            }
        }
        for (String bundleToRemove : bundlesToRemove) {
            bundles.remove(bundleToRemove);
        }
        if (bundles.size() == 0) {
            return true;
        }
        return false;
    }

    private enum BundleStatus {
        ACTIVE, RUNNING, SUSPENDED, FAILED, KILLED, SUCCEEDED
    }

    private boolean isBundleInState(Map<String, BundleJob> bundles,
                                    BundleStatus status) throws FalconException {

        // Need a separate list to avoid concurrent modification.
        List<String> bundlesToRemove = new ArrayList<>();
        // After removing MISSING bundles for clusters, if bundles.size() == 0, entity is not scheduled. Return false.
        for (Map.Entry<String, BundleJob> clusterBundle : bundles.entrySet()) {
            if (clusterBundle.getValue() == MISSING) { // There is no active bundle for this cluster
                bundlesToRemove.add(clusterBundle.getKey());
            }
        }
        for (String bundleToRemove : bundlesToRemove) {
            bundles.remove(bundleToRemove);
        }
        if (bundles.size() == 0) {
            return false;
        }

        for (BundleJob bundle : bundles.values()) {
            switch (status) {
            case ACTIVE:
                if (!BUNDLE_ACTIVE_STATUS.contains(bundle.getStatus())) {
                    return false;
                }
                break;

            case RUNNING:
                if (!BUNDLE_RUNNING_STATUS.contains(bundle.getStatus())) {
                    return false;
                }
                break;

            case SUSPENDED:
                if (!BUNDLE_SUSPENDED_STATUS.contains(bundle.getStatus())) {
                    return false;
                }
                break;

            case FAILED:
                if (!BUNDLE_FAILED_STATUS.contains(bundle.getStatus())) {
                    return false;
                }
                break;

            case KILLED:
                if (!BUNDLE_KILLED_STATUS.contains(bundle.getStatus())) {
                    return false;
                }
                break;

            case SUCCEEDED:
                if (!BUNDLE_SUCCEEDED_STATUS.contains(bundle.getStatus())) {
                    return false;
                }
                break;
            default:
            }
            LOG.debug("Bundle {} is in state {}", bundle.getAppName(), status.name());
        }
        return true;
    }

    //Return all bundles for the entity in the requested cluster
    private List<BundleJob> findBundles(Entity entity, String clusterName) throws FalconException {
        Cluster cluster = STORE.get(EntityType.CLUSTER, clusterName);
        List<BundleJob> filteredJobs = new ArrayList<BundleJob>();
        try {
            List<BundleJob> jobs = OozieClientFactory.get(cluster.getName()).getBundleJobsInfo(OozieClient.FILTER_NAME
                + "=" + EntityUtil.getWorkflowName(entity) + ";", 0, 256);
            if (jobs != null) {
                for (BundleJob job : jobs) {
                    // Path is extracted twice as to handle changes in hadoop configurations for nameservices.
                    if (EntityUtil.isStagingPath(cluster, entity,
                            new Path((new Path(job.getAppPath())).toUri().getPath()))) {
                        //Load bundle as coord info is not returned in getBundleJobsInfo()
                        BundleJob bundle = getBundleInfo(clusterName, job.getId());
                        filteredJobs.add(bundle);
                        LOG.debug("Found bundle {} with app path {} and status {}",
                                job.getId(), job.getAppPath(), job.getStatus());
                    }
                }
            }
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
        return filteredJobs;
    }

    //Return all bundles for the entity for each cluster
    private Map<String, List<BundleJob>> findBundles(Entity entity) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        Map<String, List<BundleJob>> jobMap = new HashMap<String, List<BundleJob>>();
        for (String cluster : clusters) {
            jobMap.put(cluster, findBundles(entity, cluster));
        }
        return jobMap;
    }

    //Return latest bundle(last created) for the entity for each cluster
    private Map<String, BundleJob> findLatestBundle(Entity entity) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        Map<String, BundleJob> jobMap = new HashMap<String, BundleJob>();
        for (String cluster : clusters) {
            BundleJob bundleJob = findLatestBundle(entity, cluster);
            jobMap.put(cluster, bundleJob);
        }
        return jobMap;
    }

    //Return latest bundle(last created) for the entity in the requested cluster
    private BundleJob findLatestBundle(Entity entity, String cluster) throws FalconException {
        List<BundleJob> bundles = findBundles(entity, cluster);
        if (bundles == null || bundles.isEmpty()) {
            return MISSING;
        }

        return Collections.max(bundles, new Comparator<BundleJob>() {
            @Override
            public int compare(BundleJob o1, BundleJob o2) {
                return o1.getCreatedTime().compareTo(o2.getCreatedTime());
            }
        });
    }

    @Override
    public String suspend(Entity entity) throws FalconException {
        return doBundleAction(entity, BundleAction.SUSPEND);
    }

    @Override
    public String resume(Entity entity) throws FalconException {
        return doBundleAction(entity, BundleAction.RESUME);
    }

    @Override
    public String delete(Entity entity) throws FalconException {
        return doBundleAction(entity, BundleAction.KILL);
    }

    @Override
    public String delete(Entity entity, String cluster) throws FalconException {
        return doBundleAction(entity, BundleAction.KILL, cluster);
    }

    private enum BundleAction {
        SUSPEND, RESUME, KILL
    }

    private String doBundleAction(Entity entity, BundleAction action) throws FalconException {
        if (StartupProperties.isServerInSafeMode() && !action.equals(BundleAction.SUSPEND)) {
            throwSafemodeException(action.name());
        }
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        String result = null;
        for (String cluster : clusters) {
            result = doBundleAction(entity, action, cluster);
        }
        return result;
    }

    private String doBundleAction(Entity entity, BundleAction action, String cluster) throws FalconException {
        List<BundleJob> jobs = findBundles(entity, cluster);
        beforeAction(entity, action, cluster);
        for (BundleJob job : jobs) {
            switch (action) {
            case SUSPEND:
                // not already suspended and preconditions are true
                if (!BUNDLE_SUSPENDED_STATUS.contains(job.getStatus()) && BUNDLE_SUSPEND_PRECOND.contains(
                    job.getStatus())) {
                    suspend(cluster, job.getId());
                }
                break;

            case RESUME:
                // not already running and preconditions are true
                if (!BUNDLE_RUNNING_STATUS.contains(job.getStatus()) && BUNDLE_RESUME_PRECOND.contains(
                    job.getStatus())) {
                    resume(cluster, job.getId());
                }
                break;

            case KILL:
                // not already killed and preconditions are true
                killBundle(cluster, job);
                break;

            default:
            }
        }
        afterAction(entity, action, cluster);
        return "SUCCESS";
    }

    private void killBundle(String clusterName, BundleJob job) throws FalconException {
        OozieClient client = OozieClientFactory.get(clusterName);
        try {
            //kill all coords
            for (CoordinatorJob coord : job.getCoordinators()) {
                client.kill(coord.getId());
                LOG.debug("Killed coord {} on cluster {}", coord.getId(), clusterName);
            }

            //kill bundle
            client.kill(job.getId());
            LOG.debug("Killed bundle {} on cluster {}", job.getId(), clusterName);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void beforeAction(Entity entity, BundleAction action, String cluster) throws FalconException {

        for (WorkflowEngineActionListener listener : listeners) {
            switch (action) {
            case SUSPEND:
                listener.beforeSuspend(entity, cluster);
                break;

            case RESUME:
                listener.beforeResume(entity, cluster);
                break;

            case KILL:
                listener.beforeDelete(entity, cluster);
                break;
            default:
            }
        }
    }

    private void afterAction(Entity entity, BundleAction action, String cluster) throws FalconException {

        for (WorkflowEngineActionListener listener : listeners) {
            switch (action) {
            case SUSPEND:
                listener.afterSuspend(entity, cluster);
                break;

            case RESUME:
                listener.afterResume(entity, cluster);
                break;

            case KILL:
                listener.afterDelete(entity, cluster);
                break;
            default:
            }
        }
    }

    @Override
    public InstancesResult getRunningInstances(Entity entity, List<LifeCycle> lifeCycles) throws FalconException {
        try {
            Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
            List<Instance> runInstances = new ArrayList<Instance>();

            for (String cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<String> wfNames = EntityUtil.getWorkflowNames(entity);
                List<WorkflowJob> wfs = getRunningWorkflows(cluster, wfNames);
                if (wfs != null) {
                    for (WorkflowJob job : wfs) {
                        WorkflowJob wf = client.getJobInfo(job.getId());
                        if (StringUtils.isEmpty(wf.getParentId())) {
                            continue;
                        }

                        CoordinatorAction action = client.getCoordActionInfo(wf.getParentId());
                        String nominalTimeStr = SchemaHelper.formatDateUTC(action.getNominalTime());
                        Instance instance = new Instance(cluster, nominalTimeStr, WorkflowStatus.RUNNING);
                        instance.startTime = wf.getStartTime();
                        if (entity.getEntityType() == EntityType.FEED) {
                            instance.sourceCluster = getSourceCluster(cluster, action, entity);
                        }
                        runInstances.add(instance);
                    }
                }
            }
            InstancesResult result = new InstancesResult(APIResult.Status.SUCCEEDED, "Running Instances");
            result.setInstances(runInstances.toArray(new Instance[runInstances.size()]));
            return result;

        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public InstancesResult killInstances(Entity entity, Date start, Date end,
                                         Properties props, List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(JobAction.KILL, entity, start, end, props, lifeCycles);
    }

    @Override
    public InstancesResult reRunInstances(Entity entity, Date start, Date end,
                                          Properties props, List<LifeCycle> lifeCycles,
                                          Boolean isForced) throws FalconException {
        if (isForced == null) {
            isForced = false;
        }
        return doJobAction(JobAction.RERUN, entity, start, end, props, lifeCycles, false, isForced);
    }

    @Override
    public InstancesResult suspendInstances(Entity entity, Date start, Date end,
                                            Properties props, List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(JobAction.SUSPEND, entity, start, end, props, lifeCycles);
    }

    @Override
    public InstancesResult resumeInstances(Entity entity, Date start, Date end,
                                           Properties props, List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(JobAction.RESUME, entity, start, end, props, lifeCycles);
    }

    @Override
    public InstancesResult getStatus(Entity entity, Date start, Date end,
                                     List<LifeCycle> lifeCycles, Boolean allAttempts) throws FalconException {

        return doJobAction(JobAction.STATUS, entity, start, end, null, lifeCycles, allAttempts);
    }

    @Override
    public InstancesSummaryResult getSummary(Entity entity, Date start, Date end,
                                             List<LifeCycle> lifeCycles) throws FalconException {

        return doSummaryJobAction(entity, start, end, null, lifeCycles);
    }

    @Override
    public InstancesResult getInstanceParams(Entity entity, Date start, Date end,
                                             List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(JobAction.PARAMS, entity, start, end, null, lifeCycles);
    }

    @Override
    public boolean isNotificationEnabled(String cluster, String jobID) throws FalconException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            JMSConnectionInfo jmsConnection = client.getJMSConnectionInfo();
            if (jmsConnection != null && !jmsConnection.getJNDIProperties().isEmpty()){
                String falconTopic  = StartupProperties.get().getProperty("entity.topic", "FALCON.ENTITY.TOPIC");
                String oozieTopic = client.getJMSTopicName(jobID);
                if (falconTopic.equals(oozieTopic)) {
                    return true;
                }
            }
        } catch (OozieClientException e) {
            LOG.debug("Error while retrieving JMS connection info", e);
        }

        return false;
    }

    private static enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS, SUMMARY, PARAMS
    }

    private WorkflowJob getWorkflowInfo(String cluster, String wfId) throws FalconException {
        try {
            return OozieClientFactory.get(cluster).getJobInfo(wfId);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private List<WorkflowJob> getWfsForCoordAction(String cluster, String coordActionId) throws FalconException {
        try {
            return OozieClientFactory.get(cluster).getWfsForCoordAction(coordActionId);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private InstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end,
                                        Properties props, List<LifeCycle> lifeCycles) throws FalconException {
        if (StartupProperties.isServerInSafeMode()
                && (action.equals(JobAction.RERUN) || action.equals(JobAction.RESUME))) {
            throwSafemodeException(action.name());
        }
        return doJobAction(action, entity, start, end, props, lifeCycles, null);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    private InstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end,
                                        Properties props, List<LifeCycle> lifeCycles,
                                        Boolean allAttempts, boolean isForced) throws FalconException {
        Map<String, List<CoordinatorAction>> actionsMap = getCoordActions(entity, start, end, lifeCycles);
        List<String> clusterList = getIncludedClusters(props, FALCON_INSTANCE_ACTION_CLUSTERS);
        List<String> sourceClusterList = getIncludedClusters(props, FALCON_INSTANCE_SOURCE_CLUSTERS);
        APIResult.Status overallStatus = APIResult.Status.SUCCEEDED;
        int instanceCount = 0;

        List<Instance> instances = new ArrayList<Instance>();
        for (Map.Entry<String, List<CoordinatorAction>> entry : actionsMap.entrySet()) {
            String cluster = entry.getKey();
            if (clusterList.size() != 0 && !clusterList.contains(cluster)) {
                continue;
            }

            List<CoordinatorAction> actions = entry.getValue();
            String sourceCluster = null;
            for (CoordinatorAction coordinatorAction : actions) {
                if (entity.getEntityType() == EntityType.FEED) {
                    sourceCluster = getSourceCluster(cluster, coordinatorAction, entity);
                    if (sourceClusterList.size() != 0 && !sourceClusterList.contains(sourceCluster)) {
                        continue;
                    }
                }
                instanceCount++;
                String nominalTimeStr = SchemaHelper.formatDateUTC(coordinatorAction.getNominalTime());
                List<InstancesResult.Instance> instanceList = new ArrayList<>();
                InstancesResult.Instance instance =
                        new InstancesResult.Instance(cluster, nominalTimeStr, null);
                instance.sourceCluster = sourceCluster;
                if (action.equals(JobAction.STATUS) && Boolean.TRUE.equals(allAttempts)) {
                    try {
                        performAction(cluster, action, coordinatorAction, props, instance, isForced);
                        instanceList = getAllInstances(cluster, coordinatorAction, nominalTimeStr);
                        // Happens when the action is in READY/WAITING, when no workflow is kicked off yet.
                        if (instanceList.isEmpty() || StringUtils.isBlank(coordinatorAction.getExternalId())) {
                            instanceList.add(instance);
                        }
                    } catch (FalconException e) {
                        LOG.warn("Unable to perform action {} on cluster", action, e);
                        instance.status = WorkflowStatus.ERROR;
                        overallStatus = APIResult.Status.PARTIAL;
                    }
                    for (InstancesResult.Instance instanceResult : instanceList) {
                        instanceResult.details = coordinatorAction.getMissingDependencies();
                        instanceResult.sourceCluster = sourceCluster;
                        instances.add(instanceResult);
                    }
                } else {
                    try {
                        performAction(cluster, action, coordinatorAction, props, instance, isForced);
                    } catch (FalconException e) {
                        LOG.warn("Unable to perform action {} on cluster", action, e);
                        instance.status = WorkflowStatus.ERROR;
                        overallStatus = APIResult.Status.PARTIAL;
                    }
                    instance.details = coordinatorAction.getMissingDependencies();
                    instances.add(instance);
                }
            }
        }
        if (instanceCount < 2 && overallStatus == APIResult.Status.PARTIAL) {
            overallStatus = APIResult.Status.FAILED;
        }
        InstancesResult instancesResult = new InstancesResult(overallStatus, action.name());
        instancesResult.setInstances(instances.toArray(new Instance[instances.size()]));
        return instancesResult;
    }

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private InstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end, Properties props,
                                        List<LifeCycle> lifeCycles, Boolean allAttempts) throws FalconException {
        return doJobAction(action, entity, start, end, props, lifeCycles, allAttempts, false);
    }

    private InstancesSummaryResult doSummaryJobAction(Entity entity, Date start,
                                                      Date end, Properties props,
                                                      List<LifeCycle> lifeCycles) throws FalconException {

        Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
        List<InstanceSummary> instances = new ArrayList<InstanceSummary>();
        List<String> clusterList = getIncludedClusters(props, FALCON_INSTANCE_ACTION_CLUSTERS);

        for (Map.Entry<String, List<BundleJob>> entry : bundlesMap.entrySet()) {
            Map<String, Long> instancesSummary = new HashMap<String, Long>();
            String cluster = entry.getKey();
            if (clusterList.size() != 0 && !clusterList.contains(cluster)) {
                continue;
            }

            List<BundleJob> bundles = entry.getValue();
            OozieClient client = OozieClientFactory.get(cluster);
            List<CoordinatorJob> applicableCoords = getApplicableCoords(client, start, end,
                    bundles, lifeCycles);
            long unscheduledInstances = 0;

            for (int i = 0; i < applicableCoords.size(); i++) {
                boolean isLastCoord = false;
                CoordinatorJob coord = applicableCoords.get(i);
                Frequency freq = createFrequency(String.valueOf(coord.getFrequency()), coord.getTimeUnit());
                TimeZone tz = EntityUtil.getTimeZone(coord.getTimeZone());
                Date iterStart = EntityUtil.getNextStartTime(coord.getStartTime(), freq, tz, start);
                Date iterEnd = (coord.getLastActionTime() != null && coord.getLastActionTime().before(end)
                    ? coord.getLastActionTime() : end);

                if (i == 0) {
                    isLastCoord = true;
                }

                int startActionNumber = EntityUtil.getInstanceSequence(coord.getStartTime(), freq, tz, iterStart);
                int lastMaterializedActionNumber =
                    EntityUtil.getInstanceSequence(coord.getStartTime(), freq, tz, iterEnd);
                int endActionNumber = EntityUtil.getInstanceSequence(coord.getStartTime(), freq, tz, end);

                if (lastMaterializedActionNumber < startActionNumber) {
                    continue;
                }

                if (isLastCoord && endActionNumber != lastMaterializedActionNumber) {
                    unscheduledInstances = endActionNumber - lastMaterializedActionNumber;
                }

                CoordinatorJob coordJob;
                try {
                    coordJob = client.getCoordJobInfo(coord.getId(), null, startActionNumber,
                        (lastMaterializedActionNumber - startActionNumber));
                } catch (OozieClientException e) {
                    LOG.debug("Unable to get details for coordinator {}", coord.getId(), e);
                    throw new FalconException(e);
                }

                if (coordJob != null) {
                    updateInstanceSummary(coordJob, instancesSummary);
                }
            }

            if (unscheduledInstances > 0) {
                instancesSummary.put("UNSCHEDULED", unscheduledInstances);
            }

            InstanceSummary summary = new InstanceSummary(cluster, instancesSummary);
            instances.add(summary);
        }

        InstancesSummaryResult instancesSummaryResult =
            new InstancesSummaryResult(APIResult.Status.SUCCEEDED, JobAction.SUMMARY.name());
        instancesSummaryResult.setInstancesSummary(instances.toArray(new InstanceSummary[instances.size()]));
        return instancesSummaryResult;
    }

    private void populateInstanceActions(String cluster, WorkflowJob wfJob, Instance instance)
        throws FalconException {

        List<InstancesResult.InstanceAction> instanceActions = new ArrayList<InstancesResult.InstanceAction>();

        List<WorkflowAction> wfActions = wfJob.getActions();

        // We wanna capture job urls for all user-actions & non succeeded actions of the main workflow
        for (WorkflowAction action : wfActions) {
            if (action.getType().equalsIgnoreCase("sub-workflow") && StringUtils.isNotEmpty(action.getExternalId())) {
                // if the action is sub-workflow, get job urls of all actions within the sub-workflow
                List<WorkflowAction> subWorkFlowActions = getWorkflowInfo(cluster,
                        action.getExternalId()).getActions();
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
                        && !Status.SUCCEEDED.toString().equals(action.getExternalStatus())) {
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
        instance.actions = instanceActions.toArray(new InstancesResult.InstanceAction[instanceActions.size()]);
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

    private void updateInstanceSummary(CoordinatorJob coordJob, Map<String, Long> instancesSummary) {
        List<CoordinatorAction> actions = coordJob.getActions();

        for (CoordinatorAction coordAction : actions) {
            if (instancesSummary.containsKey(coordAction.getStatus().name())) {
                instancesSummary.put(coordAction.getStatus().name(),
                    instancesSummary.get(coordAction.getStatus().name()) + 1L);
            } else {
                instancesSummary.put(coordAction.getStatus().name(), 1L);
            }
        }
    }

    private List<InstancesResult.Instance> getAllInstances(String cluster, CoordinatorAction coordinatorAction,
                                                           String nominalTimeStr) throws FalconException {
        List<InstancesResult.Instance> instanceList = new ArrayList<>();
        if (StringUtils.isNotBlank(coordinatorAction.getId())) {
            List<WorkflowJob> workflowJobList = getWfsForCoordAction(cluster, coordinatorAction.getId());
            if (workflowJobList != null && workflowJobList.size()>0) {
                for (WorkflowJob workflowJob : workflowJobList) {
                    InstancesResult.Instance newInstance = new InstancesResult.Instance(cluster, nominalTimeStr, null);
                    WorkflowJob wfJob = getWorkflowInfo(cluster, workflowJob.getId());
                    if (wfJob!=null) {
                        newInstance.startTime = wfJob.getStartTime();
                        newInstance.endTime = wfJob.getEndTime();
                        newInstance.logFile = wfJob.getConsoleUrl();
                        populateInstanceActions(cluster, wfJob, newInstance);
                        newInstance.status = WorkflowStatus.valueOf(mapActionStatus(wfJob.getStatus().name()));
                        instanceList.add(newInstance);
                    }
                }
            }
        }
        return instanceList;
    }

    private void performAction(String cluster, JobAction action, CoordinatorAction coordinatorAction,
        Properties props, InstancesResult.Instance instance, boolean isForced) throws FalconException {
        WorkflowJob jobInfo = null;
        String status = coordinatorAction.getStatus().name();
        if (StringUtils.isNotEmpty(coordinatorAction.getExternalId())) {
            jobInfo = getWorkflowInfo(cluster, coordinatorAction.getExternalId());
            status = jobInfo.getStatus().name();
            instance.startTime = jobInfo.getStartTime();
            instance.endTime = jobInfo.getEndTime();
            instance.logFile = jobInfo.getConsoleUrl();
            instance.runId = jobInfo.getRun();
        }

        switch (action) {
        case KILL:
            if (jobInfo == null) {
                StringBuilder scope = new StringBuilder();
                scope.append(SchemaHelper.formatDateUTC(coordinatorAction.getNominalTime())).append("::")
                        .append(SchemaHelper.formatDateUTC(coordinatorAction.getNominalTime()));
                kill(cluster, coordinatorAction.getJobId(), "date", scope.toString());
                status = Status.KILLED.name();
                break;
            }
            if (!WF_KILL_PRECOND.contains(jobInfo.getStatus())) {
                break;
            }


            kill(cluster, jobInfo.getId());
            status = Status.KILLED.name();
            break;

        case SUSPEND:
            if (jobInfo == null || !WF_SUSPEND_PRECOND.contains(jobInfo.getStatus())) {
                break;
            }

            suspend(cluster, jobInfo.getId());
            status = Status.SUSPENDED.name();
            break;

        case RESUME:
            if (jobInfo == null || !WF_RESUME_PRECOND.contains(jobInfo.getStatus())) {
                break;
            }

            resume(cluster, jobInfo.getId());
            status = Status.RUNNING.name();
            break;

        case RERUN:
            if (COORD_RERUN_PRECOND.contains(coordinatorAction.getStatus())) {
                status = reRunCoordAction(cluster, coordinatorAction, props, isForced).name();
            }
            break;

        case STATUS:
            if (StringUtils.isNotEmpty(coordinatorAction.getExternalId())) {
                populateInstanceActions(cluster, jobInfo, instance);
            }
            break;

        case PARAMS:
            if (StringUtils.isNotEmpty(coordinatorAction.getExternalId())) {
                instance.wfParams = getWFParams(jobInfo);
            }
            break;

        default:
            throw new IllegalArgumentException("Unhandled action " + action);
        }

        // status can be CoordinatorAction.Status, WorkflowJob.Status or Job.Status
        try {
            instance.status = WorkflowStatus.valueOf(mapActionStatus(status));
        } catch (IllegalArgumentException e) {
            LOG.error("Job status not defined in Instance status: {}", status);
            instance.status = WorkflowStatus.UNDEFINED;
        }
    }

    public CoordinatorAction.Status reRunCoordAction(String cluster, CoordinatorAction coordinatorAction,
                                                      Properties props, boolean isForced) throws FalconException {
        try {
            OozieClient client = OozieClientFactory.get(cluster);
            if (props == null) {
                props = new Properties();
            }
            // In case if both props exists, throw an exception.
            // This case will occur when user runs workflow with skip-nodes property and
            // try to do force rerun or rerun with fail-nodes property.
            if (props.containsKey(OozieClient.RERUN_FAIL_NODES)
                    && props.containsKey(OozieClient.RERUN_SKIP_NODES)) {
                String msg = "Both " + OozieClient.RERUN_SKIP_NODES + " and " + OozieClient.RERUN_FAIL_NODES
                        + " are present in workflow params for " + coordinatorAction.getExternalId();
                LOG.error(msg);
                throw new FalconException(msg);
            }

            //if user has set any of these oozie rerun properties then force rerun flag is ignored
            if (props.containsKey(OozieClient.RERUN_FAIL_NODES)) {
                isForced = false;
            }
            Properties jobprops;
            // Get conf when workflow is launched.
            if (coordinatorAction.getExternalId() != null) {
                WorkflowJob jobInfo = client.getJobInfo(coordinatorAction.getExternalId());

                jobprops = OozieUtils.toProperties(jobInfo.getConf());
                // Clear the rerun properties from existing configuration
                jobprops.remove(OozieClient.RERUN_FAIL_NODES);
                jobprops.remove(OozieClient.RERUN_SKIP_NODES);
                jobprops.putAll(props);
                jobprops.remove(OozieClient.BUNDLE_APP_PATH);
            } else {
                jobprops = props;
            }

            client.reRunCoord(coordinatorAction.getJobId(), RestConstants.JOB_COORD_SCOPE_ACTION,
                    Integer.toString(coordinatorAction.getActionNumber()), true, true, !isForced, jobprops);
            LOG.info("Rerun job {} on cluster {}", coordinatorAction.getId(), cluster);
            return assertCoordActionStatus(cluster, coordinatorAction.getId(),
                    org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                    org.apache.oozie.client.CoordinatorAction.Status.WAITING,
                    org.apache.oozie.client.CoordinatorAction.Status.READY);
        } catch (Exception e) {
            LOG.error("Unable to rerun workflows", e);
            throw new FalconException(e);
        }
    }

    private CoordinatorAction.Status assertCoordActionStatus(String cluster, String coordActionId,
        org.apache.oozie.client.CoordinatorAction.Status... statuses) throws FalconException, OozieClientException {
        OozieClient client = OozieClientFactory.get(cluster);
        CoordinatorAction actualStatus = client.getCoordActionInfo(coordActionId);
        for (int counter = 0; counter < 3; counter++) {
            for (org.apache.oozie.client.CoordinatorAction.Status status : statuses) {
                if (status.equals(actualStatus.getStatus())) {
                    return status;
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            actualStatus = client.getCoordActionInfo(coordActionId);
        }
        throw new FalconException("For Job" + coordActionId + ", actual statuses: " + actualStatus + ", "
            + "expected statuses: " + Arrays.toString(statuses));
    }

    private String getSourceCluster(String cluster, CoordinatorAction coordinatorAction, Entity entity)
        throws FalconException {
        try {
            CoordinatorJob coordJob = OozieClientFactory.get(cluster).getCoordJobInfo(coordinatorAction.getJobId());
            return EntityUtil.getWorkflowNameSuffix(coordJob.getAppName(), entity);
        } catch (OozieClientException e) {
            throw new FalconException("Unable to get oozie job id:" + e);
        }
    }

    private List<String> getIncludedClusters(Properties props, String clustersType) {
        String clusters = props == null ? "" : props.getProperty(clustersType, "");
        List<String> clusterList = new ArrayList<String>();
        for (String cluster : clusters.split(",")) {
            if (StringUtils.isNotEmpty(cluster)) {
                clusterList.add(cluster.trim());
            }
        }
        return clusterList;
    }

    private String mapActionStatus(String status) {
        if (CoordinatorAction.Status.READY.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.READY.name();
        } else if (CoordinatorAction.Status.WAITING.toString().equals(status)
            || CoordinatorAction.Status.SUBMITTED.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.WAITING.name();
        } else if (CoordinatorAction.Status.IGNORED.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.KILLED.name();
        } else if (CoordinatorAction.Status.TIMEDOUT.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.FAILED.name();
        } else if (WorkflowJob.Status.PREP.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.RUNNING.name();
        } else {
            return status;
        }
    }

    @SuppressWarnings("MagicConstant")
    protected Map<String, List<CoordinatorAction>> getCoordActions(Entity entity, Date start, Date end,
                                                                   List<LifeCycle> lifeCycles) throws FalconException {
        Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
        Map<String, List<CoordinatorAction>> actionsMap = new HashMap<String, List<CoordinatorAction>>();

        for (Map.Entry<String, List<BundleJob>> entry : bundlesMap.entrySet()) {
            String cluster = entry.getKey();
            List<BundleJob> bundles = entry.getValue();
            OozieClient client = OozieClientFactory.get(cluster);
            List<CoordinatorJob> applicableCoords =
                getApplicableCoords(client, start, end, bundles, lifeCycles);
            List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();
            int maxRetentionInstancesCount =
                Integer.parseInt(RuntimeProperties.get().getProperty("retention.instances.displaycount", "2"));
            int retentionInstancesCount = 0;

            for (CoordinatorJob coord : applicableCoords) {
                Date nextMaterializedTime = coord.getNextMaterializedTime();
                if (nextMaterializedTime == null) {
                    continue;
                }

                boolean retentionCoord  = isRetentionCoord(coord);
                Frequency freq = createFrequency(String.valueOf(coord.getFrequency()), coord.getTimeUnit());
                TimeZone tz = EntityUtil.getTimeZone(coord.getTimeZone());

                Date iterEnd = ((nextMaterializedTime.before(end) || retentionCoord) ? nextMaterializedTime : end);
                Calendar endCal = Calendar.getInstance(EntityUtil.getTimeZone(coord.getTimeZone()));
                endCal.setTime(EntityUtil.getNextStartTime(coord.getStartTime(), freq, tz, iterEnd));
                endCal.add(freq.getTimeUnit().getCalendarUnit(), -(Integer.parseInt((coord.getFrequency()))));

                while (start.compareTo(endCal.getTime()) <= 0) {
                    if (retentionCoord) {
                        if (retentionInstancesCount >= maxRetentionInstancesCount) {
                            break;
                        }
                        retentionInstancesCount++;
                    }

                    int sequence = EntityUtil.getInstanceSequence(coord.getStartTime(), freq, tz, endCal.getTime());
                    String actionId = coord.getId() + "@" + sequence;
                    addCoordAction(client, actions, actionId);
                    endCal.add(freq.getTimeUnit().getCalendarUnit(), -(Integer.parseInt((coord.getFrequency()))));
                }
            }
            actionsMap.put(cluster, actions);
        }
        return actionsMap;
    }

    private boolean isRetentionCoord(CoordinatorJob coord){
        return coord.getAppName().contains(LifeCycle.EVICTION.getTag().name());
    }

    private void addCoordAction(OozieClient client, List<CoordinatorAction> actions, String actionId) {
        CoordinatorAction coordActionInfo = null;
        try {
            coordActionInfo = client.getCoordActionInfo(actionId);
        } catch (OozieClientException e) {
            LOG.debug("Unable to get action for " + actionId + " " + e.getMessage());
        }
        if (coordActionInfo != null) {
            actions.add(coordActionInfo);
        }
    }

    private Frequency createFrequency(String frequency, Timeunit timeUnit) {
        return new Frequency(frequency, OozieTimeUnit.valueOf(timeUnit.name()).getFalconTimeUnit());
    }

    /**
     * TimeUnit as understood by Oozie.
     */
    private enum OozieTimeUnit {
        MINUTE(TimeUnit.minutes), HOUR(TimeUnit.hours), DAY(TimeUnit.days), WEEK(null), MONTH(TimeUnit.months),
        END_OF_DAY(null), END_OF_MONTH(null), NONE(null);

        private TimeUnit falconTimeUnit;

        private OozieTimeUnit(TimeUnit falconTimeUnit) {
            this.falconTimeUnit = falconTimeUnit;
        }

        public TimeUnit getFalconTimeUnit() {
            if (falconTimeUnit == null) {
                throw new IllegalStateException("Invalid coord frequency: " + name());
            }
            return falconTimeUnit;
        }
    }

    private List<CoordinatorJob> getApplicableCoords(OozieClient client, Date start, Date end,
                                                     List<BundleJob> bundles,
                                                     List<LifeCycle> lifeCycles) throws FalconException {
        List<CoordinatorJob> applicableCoords = new ArrayList<CoordinatorJob>();
        try {
            for (BundleJob bundle : bundles) {
                List<CoordinatorJob> coords = client.getBundleJobInfo(bundle.getId()).getCoordinators();
                for (CoordinatorJob coord : coords) {
                    // ignore coords in PREP state, not yet running and retention coord

                    if (coord.getStatus() == Status.PREP
                            || !isCoordApplicable(coord.getAppName(), lifeCycles)) {
                        continue;
                    }

                    // if end time is before coord-start time or start time is
                    // after coord-end time ignore.
                    if (!(end.compareTo(coord.getStartTime()) <= 0 || start.compareTo(coord.getEndTime()) >= 0)) {
                        applicableCoords.add(coord);
                    }
                }
            }

            sortDescByStartTime(applicableCoords);
            return applicableCoords;
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private boolean isCoordApplicable(String appName, List<LifeCycle> lifeCycles) {
        if (lifeCycles != null && !lifeCycles.isEmpty()) {
            for (LifeCycle lifeCycle : lifeCycles) {
                if (appName.contains(lifeCycle.getTag().name())) {
                    return true;
                }
            }
        }
        return false;
    }

    protected void sortDescByStartTime(List<CoordinatorJob> consideredCoords) {
        Collections.sort(consideredCoords, new Comparator<CoordinatorJob>() {
            @Override
            public int compare(CoordinatorJob left, CoordinatorJob right) {
                Date leftStart = left.getStartTime();
                Date rightStart = right.getStartTime();
                return rightStart.compareTo(leftStart);
            }
        });
    }


    private boolean canUpdateBundle(Entity oldEntity, Entity newEntity) throws FalconException {
        return EntityUtil.equals(oldEntity, newEntity, BUNDLE_UPDATEABLE_PROPS);
    }

    @Override
    public String update(Entity oldEntity, Entity newEntity,
                         String cluster, Boolean skipDryRun) throws FalconException {
        BundleJob bundle = findLatestBundle(oldEntity, cluster);

        boolean entityUpdated = false;
        if (bundle != MISSING) {
            entityUpdated = UpdateHelper.isEntityUpdated(oldEntity, newEntity, cluster, new Path(bundle.getAppPath()));
        }

        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        StringBuilder result = new StringBuilder();
        //entity is scheduled before and entity is updated
        if (bundle != MISSING && entityUpdated) {
            LOG.info("Updating entity through Workflow Engine {}", newEntity.toShortString());
            Date newEndTime = EntityUtil.getEndTime(newEntity, cluster);
            if (newEndTime.before(DateUtil.now())) {
                throw new FalconException("Entity's end time " + SchemaHelper.formatDateUTC(newEndTime)
                    + " is before current time. Entity can't be updated. Use remove and add");
            }

            LOG.debug("Updating for cluster: {}, bundle: {}", cluster, bundle.getId());

            if (canUpdateBundle(oldEntity, newEntity)) {
                // only concurrency and endtime are changed. So, change coords
                LOG.info("Change operation is adequate! : {}, bundle: {}", cluster, bundle.getId());
                updateCoords(cluster, bundle, EntityUtil.getParallel(newEntity),
                    EntityUtil.getEndTime(newEntity, cluster), newEntity);
                return getUpdateString(newEntity, new Date(), bundle, bundle);
            }

            LOG.debug("Going to update! : {} for cluster {}, bundle: {}",
                    newEntity.toShortString(), cluster, bundle.getId());
            result.append(updateInternal(oldEntity, newEntity, clusterEntity, bundle,
                    bundle.getUser(), skipDryRun)).append("\n");
            LOG.info("Entity update complete: {} for cluster {}, bundle: {}", newEntity.toShortString(), cluster,
                bundle.getId());
        }

        result.append(updateDependents(clusterEntity, oldEntity, newEntity, skipDryRun));
        return result.toString();
    }

    @Override
    public String touch(Entity entity, String cluster, Boolean skipDryRun) throws FalconException {
        BundleJob bundle = findLatestBundle(entity, cluster);
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        StringBuilder result = new StringBuilder();
        if (bundle != MISSING) {
            LOG.info("Updating entity {} for cluster: {}, bundle: {}",
                    entity.toShortString(), cluster, bundle.getId());
            String output = updateInternal(entity, entity, clusterEntity, bundle, CurrentUser.getUser(), skipDryRun);
            result.append(output).append("\n");
            LOG.info("Entity update complete: {} for cluster {}, bundle: {}", entity.toShortString(), cluster,
                    bundle.getId());
        }
        return result.toString();
    }

    private String getUpdateString(Entity entity, Date date, BundleJob oldBundle, BundleJob newBundle) {
        StringBuilder builder = new StringBuilder();
        builder.append(entity.toShortString()).append("/Effective Time: ").append(SchemaHelper.formatDateUTC(date));
        if (StringUtils.isNotEmpty(oldBundle.getId())) {
            builder.append(". Old bundle id: " + oldBundle.getId());
        }
        builder.append(". Old coordinator id: ");
        List<String> coords = new ArrayList<String>();
        for (CoordinatorJob coord : oldBundle.getCoordinators()) {
            coords.add(coord.getId());
        }
        builder.append(StringUtils.join(coords, ','));

        if (newBundle != null) {
            coords.clear();
            for (CoordinatorJob coord : newBundle.getCoordinators()) {
                coords.add(coord.getId());
            }
            if (coords.isEmpty()) {
                builder.append(". New bundle id: ");
                builder.append(newBundle.getId());
            } else {
                builder.append(". New coordinator id: ");
                builder.append(StringUtils.join(coords, ','));
            }
        }
        return  builder.toString();
    }

    private String updateDependents(Cluster cluster, Entity oldEntity,
                                    Entity newEntity, Boolean skipDryRun) throws FalconException {
        //Update affected entities
        Set<Entity> affectedEntities = EntityGraph.get().getDependents(oldEntity);
        StringBuilder result = new StringBuilder();
        for (Entity affectedEntity : affectedEntities) {
            if (affectedEntity.getEntityType() != EntityType.PROCESS) {
                continue;
            }

            LOG.info("Dependent entity {} need to be updated", affectedEntity.toShortString());
            if (!UpdateHelper.shouldUpdate(oldEntity, newEntity, affectedEntity, cluster.getName())) {
                continue;
            }

            BundleJob affectedProcBundle = findLatestBundle(affectedEntity, cluster.getName());
            if (affectedProcBundle == MISSING) {
                LOG.info("Dependent entity {} is not scheduled", affectedEntity.getName());
                continue;
            }

            LOG.info("Triggering update for {}, {}", cluster, affectedProcBundle.getId());

            result.append(updateInternal(affectedEntity, affectedEntity, cluster, affectedProcBundle,
                affectedProcBundle.getUser(), skipDryRun)).append("\n");
            LOG.info("Entity update complete: {} for cluster {}, bundle: {}",
                affectedEntity.toShortString(), cluster, affectedProcBundle.getId());
        }
        LOG.info("All dependent entities updated for: {}", oldEntity.toShortString());
        return result.toString();
    }

    @SuppressWarnings("MagicConstant")
    private Date getCoordLastActionTime(CoordinatorJob coord) {
        if (coord.getNextMaterializedTime() != null) {
            Calendar cal = Calendar.getInstance(EntityUtil.getTimeZone(coord.getTimeZone()));
            cal.setTime(coord.getLastActionTime());
            Frequency freq = createFrequency(String.valueOf(coord.getFrequency()), coord.getTimeUnit());
            cal.add(freq.getTimeUnit().getCalendarUnit(), -freq.getFrequencyAsInt());
            return cal.getTime();
        }
        return null;
    }

    private void updateCoords(String cluster, BundleJob bundle,
                              int concurrency, Date endTime, Entity entity) throws FalconException {
        if (endTime.compareTo(DateUtil.now()) <= 0) {
            throw new FalconException("End time " + SchemaHelper.formatDateUTC(endTime) + " can't be in the past");
        }

        if (bundle.getCoordinators() == null || bundle.getCoordinators().isEmpty()) {
            throw new FalconException("Invalid state. Oozie coords are still not created. Try again later");
        }

        // change coords
        for (CoordinatorJob coord : bundle.getCoordinators()) {

            Frequency delay = null;
            //get Delay to calculate coordinator end time in case of feed replication with delay.
            if (entity.getEntityType().equals(EntityType.FEED)) {
                delay = getDelay((Feed) entity, coord);
            }

            //calculate next start time based on delay.
            endTime = (delay == null) ? endTime
                    : EntityUtil.getNextInstanceTimeWithDelay(endTime, delay, EntityUtil.getTimeZone(entity));
            LOG.debug("Updating endtime of coord {} to {} on cluster {}",
                    coord.getId(), SchemaHelper.formatDateUTC(endTime), cluster);

            Date lastActionTime = getCoordLastActionTime(coord);
            if (lastActionTime == null) { // nothing is materialized
                LOG.info("Nothing is materialized for this coord: {}", coord.getId());
                if (endTime.compareTo(coord.getStartTime()) <= 0) {
                    LOG.info("Setting end time to START TIME {}", SchemaHelper.formatDateUTC(coord.getStartTime()));
                    change(cluster, coord.getId(), concurrency, coord.getStartTime(), null);
                } else {
                    LOG.info("Setting end time to START TIME {}", SchemaHelper.formatDateUTC(endTime));
                    change(cluster, coord.getId(), concurrency, endTime, null);
                }
            } else {
                LOG.info("Actions have materialized for this coord: {}, last action {}",
                        coord.getId(), SchemaHelper.formatDateUTC(lastActionTime));
                if (!endTime.after(lastActionTime)) {
                    Date pauseTime = DateUtil.offsetTime(endTime, -1*60);
                    // set pause time which deletes future actions
                    LOG.info("Setting pause time on coord: {} to {}",
                            coord.getId(), SchemaHelper.formatDateUTC(pauseTime));
                    change(cluster, coord.getId(), concurrency, null, SchemaHelper.formatDateUTC(pauseTime));
                }
                change(cluster, coord.getId(), concurrency, endTime, null);
            }
        }
    }

    private Frequency getDelay(Feed entity, CoordinatorJob coord) {
        Feed feed = entity;
        for (org.apache.falcon.entity.v0.feed.Cluster entityCluster : feed.getClusters().getClusters()){
            if (coord.getAppName().contains(entityCluster.getName()) && coord.getAppName().contains("REPLICATION")
                    && entityCluster.getDelay() != null){
                return entityCluster.getDelay();
            }
        }
        return null;
    }

    private String updateInternal(Entity oldEntity, Entity newEntity, Cluster cluster, BundleJob oldBundle,
                                  String user, Boolean skipDryRun) throws FalconException {
        String currentUser = CurrentUser.getUser();
        switchUser(user);

        String clusterName = cluster.getName();

        Date effectiveTime = getEffectiveTime(cluster, newEntity);
        LOG.info("Effective time " + effectiveTime);
        try {
            //Validate that new entity can be scheduled
            dryRunForUpdate(cluster, newEntity, effectiveTime, skipDryRun);

            boolean suspended = BUNDLE_SUSPENDED_STATUS.contains(oldBundle.getStatus());

            //Set end times for old coords
            updateCoords(clusterName, oldBundle, EntityUtil.getParallel(oldEntity), effectiveTime, newEntity);
            //schedule new entity
            String newJobId = scheduleForUpdate(newEntity, cluster, effectiveTime);
            BundleJob newBundle = null;
            if (newJobId != null) {
                newBundle = getBundleInfo(clusterName, newJobId);
            }

            //Sometimes updateCoords() resumes the suspended coords. So, if already suspended, resume now
            //Also suspend new bundle
            if (suspended) {
                doBundleAction(newEntity, BundleAction.SUSPEND, cluster.getName());
            }
            return getUpdateString(newEntity, effectiveTime, oldBundle, newBundle);
        } finally {
            // Switch back to current user in case of exception.
            switchUser(currentUser);
        }
    }

    private Date getEffectiveTime(Cluster cluster, Entity newEntity) {
        //pick effective time as now() + 3 min to handle any time diff between falcon and oozie
        //oozie rejects changes with endtime < now
        Date effectiveTime = DateUtil.offsetTime(DateUtil.now(), 3*60);

        //pick start time for new bundle which is after effectiveTime
        return EntityUtil.getNextStartTime(newEntity, cluster, effectiveTime);
    }

    private void dryRunForUpdate(Cluster cluster, Entity entity, Date startTime,
                                 Boolean skipDryRun) throws FalconException {
        Entity clone = entity.copy();
        EntityUtil.setStartDate(clone, cluster.getName(), startTime);
        try {
            dryRun(clone, cluster.getName(), skipDryRun);
        } catch (FalconException e) {
            throw new FalconException("The new entity " + entity.toShortString() + " can't be scheduled", e);
        }
    }

    private String scheduleForUpdate(Entity entity, Cluster cluster, Date startDate) throws FalconException {
        Entity clone = entity.copy();
        EntityUtil.setStartDate(clone, cluster.getName(), startDate);
        Path buildPath = EntityUtil.getNewStagingPath(cluster, clone);
        OozieEntityBuilder builder = OozieEntityBuilder.get(clone);
        Properties properties = builder.build(cluster, buildPath);
        if (properties != null) {
            LOG.info("Scheduling {} on cluster {} with props {}", entity.toShortString(), cluster.getName(),
                    properties);
            return scheduleEntity(cluster.getName(), properties, entity);
        } else {
            LOG.info("No new workflow to be scheduled for this " + entity.toShortString());
            return null;
        }
    }

    private void switchUser(String user) {
        if (!CurrentUser.getUser().equals(user)) {
            CurrentUser.authenticate(user);
        }
    }

    private BundleJob getBundleInfo(String cluster, String bundleId) throws FalconException {
        try {
            return OozieClientFactory.get(cluster).getBundleJobInfo(bundleId);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private List<WorkflowJob> getRunningWorkflows(String cluster, List<String> wfNames) throws
        FalconException {
        StringBuilder filter = new StringBuilder();
        filter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.RUNNING.name());
        for (String wfName : wfNames) {
            filter.append(';').append(OozieClient.FILTER_NAME).append('=').append(wfName);
        }

        try {
            return OozieClientFactory.get(cluster).getJobsInfo(filter.toString(), 1, 1000);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public String reRun(String cluster, String id, Properties props, boolean isForced) throws FalconException {
        OozieClient client = OozieClientFactory.get(cluster);
        String actionId = id;
        try {
            // If a workflow job is supplied, get its parent coord action
            if (id.endsWith("-W")) {
                actionId = client.getJobInfo(id).getParentId();
            }
            if (StringUtils.isBlank(actionId) || !actionId.contains("-C@")) {
                throw new FalconException("coord action id supplied for rerun, " + actionId + ", is not valid.");
            }
            return reRunCoordAction(cluster, client.getCoordActionInfo(actionId), props, isForced).name();
        } catch (Exception e) {
            LOG.error("Unable to rerun action " + actionId, e);
            throw new FalconException(e);
        }
    }


    private String assertStatus(String cluster, String jobId, Status... statuses) throws FalconException {
        String actualStatus = null;
        int retryCount;
        String retry = RuntimeProperties.get().getProperty(WORKFLOW_STATUS_RETRY_COUNT, "30");
        try {
            retryCount = Integer.parseInt(retry);
        } catch (NumberFormatException nfe) {
            throw new FalconException("Invalid value provided for runtime property \""
                    + WORKFLOW_STATUS_RETRY_COUNT + "\". Please provide an integer value.");
        }
        for (int counter = 0; counter < retryCount; counter++) {
            actualStatus = getWorkflowStatus(cluster, jobId);
            if (!statusEquals(actualStatus, statuses)) {
                try {
                    Thread.sleep(WORKFLOW_STATUS_RETRY_DELAY_MS);
                } catch (InterruptedException ignore) {
                    //ignore
                }
            } else {
                return actualStatus;
            }
        }
        throw new FalconException("For Job" + jobId + ", actual statuses: " + actualStatus + ", expected statuses: "
            + Arrays.toString(statuses));
    }

    private boolean statusEquals(String left, Status... right) {
        for (Status rightElement : right) {
            if (left.equals(rightElement.name())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getWorkflowStatus(String cluster, String jobId) throws FalconException {

        OozieClient client = OozieClientFactory.get(cluster);
        try {
            if (jobId.endsWith("-W")) {
                WorkflowJob jobInfo = client.getJobInfo(jobId);
                return jobInfo.getStatus().name();
            } else if (jobId.endsWith("-C")) {
                CoordinatorJob coord = client.getCoordJobInfo(jobId);
                return coord.getStatus().name();
            } else if (jobId.endsWith("-B")) {
                BundleJob bundle = client.getBundleJobInfo(jobId);
                return bundle.getStatus().name();
            } else if (jobId.contains("-C@")) {
                return client.getCoordActionInfo(jobId).getStatus().name();
            }
            throw new IllegalArgumentException("Unhandled jobs id: " + jobId);
        } catch (Exception e) {
            LOG.error("Unable to get status of workflows", e);
            throw new FalconException(e);
        }
    }

    private String scheduleEntity(String cluster, Properties props, Entity entity) throws FalconException {
        for (WorkflowEngineActionListener listener : listeners) {
            listener.beforeSchedule(entity, cluster);
        }
        String jobId = run(cluster, props);
        for (WorkflowEngineActionListener listener : listeners) {
            listener.afterSchedule(entity, cluster);
        }
        return jobId;
    }

    private String run(String cluster, Properties props) throws FalconException {
        try {
            LOG.info("Scheduling on cluster {} with properties {}", cluster, props);
            props.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
            String jobId = OozieClientFactory.get(cluster).run(props);
            LOG.info("Submitted {} on cluster {}", jobId, cluster);
            return jobId;
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new FalconException("Unable to schedule workflows", e);
        }
    }

    private void suspend(String cluster, String jobId) throws FalconException {
        try {
            OozieClientFactory.get(cluster).suspend(jobId);
            assertStatus(cluster, jobId, Status.PREPSUSPENDED, Status.SUSPENDED, Status.SUCCEEDED, Status.FAILED,
                Status.KILLED);
            LOG.info("Suspended job {} on cluster {}", jobId, cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void resume(String cluster, String jobId) throws FalconException {
        try {
            OozieClientFactory.get(cluster).resume(jobId);
            assertStatus(cluster, jobId, Status.PREP, Status.RUNNING, Status.SUCCEEDED, Status.FAILED, Status.KILLED);
            LOG.info("Resumed job {} on cluster {}", jobId, cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void kill(String cluster, String jobId, String rangeType, String scope) throws FalconException {
        try {
            OozieClientFactory.get(cluster).kill(jobId, rangeType, scope);
            LOG.info("Killed job {} for instances {} on cluster {}", jobId, scope, cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void kill(String cluster, String jobId) throws FalconException {
        try {
            OozieClientFactory.get(cluster).kill(jobId);
            assertStatus(cluster, jobId, Status.KILLED, Status.SUCCEEDED, Status.FAILED);
            LOG.info("Killed job {} on cluster {}", jobId, cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void change(String cluster, String jobId, String changeValue) throws FalconException {
        try {
            OozieClientFactory.get(cluster).change(jobId, changeValue);
            LOG.info("Changed bundle/coord {}: {} on cluster {}", jobId, changeValue, cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void change(String cluster, String id, int concurrency, Date endTime, String pauseTime)
        throws FalconException {
        StringBuilder changeValue = new StringBuilder();
        changeValue.append(OozieClient.CHANGE_VALUE_CONCURRENCY).append("=").append(concurrency).append(";");
        if (endTime != null) {
            String endTimeStr = SchemaHelper.formatDateUTC(endTime);
            changeValue.append(OozieClient.CHANGE_VALUE_ENDTIME).append("=").append(endTimeStr).append(";");
        }
        if (pauseTime != null) {
            changeValue.append(OozieClient.CHANGE_VALUE_PAUSETIME).append("=").append(pauseTime);
        }

        String changeValueStr = changeValue.toString();
        if (changeValue.toString().endsWith(";")) {
            changeValueStr = changeValue.substring(0, changeValueStr.length() - 1);
        }

        change(cluster, id, changeValueStr);

        // assert that its really changed
        try {
            OozieClient client = OozieClientFactory.get(cluster);
            CoordinatorJob coord = client.getCoordJobInfo(id);
            for (int counter = 0; counter < 3; counter++) {
                Date intendedPauseTime = (StringUtils.isEmpty(pauseTime) ? null : SchemaHelper.parseDateUTC(pauseTime));
                if (coord.getConcurrency() != concurrency || (endTime != null && !coord.getEndTime().equals(endTime))
                    || (intendedPauseTime != null && !intendedPauseTime.equals(coord.getPauseTime()))) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                        //ignore
                    }
                } else {
                    return;
                }
                coord = client.getCoordJobInfo(id);
            }
            LOG.error("Failed to change coordinator. Current value {}, {}, {}",
                    coord.getConcurrency(), SchemaHelper.formatDateUTC(coord.getEndTime()),
                    SchemaHelper.formatDateUTC(coord.getPauseTime()));
            throw new FalconException("Failed to change coordinator " + id + " with change value " + changeValueStr);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public Properties getWorkflowProperties(String cluster, String jobId) throws FalconException {
        try {
            WorkflowJob jobInfo = OozieClientFactory.get(cluster).getJobInfo(jobId);
            String conf = jobInfo.getConf();
            return OozieUtils.toProperties(conf);
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    @Override
    public InstancesResult getJobDetails(String cluster, String jobId) throws FalconException {
        Instance[] instances = new Instance[1];
        Instance instance = new Instance();
        try {
            WorkflowJob jobInfo = OozieClientFactory.get(cluster).getJobInfo(jobId);
            instance.startTime = jobInfo.getStartTime();
            if (jobInfo.getStatus().name().equals(Status.RUNNING.name())) {
                instance.endTime = new Date();
            } else {
                instance.endTime = jobInfo.getEndTime();
            }
            instance.cluster = cluster;
            instance.runId = jobInfo.getRun();
            instance.status = WorkflowStatus.valueOf(jobInfo.getStatus().name());
            instance.wfParams = getWFParams(jobInfo);
            instances[0] = instance;
            InstancesResult result = new InstancesResult(APIResult.Status.SUCCEEDED,
                    "Instance for workflow id:" + jobId);
            result.setInstances(instances);
            return result;
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    @Override
    public Boolean isWorkflowKilledByUser(String cluster, String jobId) throws FalconException {
        // In case of a failure, the Oozie action has an errorCode.
        // In case of no errorCode in any of the actions would mean its killed by user
        try {
            // Check for error code in all the actions in main workflow
            OozieClient oozieClient = OozieClientFactory.get(cluster);
            List<WorkflowAction> wfActions = oozieClient.getJobInfo(jobId).getActions();
            for (WorkflowAction subWfAction : wfActions) {
                if (StringUtils.isNotEmpty(subWfAction.getErrorCode())) {
                    return false;
                }
            }
            // Assumption taken, there are no sub workflows in user action.
            String subWfId = getUserWorkflowAction(wfActions);
            List<WorkflowAction> subWfActions;
            // Check for error code in all the user-workflow(sub-workflow)'s actions.
            if (StringUtils.isNotBlank(subWfId)) {
                subWfActions = oozieClient.getJobInfo(subWfId).getActions();
                for (WorkflowAction subWfAction : subWfActions) {
                    if (StringUtils.isNotEmpty(subWfAction.getErrorCode())) {
                        return false;
                    }
                }
            }
            return true;
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    @Override
    public String getName() {
        return "oozie";
    }

    private String getUserWorkflowAction(List<WorkflowAction> actionsList){
        for (WorkflowAction wfAction : actionsList) {
            if (StringUtils.equals(wfAction.getName(), "user-action")) {
                return wfAction.getExternalId();
            }
        }
        return null;
    }
}

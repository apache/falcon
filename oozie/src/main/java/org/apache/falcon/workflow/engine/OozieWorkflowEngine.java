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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.*;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.InstancesSummaryResult.InstanceSummary;
import org.apache.falcon.update.UpdateHelper;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.workflow.OozieWorkflowBuilder;
import org.apache.falcon.workflow.WorkflowBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.rest.RestConstants;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Workflow engine which uses oozies APIs.
 */
public class OozieWorkflowEngine extends AbstractWorkflowEngine {

    private static final Logger LOG = Logger.getLogger(OozieWorkflowEngine.class);

    public static final String ENGINE = "oozie";
    private static final BundleJob MISSING = new NullBundleJob();

    private static final List<WorkflowJob.Status> WF_KILL_PRECOND = Arrays.asList(WorkflowJob.Status.PREP,
            WorkflowJob.Status.RUNNING, WorkflowJob.Status.SUSPENDED, WorkflowJob.Status.FAILED);
    private static final List<WorkflowJob.Status> WF_SUSPEND_PRECOND = Arrays
            .asList(WorkflowJob.Status.RUNNING);
    private static final List<WorkflowJob.Status> WF_RESUME_PRECOND = Arrays
            .asList(WorkflowJob.Status.SUSPENDED);
    private static final List<WorkflowJob.Status> WF_RERUN_PRECOND = Arrays.asList(WorkflowJob.Status.FAILED,
            WorkflowJob.Status.KILLED, WorkflowJob.Status.SUCCEEDED);
    private static final List<CoordinatorAction.Status> COORD_RERUN_PRECOND = Arrays
            .asList(CoordinatorAction.Status.TIMEDOUT, CoordinatorAction.Status.FAILED);

    private static final List<Job.Status> BUNDLE_ACTIVE_STATUS = Arrays.asList(
            Job.Status.PREP, Job.Status.RUNNING, Job.Status.SUSPENDED,
            Job.Status.PREPSUSPENDED, Job.Status.DONEWITHERROR);
    private static final List<Job.Status> BUNDLE_SUSPENDED_STATUS = Arrays.asList(
            Job.Status.PREPSUSPENDED, Job.Status.SUSPENDED);
    private static final List<Job.Status> BUNDLE_RUNNING_STATUS = Arrays.asList(
            Job.Status.PREP, Job.Status.RUNNING);

    private static final List<Job.Status> BUNDLE_SUSPEND_PRECOND = Arrays.asList(
            Job.Status.PREP, Job.Status.RUNNING, Job.Status.DONEWITHERROR);
    private static final List<Job.Status> BUNDLE_RESUME_PRECOND = Arrays.asList(
            Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED);
    private static final String FALCON_INSTANCE_ACTION_CLUSTERS = "falcon.instance.action.clusters";
    private static final String FALCON_INSTANCE_SOURCE_CLUSTERS = "falcon.instance.source.clusters";

    private static final String[] BUNDLE_UPDATEABLE_PROPS = new String[]{
        "parallel", "clusters.clusters[\\d+].validity.end", };

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
    public void schedule(Entity entity) throws FalconException {
        Map<String, BundleJob> bundleMap = findLatestBundle(entity);
        List<String> schedClusters = new ArrayList<String>();
        for (Map.Entry<String, BundleJob> entry : bundleMap.entrySet()) {
            String cluster = entry.getKey();
            BundleJob bundleJob = entry.getValue();
            if (bundleJob == MISSING) {
                schedClusters.add(cluster);
            } else {
                LOG.debug("The entity " + entity.getName() + " is already scheduled on cluster " + cluster);
            }
        }

        if (!schedClusters.isEmpty()) {
            WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            Map<String, Properties> newFlows = builder.newWorkflowSchedule(entity, schedClusters);
            for (Map.Entry<String, Properties> entry : newFlows.entrySet()) {
                String cluster = entry.getKey();
                LOG.info("Scheduling " + entity.toShortString() + " on cluster " + cluster);
                scheduleEntity(cluster, entry.getValue(), entity);
                commitStagingPath(cluster, entry.getValue().getProperty(OozieClient.BUNDLE_APP_PATH));
            }
        }
    }

    private void commitStagingPath(String cluster, String path) throws FalconException {
        path = StringUtils.removeStart(path, "${nameNode}");
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        FileSystem fs = HadoopClientFactory.get().createFileSystem(ClusterHelper.getConfiguration(clusterEntity));
        try {
            fs.create(new Path(path, EntityUtil.SUCCEEDED_FILE_NAME)).close();
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public boolean isActive(Entity entity) throws FalconException {
        return isBundleInState(entity, BundleStatus.ACTIVE);
    }

    @Override
    public boolean isSuspended(Entity entity) throws FalconException {
        return isBundleInState(entity, BundleStatus.SUSPENDED);
    }

    private enum BundleStatus {
        ACTIVE, RUNNING, SUSPENDED
    }

    private boolean isBundleInState(Entity entity, BundleStatus status)
        throws FalconException {

        Map<String, BundleJob> bundles = findLatestBundle(entity);
        for (BundleJob bundle : bundles.values()) {
            if (bundle == MISSING) {// There is no active bundle
                return false;
            }

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
            default:
            }
        }
        return true;
    }

    //Return all bundles for the entity in the requested cluster
    private List<BundleJob> findBundles(Entity entity, String clusterName) throws FalconException {

        try {
            List<BundleJob> jobs = OozieClientFactory.get(clusterName).getBundleJobsInfo(
                    OozieClient.FILTER_NAME + "=" + EntityUtil.getWorkflowName(entity) + ";", 0, 256);
            if (jobs != null) {
                List<BundleJob> filteredJobs = new ArrayList<BundleJob>();
                for (BundleJob job : jobs) {
                    //Filtering bundles that correspond to deleted entities(endtime is set when an entity is deleted)
                    if (job.getEndTime() == null) {
                        filteredJobs.add(job);
                        LOG.debug("Found bundle " + job.getId());
                    }
                }
                return filteredJobs;
            }
            return new ArrayList<BundleJob>();
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
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
            jobMap.put(cluster, findLatestBundle(entity, cluster));
        }
        return jobMap;
    }

    //Return latest bundle(last created) for the entity in the requested cluster
    private BundleJob findLatestBundle(Entity entity, String cluster) throws FalconException {
        List<BundleJob> bundles = findBundles(entity, cluster);
        Date latest = null;
        BundleJob bundle = MISSING;
        for (BundleJob job : bundles) {
            if (latest == null || latest.before(job.getCreatedTime())) {
                bundle = job;
                latest = job.getCreatedTime();
            }
        }
        return bundle;
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
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        String result = null;
        for (String cluster : clusters) {
            result = doBundleAction(entity, action, cluster);
        }
        return result;
    }

    private String doBundleAction(Entity entity, BundleAction action, String cluster)
        throws FalconException {

        List<BundleJob> jobs = findBundles(entity, cluster);
        if (jobs.isEmpty()) {
            LOG.warn("No active job found for " + entity.getName());
            return "FAILED";
        }

        beforeAction(entity, action, cluster);
        for (BundleJob job : jobs) {
            switch (action) {
            case SUSPEND:
                // not already suspended and preconditions are true
                if (!BUNDLE_SUSPENDED_STATUS.contains(job.getStatus())
                        && BUNDLE_SUSPEND_PRECOND.contains(job.getStatus())) {
                    suspend(cluster, job.getId());
                }
                break;

            case RESUME:
                // not already running and preconditions are true
                if (!BUNDLE_RUNNING_STATUS.contains(job.getStatus())
                        && BUNDLE_RESUME_PRECOND.contains(job.getStatus())) {
                    resume(cluster, job.getId());
                }
                break;

            case KILL:
                // not already killed and preconditions are true
                killBundle(cluster, job);
                break;

            default:
            }
            afterAction(entity, action, cluster);
        }

        return "SUCCESS";
    }

    private void killBundle(String clusterName, BundleJob job) throws FalconException {
        ProxyOozieClient client = OozieClientFactory.get(clusterName);
        try {
            //kill all coords
            for (CoordinatorJob coord : job.getCoordinators()) {
                client.kill(coord.getId());
                LOG.debug("Killed coord " + coord.getId() + " on cluster " + clusterName);
            }

            //set end time of bundle
            client.change(job.getId(),
                    OozieClient.CHANGE_VALUE_ENDTIME + "=" + SchemaHelper.formatDateUTC(new Date()));
            LOG.debug("Changed end time of bundle " + job.getId() + " on cluster " + clusterName);

            //kill bundle
            client.kill(job.getId());
            LOG.debug("Killed bundle " + job.getId() + " on cluster " + clusterName);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void beforeAction(Entity entity, BundleAction action, String cluster)
        throws FalconException {

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

    private void afterAction(Entity entity, BundleAction action, String cluster)
        throws FalconException {

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
    public InstancesResult getRunningInstances(Entity entity)
        throws FalconException {
        try {
            WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(
                    ENGINE, entity);
            Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
            List<Instance> runInstances = new ArrayList<Instance>();
            String[] wfNames = builder.getWorkflowNames(entity);
            List<String> coordNames = new ArrayList<String>();
            for (String wfName : wfNames) {
                if (EntityUtil.getWorkflowName(Tag.RETENTION, entity)
                        .toString().equals(wfName)) {
                    continue;
                }
                coordNames.add(wfName);
            }

            for (String cluster : clusters) {
                ProxyOozieClient client = OozieClientFactory.get(cluster);
                List<WorkflowJob> wfs = getRunningWorkflows(cluster, coordNames);
                if (wfs != null) {
                    for (WorkflowJob job : wfs) {
                        WorkflowJob wf = client.getJobInfo(job.getId());
                        if (StringUtils.isEmpty(wf.getParentId())) {
                            continue;
                        }

                        CoordinatorAction action = client.getCoordActionInfo(wf
                                .getParentId());
                        String nominalTimeStr = SchemaHelper
                                .formatDateUTC(action.getNominalTime());
                        Instance instance = new Instance(cluster,
                                nominalTimeStr, WorkflowStatus.RUNNING);
                        instance.startTime = wf.getStartTime();
                        if (entity.getEntityType() == EntityType.FEED) {
                            instance.sourceCluster = getSourceCluster(cluster,
                                    action, entity);
                        }
                        runInstances.add(instance);
                    }
                }
            }
            return new InstancesResult("Running Instances",
                    runInstances.toArray(new Instance[runInstances.size()]));

        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public InstancesResult killInstances(Entity entity, Date start, Date end,
                                         Properties props) throws FalconException {
        return doJobAction(JobAction.KILL, entity, start, end, props);
    }

    @Override
    public InstancesResult reRunInstances(Entity entity, Date start, Date end,
                                          Properties props) throws FalconException {
        return doJobAction(JobAction.RERUN, entity, start, end, props);
    }

    @Override
    public InstancesResult suspendInstances(Entity entity, Date start,
                                            Date end, Properties props) throws FalconException {
        return doJobAction(JobAction.SUSPEND, entity, start, end, props);
    }

    @Override
    public InstancesResult resumeInstances(Entity entity, Date start, Date end,
                                           Properties props) throws FalconException {
        return doJobAction(JobAction.RESUME, entity, start, end, props);
    }

    @Override
    public InstancesResult getStatus(Entity entity, Date start, Date end)
        throws FalconException {

        return doJobAction(JobAction.STATUS, entity, start, end, null);
    }

    @Override
    public InstancesSummaryResult getSummary(Entity entity, Date start, Date end)
        throws FalconException {

        return doSummaryJobAction(entity, start, end, null);
    }

    private static enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS, SUMMARY
    }

    private WorkflowJob getWorkflowInfo(String cluster, String wfId)
        throws FalconException {
        try {
            return OozieClientFactory.get(cluster).getJobInfo(wfId);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private InstancesResult doJobAction(JobAction action, Entity entity,
                                        Date start, Date end, Properties props) throws FalconException {
        Map<String, List<CoordinatorAction>> actionsMap = getCoordActions(entity, start, end);
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
                String status;
                instanceCount++;
                try {
                    status = performAction(cluster, action, coordinatorAction, props);
                } catch (FalconException e) {
                    LOG.warn("Unable to perform action " + action + " on cluster ", e);
                    status = WorkflowStatus.ERROR.name();
                    overallStatus = APIResult.Status.PARTIAL;
                }

                String nominalTimeStr = SchemaHelper.formatDateUTC(coordinatorAction.getNominalTime());
                InstancesResult.Instance instance = new InstancesResult.Instance(
                        cluster, nominalTimeStr, WorkflowStatus.valueOf(status));
                if (StringUtils.isNotEmpty(coordinatorAction.getExternalId())) {
                    WorkflowJob jobInfo = getWorkflowInfo(cluster, coordinatorAction.getExternalId());
                    instance.startTime = jobInfo.getStartTime();
                    instance.endTime = jobInfo.getEndTime();
                    instance.logFile = jobInfo.getConsoleUrl();
                    instance.sourceCluster = sourceCluster;
                }
                instance.details = coordinatorAction.getMissingDependencies();
                instances.add(instance);
            }
        }
        if (instanceCount < 2 && overallStatus == APIResult.Status.PARTIAL) {
            overallStatus = APIResult.Status.FAILED;
        }
        InstancesResult instancesResult = new InstancesResult(overallStatus, action.name());
        instancesResult.setInstances(instances.toArray(new Instance[instances.size()]));
        return instancesResult;
    }

    private InstancesSummaryResult doSummaryJobAction(Entity entity,
                                        Date start, Date end, Properties props) throws FalconException {

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
            ProxyOozieClient client = OozieClientFactory.get(cluster);
            List<CoordinatorJob> applicableCoords = getApplicableCoords(entity, client, start, end, bundles);
            long unscheduledInstances = 0;
            boolean isLastCoord = false;

            for (int i = 0; i < applicableCoords.size(); i++) {
                CoordinatorJob coord = applicableCoords.get(i);
                Frequency freq = createFrequency(String.valueOf(coord.getFrequency()), coord.getTimeUnit());
                TimeZone tz = EntityUtil.getTimeZone(coord.getTimeZone());
                Date iterStart = EntityUtil.getNextStartTime(coord.getStartTime(), freq, tz, start);
                Date iterEnd = (coord.getLastActionTime().before(end) ? coord.getLastActionTime() : end);

                if (i == (applicableCoords.size() - 1)) {
                    isLastCoord = true;
                }

                int startActionNumber = EntityUtil.getInstanceSequence(coord.getStartTime(), freq, tz, iterStart);
                int lastMaterializedActionNumber = EntityUtil.getInstanceSequence(coord.getStartTime(),
                        freq, tz, iterEnd);
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
                    LOG.debug("Unable to get details for coordinator " + coord.getId() + " " + e.getMessage());
                    throw new FalconException(e);
                }

                if (coordJob != null) {
                    updateInstanceSummary(coordJob, instancesSummary);
                }
            }

            if (unscheduledInstances > 0) {
                instancesSummary.put("UNSCHEDULED", unscheduledInstances);
            }

            InstanceSummary summary= new InstanceSummary(cluster, instancesSummary);
            instances.add(summary);
        }

        InstancesSummaryResult instancesSummaryResult = new InstancesSummaryResult(APIResult.Status.SUCCEEDED,
                JobAction.SUMMARY.name());
        instancesSummaryResult.setInstancesSummary(instances.toArray(new InstanceSummary[instances.size()]));
        return instancesSummaryResult;
    }

    private void updateInstanceSummary(CoordinatorJob coordJob, Map<String, Long> instancesSummary) {
        List<CoordinatorAction> actions = coordJob.getActions();

        for (CoordinatorAction coordAction :  actions) {
            if (instancesSummary.containsKey(coordAction.getStatus().name())) {
                instancesSummary.put(coordAction.getStatus().name(),
                        instancesSummary.get(coordAction.getStatus().name()) + 1L);
            } else {
                instancesSummary.put(coordAction.getStatus().name(), 1L);
            }
        }
    }

    private String performAction(String cluster, JobAction action, CoordinatorAction coordinatorAction,
                                 Properties props) throws FalconException {
        WorkflowJob jobInfo = null;
        String status = coordinatorAction.getStatus().name();
        if (StringUtils.isNotEmpty(coordinatorAction.getExternalId())) {
            jobInfo = getWorkflowInfo(cluster, coordinatorAction.getExternalId());
            status = jobInfo.getStatus().name();
        }
        switch (action) {
        case KILL:
            if (jobInfo == null || !WF_KILL_PRECOND.contains(jobInfo.getStatus())) {
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
            if (jobInfo == null && COORD_RERUN_PRECOND.contains(coordinatorAction.getStatus())) {
                //Coord action re-run
                reRunCoordAction(cluster, coordinatorAction);
                status = Status.RUNNING.name();
            } else if (jobInfo != null && WF_RERUN_PRECOND.contains(jobInfo.getStatus())) {
                //wf re-run
                reRun(cluster, jobInfo.getId(), props);
                status = Status.RUNNING.name();
            }
            break;


        case STATUS:
            break;

        default:
            throw new IllegalArgumentException("Unhandled action " + action);
        }

        return mapActionStatus(status);
    }

    private void reRunCoordAction(String cluster, CoordinatorAction coordinatorAction) throws FalconException  {
        try{
            OozieClient client = OozieClientFactory.get(cluster);
            client.reRunCoord(coordinatorAction.getJobId(),
                RestConstants.JOB_COORD_RERUN_ACTION,
                    Integer.toString(coordinatorAction.getActionNumber()), true, true);
            assertCoordActionStatus(cluster, coordinatorAction.getId(),
                org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                    org.apache.oozie.client.CoordinatorAction.Status.WAITING,
                org.apache.oozie.client.CoordinatorAction.Status.READY);
            LOG.info("Rerun job " + coordinatorAction.getId() + " on cluster " + cluster);
        }catch (Exception e) {
            LOG.error("Unable to rerun workflows", e);
            throw new FalconException(e);
        }
    }

    private void assertCoordActionStatus(String cluster, String coordActionId,
            org.apache.oozie.client.CoordinatorAction.Status... statuses)
        throws FalconException, OozieClientException {
        OozieClient client = OozieClientFactory.get(cluster);
        CoordinatorAction actualStatus = client.getCoordActionInfo(coordActionId);
        for (int counter = 0; counter < 3; counter++) {
            for(org.apache.oozie.client.CoordinatorAction.Status status : statuses) {
                if (status.equals(actualStatus.getStatus())) {
                    return;
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            actualStatus = client.getCoordActionInfo(coordActionId);
        }
        throw new FalconException("For Job" + coordActionId + ", actual statuses: "
            +actualStatus + ", expected statuses: "
                 + Arrays.toString(statuses));
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

    private List<String> getIncludedClusters(Properties props,
                                             String clustersType) {
        String clusters = props == null ? "" : props.getProperty(clustersType,
                "");
        List<String> clusterList = new ArrayList<String>();
        for (String cluster : clusters.split(",")) {
            if (StringUtils.isNotEmpty(cluster)) {
                clusterList.add(cluster.trim());
            }
        }
        return clusterList;
    }

    private String mapActionStatus(String status) {
        if (CoordinatorAction.Status.READY.toString().equals(status)
                || CoordinatorAction.Status.WAITING.toString().equals(status)
                || CoordinatorAction.Status.SUBMITTED.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.WAITING.name();
        } else if (CoordinatorAction.Status.DISCARDED.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.KILLED.name();
        } else if (CoordinatorAction.Status.TIMEDOUT.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.FAILED.name();
        } else if (WorkflowJob.Status.PREP.toString().equals(status)) {
            return InstancesResult.WorkflowStatus.RUNNING.name();
        } else {
            return status;
        }
    }

    protected Map<String, List<CoordinatorAction>> getCoordActions(
            Entity entity, Date start, Date end) throws FalconException {
        Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
        Map<String, List<CoordinatorAction>> actionsMap = new HashMap<String, List<CoordinatorAction>>();

        for (Map.Entry<String, List<BundleJob>> entry : bundlesMap.entrySet()) {
            String cluster = entry.getKey();
            List<BundleJob> bundles = entry.getValue();
            ProxyOozieClient client = OozieClientFactory.get(cluster);
            List<CoordinatorJob> applicableCoords = getApplicableCoords(entity, client, start, end, bundles);
            List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();

            for (CoordinatorJob coord : applicableCoords) {
                Frequency freq = createFrequency(String.valueOf(coord.getFrequency()), coord.getTimeUnit());
                TimeZone tz = EntityUtil.getTimeZone(coord.getTimeZone());
                Date iterStart = EntityUtil.getNextStartTime(coord.getStartTime(), freq, tz, start);
                Date iterEnd = (coord.getNextMaterializedTime().before(end) ? coord.getNextMaterializedTime() : end);
                while (!iterStart.after(iterEnd)) {
                    int sequence = EntityUtil.getInstanceSequence(coord.getStartTime(), freq, tz, iterStart);
                    String actionId = coord.getId() + "@" + sequence;
                    CoordinatorAction coordActionInfo = null;
                    try {
                        coordActionInfo = client.getCoordActionInfo(actionId);
                    } catch (OozieClientException e) {
                        LOG.debug("Unable to get action for " + actionId + " " + e.getMessage());
                    }
                    if (coordActionInfo != null) {
                        actions.add(coordActionInfo);
                    }
                    Calendar startCal = Calendar.getInstance(EntityUtil.getTimeZone(coord.getTimeZone()));
                    startCal.setTime(iterStart);
                    startCal.add(freq.getTimeUnit().getCalendarUnit(), Integer.valueOf((coord.getFrequency())));
                    iterStart = startCal.getTime();
                }
            }
            actionsMap.put(cluster, actions);
        }
        return actionsMap;
    }

    private Frequency createFrequency(String frequency, Timeunit timeUnit) {
        return new Frequency(frequency, OozieTimeUnit.valueOf(timeUnit.name())
                .getFalconTimeUnit());
    }

    /**
     * TimeUnit as understood by Oozie.
     */
    private enum OozieTimeUnit {
        MINUTE(TimeUnit.minutes), HOUR(TimeUnit.hours), DAY(TimeUnit.days), WEEK(
                null), MONTH(TimeUnit.months), END_OF_DAY(null), END_OF_MONTH(
                null), NONE(null);

        private TimeUnit falconTimeUnit;

        private OozieTimeUnit(TimeUnit falconTimeUnit) {
            this.falconTimeUnit = falconTimeUnit;
        }

        public TimeUnit getFalconTimeUnit() {
            if (falconTimeUnit == null) {
                throw new IllegalStateException("Invalid coord frequency: "
                        + name());
            }
            return falconTimeUnit;
        }
    }

    private List<CoordinatorJob> getApplicableCoords(Entity entity, ProxyOozieClient client,
                                                     Date start, Date end, List<BundleJob> bundles)
        throws FalconException {

        List<CoordinatorJob> applicableCoords = new ArrayList<CoordinatorJob>();
        try {
            for (BundleJob bundle : bundles) {
                List<CoordinatorJob> coords = client.getBundleJobInfo(
                        bundle.getId()).getCoordinators();
                for (CoordinatorJob coord : coords) {
                    String coordName = EntityUtil.getWorkflowName(
                            Tag.RETENTION, entity).toString();
                    if (coordName.equals(coord.getAppName())) {
                        continue;
                    }
                    // if end time is before coord-start time or start time is
                    // after coord-end time ignore.
                    if (!(end.compareTo(coord.getStartTime()) <= 0 || start
                            .compareTo(coord.getEndTime()) >= 0)) {
                        applicableCoords.add(coord);
                    }
                }
            }
            sortCoordsByStartTime(applicableCoords);
            return applicableCoords;
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    protected void sortCoordsByStartTime(List<CoordinatorJob> consideredCoords) {
        Collections.sort(consideredCoords, new Comparator<CoordinatorJob>() {
            @Override
            public int compare(CoordinatorJob left, CoordinatorJob right) {
                Date leftStart = left.getStartTime();
                Date rightStart = right.getStartTime();
                return leftStart.compareTo(rightStart);
            }
        });
    }

    private boolean canUpdateBundle(Entity oldEntity, Entity newEntity, boolean wfUpdated) throws FalconException {
        return !wfUpdated && EntityUtil.equals(oldEntity, newEntity, BUNDLE_UPDATEABLE_PROPS);
    }

    @Override
    public Date update(Entity oldEntity, Entity newEntity, String cluster, Date effectiveTime) throws FalconException {
        boolean entityUpdated = UpdateHelper.isEntityUpdated(oldEntity, newEntity, cluster);
        boolean wfUpdated = UpdateHelper.isWorkflowUpdated(cluster, newEntity);

        if (!entityUpdated && !wfUpdated) {
            LOG.debug("Nothing to update for cluster " + cluster);
            return null;
        }

        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        Path stagingPath = EntityUtil.getLastCommittedStagingPath(clusterEntity, oldEntity);
        if (stagingPath != null) {  //update if entity is scheduled
            BundleJob bundle = findBundleForStagingPath(cluster, oldEntity, stagingPath);
            bundle = getBundleInfo(cluster, bundle.getId());
            LOG.info("Updating entity through Workflow Engine" + newEntity.toShortString());
            Date newEndTime = EntityUtil.getEndTime(newEntity, cluster);
            if (newEndTime.before(now())) {
                throw new FalconException("New end time for " + newEntity.getName()
                        + " is past current time. Entity can't be updated. Use remove and add");
            }

            LOG.debug("Updating for cluster : " + cluster + ", bundle: " + bundle.getId());

            if (canUpdateBundle(oldEntity, newEntity, wfUpdated)) {
                // only concurrency and endtime are changed. So, change coords
                LOG.info("Change operation is adequate! : " + cluster + ", bundle: " + bundle.getId());
                updateCoords(cluster, bundle, EntityUtil.getParallel(newEntity),
                        EntityUtil.getEndTime(newEntity, cluster));
                return newEndTime;
            }

            LOG.debug("Going to update ! : " + newEntity.toShortString() + "for cluster " + cluster + ", bundle: "
                    + bundle.getId());
            effectiveTime = updateInternal(oldEntity, newEntity, cluster, bundle, false, effectiveTime);
            LOG.info("Entity update complete : " + newEntity.toShortString() + cluster + ", bundle: " + bundle.getId());
        }

        //Update affected entities
        Set<Entity> affectedEntities = EntityGraph.get().getDependents(oldEntity);
        for (Entity affectedEntity : affectedEntities) {
            if (affectedEntity.getEntityType() != EntityType.PROCESS) {
                continue;
            }

            LOG.info("Dependent entities need to be updated " + affectedEntity.toShortString());
            if (!UpdateHelper.shouldUpdate(oldEntity, newEntity, affectedEntity, cluster)) {
                continue;
            }

            BundleJob affectedProcBundle = findLatestBundle(affectedEntity, cluster);
            if (affectedProcBundle == MISSING) {
                continue;
            }

            LOG.info("Triggering update for " + cluster + ", " + affectedProcBundle.getId());

            //TODO handle roll forward
//            BundleJob feedBundle = findLatestBundle(newEntity, cluster);
//            if (feedBundle == MISSING) {
//                throw new IllegalStateException("Unable to find feed bundle in " + cluster
//                        + " for entity " + newEntity.getName());
//            }
//            boolean processCreated = feedBundle.getCreatedTime().before(
//                    affectedProcBundle.getCreatedTime());

            Date depEndTime =
                    updateInternal(affectedEntity, affectedEntity, cluster, affectedProcBundle, false, effectiveTime);
            if (effectiveTime == null || effectiveTime.after(depEndTime)) {
                effectiveTime = depEndTime;
            }
            LOG.info("Entity update complete : " + affectedEntity.toShortString() + cluster
                    + ", bundle: " + affectedProcBundle.getId());
        }
        LOG.info("Entity update and all dependent entities updated: " + oldEntity.toShortString());
        return effectiveTime;
    }

    //Returns bundle whose app path is same as the staging path(argument)
    private BundleJob findBundleForStagingPath(String cluster, Entity entity, Path stagingPath) throws FalconException {
        List<BundleJob> bundles = findBundles(entity, cluster);
        String bundlePath = stagingPath.toUri().getPath();
        for (BundleJob bundle : bundles) {
            if (bundle.getAppPath().endsWith(bundlePath)) {
                return bundle;
            }
        }
        return null;
    }

    private Date now() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    private Date offsetTime(Date date, int minute) {
        return new Date(1000L * 60 * minute + date.getTime());
    }

    private Date getCoordLastActionTime(CoordinatorJob coord) {
        if (coord.getNextMaterializedTime() != null) {
            Calendar cal = Calendar.getInstance(EntityUtil.getTimeZone(coord
                    .getTimeZone()));
            cal.setTime(coord.getLastActionTime());
            Frequency freq = createFrequency(String.valueOf(coord.getFrequency()), coord.getTimeUnit());
            cal.add(freq.getTimeUnit().getCalendarUnit(), -freq.getFrequencyAsInt());
            return cal.getTime();
        }
        return null;
    }

    private void updateCoords(String cluster, BundleJob bundle, int concurrency,
                              Date endTime) throws FalconException {
        if (endTime.compareTo(now()) <= 0) {
            throw new FalconException("End time " + SchemaHelper.formatDateUTC(endTime) + " can't be in the past");
        }

        // change coords
        for (CoordinatorJob coord : bundle.getCoordinators()) {
            LOG.debug("Updating endtime of coord " + coord.getId() + " to "
                    + SchemaHelper.formatDateUTC(endTime) + " on cluster " + cluster);
            Date lastActionTime = getCoordLastActionTime(coord);
            if (lastActionTime == null) { // nothing is materialized
                LOG.info("Nothing is materialized for this coord: " + coord.getId());
                if (endTime.compareTo(coord.getStartTime()) <= 0) {
                    LOG.info("Setting end time to START TIME " + SchemaHelper.formatDateUTC(coord.getStartTime()));
                    change(cluster, coord.getId(), concurrency, coord.getStartTime(), null);
                } else {
                    LOG.info("Setting end time to START TIME " + SchemaHelper.formatDateUTC(endTime));
                    change(cluster, coord.getId(), concurrency, endTime, null);
                }
            } else {
                LOG.info("Actions have materialized for this coord: " + coord.getId() + ", last action "
                        + SchemaHelper.formatDateUTC(lastActionTime));
                if (!endTime.after(lastActionTime)) {
                    Date pauseTime = offsetTime(endTime, -1);
                    // set pause time which deletes future actions
                    LOG.info("Setting pause time on coord : " + coord.getId() + " to "
                            + SchemaHelper.formatDateUTC(pauseTime));
                    change(cluster, coord.getId(), concurrency, null, SchemaHelper.formatDateUTC(pauseTime));
                }
                change(cluster, coord.getId(), concurrency, endTime, "");
            }
        }
    }

    private void suspendCoords(String cluster, BundleJob bundle) throws FalconException {
        for (CoordinatorJob coord : bundle.getCoordinators()) {
            suspend(cluster, coord.getId());
        }
    }

    private void resumeCoords(String cluster, BundleJob bundle) throws FalconException {
        for (CoordinatorJob coord : bundle.getCoordinators()) {
            resume(cluster, coord.getId());
        }
    }

    private Date updateInternal(Entity oldEntity, Entity newEntity, String cluster, BundleJob oldBundle,
                                boolean alreadyCreated, Date inEffectiveTime) throws FalconException {
        OozieWorkflowBuilder<Entity> builder =
                (OozieWorkflowBuilder<Entity>) WorkflowBuilder.getBuilder(ENGINE, oldEntity);

        // Change end time of coords and schedule new bundle
        Job.Status oldBundleStatus = oldBundle.getStatus();
        suspendCoords(cluster, oldBundle);

        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        Path stagingPath = EntityUtil.getLatestStagingPath(clusterEntity, oldEntity);
        BundleJob newBundle = findBundleForStagingPath(cluster, oldEntity, stagingPath);
        Date effectiveTime;
        if (oldBundle.getAppPath().endsWith(stagingPath.toUri().getPath()) || newBundle == null || !alreadyCreated) {
            // new entity is not scheduled yet, create new bundle
            LOG.info("New bundle hasn't been created yet. So will create one");
            effectiveTime = offsetTime(now(), 3);
            if (inEffectiveTime != null && inEffectiveTime.after(effectiveTime)) {
                effectiveTime = inEffectiveTime;
            }
            effectiveTime = builder.getNextStartTime(newEntity, cluster, effectiveTime);
            newBundle =
                    getBundleInfo(cluster, scheduleForUpdate(newEntity, cluster, effectiveTime, oldBundle.getUser()));
            LOG.info("New bundle " + newBundle.getId() + " scheduled successfully with start time "
                    + SchemaHelper.formatDateUTC(effectiveTime));
        } else {
            LOG.info("New bundle has already been created. Bundle Id: " + newBundle.getId() + ", Start: "
                    + SchemaHelper.formatDateUTC(newBundle.getStartTime()) + ", End: " + newBundle.getEndTime());
            effectiveTime = getMinStartTime(newBundle);
            LOG.info("Will set old coord end time to " + SchemaHelper.formatDateUTC(effectiveTime));
        }
        if (effectiveTime != null) {
            //set endtime for old coords
            updateCoords(cluster, oldBundle, EntityUtil.getParallel(oldEntity), effectiveTime);
        }

        if (oldBundleStatus != Job.Status.SUSPENDED && oldBundleStatus != Job.Status.PREPSUSPENDED) {
            resumeCoords(cluster, oldBundle);
        }

        //create _SUCCESS in staging path to mark update is complete(to handle roll-forward for updates)
        commitStagingPath(cluster, newBundle.getAppPath());
        return effectiveTime;
    }

    private String scheduleForUpdate(Entity entity, String cluster, Date startDate, String user)
        throws FalconException {
        WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        Properties bundleProps = builder.newWorkflowSchedule(entity, startDate, cluster, user);
        LOG.info("Scheduling " + entity.toShortString() + " on cluster " + cluster + " with props " + bundleProps);
        if (bundleProps != null) {
            return scheduleEntity(cluster, bundleProps, entity);
        } else {
            LOG.info("No new workflow to be scheduled for this " + entity.toShortString());
            return null;
        }
    }

    private Date getMinStartTime(BundleJob bundle) {
        Date startTime = null;
        if (bundle.getCoordinators() != null) {
            for (CoordinatorJob coord : bundle.getCoordinators()) {
                if (startTime == null || startTime.after(coord.getStartTime())) {
                    startTime = coord.getStartTime();
                }
            }
        }
        return startTime;
    }

    private BundleJob getBundleInfo(String cluster, String bundleId)
        throws FalconException {
        try {
            return OozieClientFactory.get(cluster).getBundleJobInfo(bundleId);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private List<WorkflowJob> getRunningWorkflows(String cluster,
                                                  List<String> wfNames) throws FalconException {
        StringBuilder filter = new StringBuilder();
        filter.append(OozieClient.FILTER_STATUS).append('=')
                .append(Job.Status.RUNNING.name());
        for (String wfName : wfNames) {
            filter.append(';').append(OozieClient.FILTER_NAME).append('=')
                    .append(wfName);
        }

        try {
            return OozieClientFactory.get(cluster).getJobsInfo(filter.toString(), 1, 1000);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public void reRun(String cluster, String jobId, Properties props)
        throws FalconException {

        ProxyOozieClient client = OozieClientFactory.get(cluster);
        try {
            WorkflowJob jobInfo = client.getJobInfo(jobId);
            Properties jobprops = OozieUtils.toProperties(jobInfo.getConf());
            if (props == null || props.isEmpty()) {
                jobprops.put(OozieClient.RERUN_FAIL_NODES, "false");
            } else {
                for (Entry<Object, Object> entry : props.entrySet()) {
                    jobprops.put(entry.getKey(), entry.getValue());
                }
            }
            jobprops.remove(OozieClient.COORDINATOR_APP_PATH);
            jobprops.remove(OozieClient.BUNDLE_APP_PATH);
            client.reRun(jobId, jobprops);
            assertStatus(cluster, jobId, Job.Status.RUNNING);
            LOG.info("Rerun job " + jobId + " on cluster " + cluster);
        } catch (Exception e) {
            LOG.error("Unable to rerun workflows", e);
            throw new FalconException(e);
        }
    }

    private void assertStatus(String cluster, String jobId, Status... statuses)
        throws FalconException {

        String actualStatus = getWorkflowStatus(cluster, jobId);
        for (int counter = 0; counter < 3; counter++) {
            if (!statusEquals(actualStatus, statuses)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignore) {
                    //ignore
                }
            } else {
                return;
            }
            actualStatus = getWorkflowStatus(cluster, jobId);
        }
        throw new FalconException("For Job" + jobId + ", actual statuses: "
                + actualStatus + ", expected statuses: "
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
    public String getWorkflowStatus(String cluster, String jobId)
        throws FalconException {

        ProxyOozieClient client = OozieClientFactory.get(cluster);
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
            String jobId = OozieClientFactory.get(cluster).run(props);
            LOG.info("Submitted " + jobId + " on cluster " + cluster
                    + " with properties : " + props);
            return jobId;
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new FalconException("Unable to schedule workflows", e);
        }
    }

    private void suspend(String cluster, String jobId) throws FalconException {
        try {
            OozieClientFactory.get(cluster).suspend(jobId);
            assertStatus(cluster, jobId, Status.PREPSUSPENDED, Status.SUSPENDED, Status.SUCCEEDED,
                    Status.FAILED, Status.KILLED);
            LOG.info("Suspended job " + jobId + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void resume(String cluster, String jobId) throws FalconException {
        try {
            OozieClientFactory.get(cluster).resume(jobId);
            assertStatus(cluster, jobId, Status.RUNNING, Status.SUCCEEDED,
                    Status.FAILED, Status.KILLED);
            LOG.info("Resumed job " + jobId + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void kill(String cluster, String jobId) throws FalconException {
        try {
            OozieClientFactory.get(cluster).kill(jobId);
            assertStatus(cluster, jobId, Status.KILLED, Status.SUCCEEDED,
                    Status.FAILED);
            LOG.info("Killed job " + jobId + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void change(String cluster, String jobId, String changeValue)
        throws FalconException {
        try {
            OozieClientFactory.get(cluster).change(jobId, changeValue);
            LOG.info("Changed bundle/coord " + jobId + ": " + changeValue
                    + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new FalconException(e);
        }
    }

    private void change(String cluster, String id, int concurrency,
                        Date endTime, String pauseTime) throws FalconException {
        StringBuilder changeValue = new StringBuilder();
        changeValue.append(OozieClient.CHANGE_VALUE_CONCURRENCY).append("=")
                .append(concurrency).append(";");
        if (endTime != null) {
            String endTimeStr = SchemaHelper.formatDateUTC(endTime);
            changeValue.append(OozieClient.CHANGE_VALUE_ENDTIME).append("=")
                    .append(endTimeStr).append(";");
        }
        if (pauseTime != null) {
            changeValue.append(OozieClient.CHANGE_VALUE_PAUSETIME).append("=")
                    .append(pauseTime);
        }

        String changeValueStr = changeValue.toString();
        if (changeValue.toString().endsWith(";")) {
            changeValueStr = changeValue.substring(0,
                    changeValueStr.length() - 1);
        }

        change(cluster, id, changeValueStr);

        // assert that its really changed
        try {
            ProxyOozieClient client = OozieClientFactory.get(cluster);
            CoordinatorJob coord = client.getCoordJobInfo(id);
            for (int counter = 0; counter < 3; counter++) {
                Date intendedPauseTime = (StringUtils.isEmpty(pauseTime) ? null
                        : SchemaHelper.parseDateUTC(pauseTime));
                if (coord.getConcurrency() != concurrency
                        || (endTime != null && !coord.getEndTime().equals(endTime))
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
            LOG.error("Failed to change coordinator. Current value "
                    + coord.getConcurrency() + ", "
                    + SchemaHelper.formatDateUTC(coord.getEndTime()) + ", "
                    + SchemaHelper.formatDateUTC(coord.getPauseTime()));
            throw new FalconException("Failed to change coordinator " + id
                    + " with change value " + changeValueStr);
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
    public InstancesResult getJobDetails(String cluster, String jobId)
        throws FalconException {
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
            instances[0] = instance;
            return new InstancesResult("Instance for workflow id:" + jobId,
                    instances);
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }
}

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

package org.apache.ivory.workflow.engine;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.transaction.TransactionManager;
import org.apache.ivory.update.UpdateHelper;
import org.apache.ivory.workflow.OozieWorkflowBuilder;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.ivory.workflow.engine.OozieWorkflowEngineAction.Action;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.oozie.util.XConfiguration;

/**
 * Workflow engine which uses oozies APIs
 * 
 */
@SuppressWarnings("unchecked")
public class OozieWorkflowEngine implements WorkflowEngine {

    private static final Logger LOG = Logger.getLogger(OozieWorkflowEngine.class);

    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    public static final String ENGINE = "oozie";
    private static final BundleJob MISSING = new NullBundleJob();

    private static final WorkflowEngineActionListener listener = new OozieHouseKeepingService();
    private static final String NOT_STARTED = "WAITING";

    private static List<Status> WF_KILL_PRECOND = Arrays.asList(Status.PREP, Status.RUNNING, Status.SUSPENDED, Status.FAILED);
    private static List<Status> WF_SUSPEND_PRECOND = Arrays.asList(Status.RUNNING);
    private static List<Status> WF_RESUME_PRECOND = Arrays.asList(Status.SUSPENDED);
    private static List<Status> WF_RERUN_PRECOND = Arrays.asList(Status.FAILED, Status.KILLED, Status.SUCCEEDED);

    private static List<Job.Status> BUNDLE_ACTIVE_STATUS = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING, Job.Status.SUSPENDED,
            Job.Status.PREPSUSPENDED);
    private static List<Job.Status> BUNDLE_SUSPENDED_STATUS = Arrays.asList(Job.Status.PREPSUSPENDED, Job.Status.SUSPENDED);
    private static List<Job.Status> BUNDLE_RUNNING_STATUS = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING);
    private static List<Job.Status> BUNDLE_KILLED_STATUS = Arrays.asList(Job.Status.KILLED);

    private static List<Job.Status> BUNDLE_KILL_PRECOND = BUNDLE_ACTIVE_STATUS;
    private static List<Job.Status> BUNDLE_SUSPEND_PRECOND = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING,
            Job.Status.DONEWITHERROR);
    private static List<Job.Status> BUNDLE_RESUME_PRECOND = Arrays.asList(Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED);

    @Override
    public void schedule(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>) newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>) newFlows.get(WorkflowBuilder.CLUSTERS);

        for (int index = 0; index < workflowProps.size(); index++) {
            run(clusters.get(index), workflowProps.get(index), entity);
        }
    }

    @Override
    public String dryRun(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>) newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>) newFlows.get(WorkflowBuilder.CLUSTERS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (int index = 0; index < workflowProps.size(); index++) {
                OozieClient client = OozieClientFactory.get(clusters.get(index));
                String result = client.dryrun(workflowProps.get(0));
                buffer.append(result).append(',');
            }
        } catch (OozieClientException e) {
            throw new IvoryException("Unable to schedule workflows", e);
        }
        return buffer.toString();
    }

    @Override
    public boolean isActive(Entity entity) throws IvoryException {
        return isBundleInState(entity, BundleStatus.ACTIVE);
    }

    @Override
    public boolean isSuspended(Entity entity) throws IvoryException {
        return isBundleInState(entity, BundleStatus.SUSPENDED);
    }

    @Override
    public boolean isRunning(Entity entity) throws IvoryException {
        return isBundleInState(entity, BundleStatus.RUNNING);
    }

    private enum BundleStatus {
        ACTIVE, RUNNING, SUSPENDED
    }

    private boolean isBundleInState(Entity entity, BundleStatus status) throws IvoryException {
        Map<Cluster, BundleJob> bundles = findLatestBundle(entity);
        for (BundleJob bundle : bundles.values()) {
            if (bundle == MISSING) // There is no active bundle
                return false;

            switch (status) {
                case ACTIVE:
                    if (!BUNDLE_ACTIVE_STATUS.contains(bundle.getStatus()))
                        return false;
                    break;

                case RUNNING:
                    if (!BUNDLE_RUNNING_STATUS.contains(bundle.getStatus()))
                        return false;
                    break;

                case SUSPENDED:
                    if (!BUNDLE_SUSPENDED_STATUS.contains(bundle.getStatus()))
                        return false;
                    break;
            }
        }
        return true;
    }

    private Map<Cluster, List<BundleJob>> findActiveBundles(Entity entity) throws IvoryException {
        try {
            WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String name = entity.getWorkflowName();

            Cluster[] clusters = builder.getScheduledClustersFor(entity);
            Map<Cluster, List<BundleJob>> jobArray = new HashMap<Cluster, List<BundleJob>>();

            for (Cluster cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<BundleJob> jobsForCluster = new ArrayList<BundleJob>();
                List<BundleJob> jobs = client.getBundleJobsInfo(OozieClient.FILTER_NAME + "=" + name + ";", 0, 100);
                if (jobs != null)
                    for (BundleJob job : jobs) {
                        if (job.getStatus() != Job.Status.KILLED)
                            jobsForCluster.add(job);
                    }
                jobArray.put(cluster, jobsForCluster);
            }
            return jobArray;
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private Map<Cluster, BundleJob> findLatestBundle(Entity entity) throws IvoryException {
        Map<Cluster, List<BundleJob>> bundlesMap = findActiveBundles(entity);
        Map<Cluster, BundleJob> returnMap = new HashMap<Cluster, BundleJob>();
        for (Cluster cluster : bundlesMap.keySet()) {
            List<BundleJob> bundles = bundlesMap.get(cluster);
            if (bundles.isEmpty())
                returnMap.put(cluster, MISSING);
            else { // Find latest bundle
                BundleJob latestBundle = null;
                for (BundleJob bundle : bundles)
                    if (latestBundle == null || bundle.getCreatedTime().after(latestBundle.getCreatedTime()))
                        latestBundle = bundle;
                returnMap.put(cluster, latestBundle);
            }
        }
        return returnMap;
    }

    @Override
    public String suspend(Entity entity) throws IvoryException {
        return doBundleAction(entity, BundleAction.SUSPEND);
    }

    @Override
    public String resume(Entity entity) throws IvoryException {
        return doBundleAction(entity, BundleAction.RESUME);
    }

    @Override
    public String delete(Entity entity) throws IvoryException {
        return doBundleAction(entity, BundleAction.KILL);
    }

    private enum BundleAction {
        SUSPEND, RESUME, KILL
    }

    private String doBundleAction(Entity entity, BundleAction action) throws IvoryException {
        boolean success = true;
        Map<Cluster, List<BundleJob>> jobsMap = findActiveBundles(entity);
        for (Cluster cluster : jobsMap.keySet()) {
            List<BundleJob> jobs = jobsMap.get(cluster);
            if (jobs.isEmpty()) {
                LOG.warn("No active job found for " + entity.getName());
                success = false;
                break;
            }

            for (BundleJob job : jobs) {
                switch (action) {
                    case SUSPEND:
                        // not already suspended and preconditions are true
                        if (!BUNDLE_SUSPENDED_STATUS.contains(job.getStatus()) && BUNDLE_SUSPEND_PRECOND.contains(job.getStatus())) {
                            suspend(cluster, job.getId(), entity);
                            success = true;
                        }
                        break;

                    case RESUME:
                        // not already running and preconditions are true
                        if (!BUNDLE_RUNNING_STATUS.contains(job.getStatus()) && BUNDLE_RESUME_PRECOND.contains(job.getStatus())) {
                            resume(cluster, job.getId(), entity);
                            success = true;
                        }
                        break;

                    case KILL:
                        // not already killed and preconditions are true
                        if (!BUNDLE_KILLED_STATUS.contains(job.getStatus()) && BUNDLE_KILL_PRECOND.contains(job.getStatus())) {
                            kill(cluster, job.getId(), entity);
                            success = true;
                        }
                        break;
                }
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }

    @Override
    public Map<String, Set<String>> getRunningInstances(Entity entity) throws IvoryException {
        Map<String, Set<String>> runInstancesMap = new HashMap<String, Set<String>>();

        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        Cluster[] clusters = builder.getScheduledClustersFor(entity);
        for (Cluster cluster : clusters) {
            Set<String> runInstances = new HashSet<String>();
            List<WorkflowJob> wfs = getRunningWorkflows(cluster);
            if (wfs != null) {
                for (WorkflowJob wf : wfs) {
                    if (StringUtils.isEmpty(wf.getExternalId()))
                        continue;
                    ExternalId extId = new ExternalId(wf.getExternalId());
                    if (extId.getName().equals(entity.getName()))
                        runInstances.add(extId.getDateAsString());
                }
            }
            runInstancesMap.put(cluster.getName(), runInstances);
        }
        return runInstancesMap;
    }

    @Override
    public Map<String, Map<String, String>> killInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.KILL, entity, start, end);
    }

    @Override
    public Map<String, Map<String, String>> reRunInstances(Entity entity, Date start, Date end, Properties props)
            throws IvoryException {
        return doJobAction(JobAction.RERUN, entity, start, end, props);
    }

    @Override
    public Map<String, Map<String, String>> suspendInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.SUSPEND, entity, start, end);
    }

    @Override
    public Map<String, Map<String, String>> resumeInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.RESUME, entity, start, end);
    }

    @Override
    public Map<String, Map<String, String>> getStatus(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.STATUS, entity, start, end);
    }

    private static enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS
    }

    private Map<String, Map<String, String>> doJobAction(JobAction action, Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(action, entity, start, end, null);
    }

    private Map<String, Map<String, String>> doJobAction(JobAction action, Entity entity, Date start, Date end, Properties props)
            throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        Cluster[] clusters = builder.getScheduledClustersFor(entity);
        Map<String, Map<String, String>> instMap = new HashMap<String, Map<String, String>>();

        try {
            for (Cluster cluster : clusters) {
                List<ExternalId> extIds = builder.getExternalIds(entity, cluster.getName(), start, end);
                Map<String, String> instStatusMap = new HashMap<String, String>();
                OozieClient client = OozieClientFactory.get(cluster);

                for (ExternalId extId : extIds) {
                    String jobId = client.getJobId(extId.getId());
                    String status = NOT_STARTED;
                    if (StringUtils.isNotEmpty(jobId)) {
                        WorkflowJob jobInfo = client.getJobInfo(jobId);
                        status = jobInfo.getStatus().name();

                        switch (action) {
                            case KILL:
                                if (!WF_KILL_PRECOND.contains(jobInfo.getStatus()))
                                    break;

                                kill(cluster, jobId);
                                status = Status.KILLED.name();
                                break;

                            case RERUN:
                                if (!WF_RERUN_PRECOND.contains(jobInfo.getStatus()))
                                    break;

                                reRun(cluster, jobId, props);
                                status = Status.RUNNING.name();
                                break;

                            case SUSPEND:
                                if (!WF_SUSPEND_PRECOND.contains(jobInfo.getStatus()))
                                    break;

                                suspend(cluster, jobId);
                                status = Status.SUSPENDED.name();
                                break;

                            case RESUME:
                                if (!WF_RESUME_PRECOND.contains(jobInfo.getStatus()))
                                    break;

                                resume(cluster, jobId);
                                status = Status.RUNNING.name();
                                break;

                            case STATUS:
                                break;
                        }
                    }

                    instStatusMap.put(extId.getDateAsString(), consolidateStatus(status, instStatusMap.get(extId.getDateAsString())));
                }
                instMap.put(cluster.getName(), instStatusMap);
            }
        } catch (Exception e) {
            throw new IvoryException(e);
        }
        return instMap;
    }

    private String consolidateStatus(String newStatus, String oldStatus) {
        if (oldStatus == null || oldStatus.equals(NOT_STARTED)
                || (oldStatus.equals(Status.SUCCEEDED.name()) && !newStatus.equals(NOT_STARTED)))
            return newStatus;
        return oldStatus;
    }

    @Override
    public void update(Entity oldEntity, Entity newEntity) throws IvoryException {
        Map<Cluster, BundleJob> bundleMap = findLatestBundle(oldEntity);
        OozieWorkflowBuilder<Entity> builder = (OozieWorkflowBuilder<Entity>) WorkflowBuilder.getBuilder(ENGINE, oldEntity);

        LOG.info("Updating entity through Workflow Engine" + newEntity.toShortString());
        for (Map.Entry<Cluster, BundleJob> entry : bundleMap.entrySet()) {
            if (entry.getValue() == MISSING)
                continue;

            Cluster cluster = entry.getKey();
            BundleJob bundle = entry.getValue();
            String clusterName = cluster.getName();
            LOG.debug("Updating for cluster : " + clusterName + ", bundle: " + bundle.getId());

            int oldConcurrency = builder.getConcurrency(oldEntity);
            int newConcurrency = builder.getConcurrency(newEntity);
            String oldEndTime = builder.getEndTime(oldEntity, clusterName);
            String newEndTime = builder.getEndTime(newEntity, clusterName);

            if (oldConcurrency != newConcurrency || !oldEndTime.equals(newEndTime)) {
                Entity clonedOldEntity = oldEntity.clone();
                builder.setConcurrency(clonedOldEntity, newConcurrency);
                Date endTime = EntityUtil.parseDateUTC(newEndTime);
                builder.setEndTime(clonedOldEntity, clusterName, endTime);
                if (clonedOldEntity.deepEquals(newEntity)) {
                    // only concurrency and endtime are changed. So, change
                    // coords
                    LOG.info("Change operation is adequate! : " + clusterName + ", bundle: " + bundle.getId());

                    List<CoordinatorJob> coords = getBundleInfo(cluster, bundle.getId()).getCoordinators();

                    // Find default coord's start time(min start time)
                    Date minCoordStartTime = null;
                    for (CoordinatorJob coord : coords) {
                        if (minCoordStartTime == null || minCoordStartTime.after(coord.getStartTime()))
                            minCoordStartTime = coord.getStartTime();
                    }
                    
                    for (CoordinatorJob coord : coords) {
                        // Add offset to end time for late coords
                        Date localEndTime = addOffest(endTime, minCoordStartTime, coord.getStartTime());
                        //if end time < last action time, use pause time to delete future actions
                        if (coord.getLastActionTime() != null && localEndTime.before(coord.getLastActionTime())) {
                            change(cluster, coord.getId(), newConcurrency, coord.getLastActionTime(), EntityUtil.formatDateUTC(localEndTime));
                            change(cluster, coord.getId(), newConcurrency, localEndTime, "");
                        } else
                            change(cluster, coord.getId(), newConcurrency, localEndTime, null);
                    }
                    return;
                }
            }

            LOG.debug("Going to update ! : " + newEntity.toShortString() + clusterName + ", bundle: " + bundle.getId());
            updateInternal(oldEntity, newEntity, cluster, bundle);
            LOG.info("Entity update complete : " + newEntity.toShortString() + clusterName + ", bundle: " + bundle.getId());
        }

        Set<Entity> affectedEntities = EntityGraph.get().getDependents(oldEntity);
        for (Entity entity : affectedEntities) {
            if (entity.getEntityType() != EntityType.PROCESS)
                continue;
            LOG.info("Dependent entities need to be updated " + entity.toShortString());
            if (!UpdateHelper.shouldUpdate(oldEntity, newEntity, entity))
                continue;
            Map<Cluster, BundleJob> processBundles = findLatestBundle(entity);
            for (Map.Entry<Cluster, BundleJob> processBundle : processBundles.entrySet()) {
                if (processBundle.getValue() == MISSING)
                    continue;
                LOG.info("Triggering update for " + processBundle.getKey().getName() + ", " + processBundle.getValue().getId());
                updateInternal(entity, entity, processBundle.getKey(), processBundle.getValue());
            }
        }
    }

    private void updateInternal(Entity oldEntity, Entity newEntity, Cluster cluster, BundleJob bundle) throws IvoryException {

        OozieWorkflowBuilder<Entity> builder = (OozieWorkflowBuilder<Entity>) WorkflowBuilder.getBuilder(ENGINE, oldEntity);
        String clusterName = cluster.getName();

        Date newEndDate = EntityUtil.parseDateUTC(builder.getEndTime(newEntity, clusterName));
        if (newEndDate.before(new Date())) {
            throw new IvoryException("New end time for " + newEntity.getName()
                    + " is past current time. Entity can't be updated. Use remove and add");
        }

        // Change end time of coords and schedule new bundle

        // suspend so that no new coord actions are created
        suspend(cluster, bundle.getId(), oldEntity);
        List<CoordinatorJob> coords = getBundleInfo(cluster, bundle.getId()).getCoordinators();

        // Find default coord's start time(min start time)
        Date minCoordStartTime = null;
        for (CoordinatorJob coord : coords) {
            if (minCoordStartTime == null || minCoordStartTime.after(coord.getStartTime()))
                minCoordStartTime = coord.getStartTime();
        }

        // Pause time should be > now in oozie. So, add offset to pause time to
        // account for time difference between ivory host and oozie host
        // set to the next minute. Since time is rounded off, it will be always
        // less than oozie server time
        // ensure that we are setting it to the next minute.
        Date endTime = new Date(System.currentTimeMillis() + 70000);
        Date newStartTime = null;

        for (CoordinatorJob coord : coords) {
            // Add offset to pause time for late coords
            Date localEndTime = addOffest(endTime, minCoordStartTime, coord.getStartTime());

            // Set pause time to now so that future coord actions are deleted
            change(cluster, coord.getId(), null, null, EntityUtil.formatDateUTC(localEndTime));

            // Change end time and reset pause time
            change(cluster, coord.getId(), null, localEndTime, "");

            // calculate start time for updated entity as next schedule time
            // after end date
            Date localNewStartTime = builder.getNextStartTime(oldEntity, clusterName, localEndTime);
            if (newStartTime == null || newStartTime.after(localNewStartTime)) {
                newStartTime = localNewStartTime;
            }
        }
        resume(cluster, bundle.getId(), oldEntity);

        // schedule new entity
        Entity schedEntity = newEntity.clone();
        builder.setStartDate(schedEntity, clusterName, newStartTime);
        schedule(schedEntity);
    }

    private Date addOffest(Date target, Date globalTime, Date localTime) {
        long offset = localTime.getTime() - globalTime.getTime();
        return new Date(target.getTime() + offset);
    }

    private BundleJob getBundleInfo(Cluster cluster, String bundleId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getBundleJobInfo(bundleId);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    // TODO just returns first 1000
    private List<WorkflowJob> getRunningWorkflows(Cluster cluster) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getJobsInfo("status=" + Job.Status.RUNNING, 1, 1000);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public static void suspend(Cluster cluster, String id) throws IvoryException {
        suspend(cluster, id, null);
    }

    public static void resume(Cluster cluster, String id) throws IvoryException {
        resume(cluster, id, null);
    }

    public static void kill(Cluster cluster, String id) throws IvoryException {
        kill(cluster, id, null);
    }

    public static void reRun(Cluster cluster, String jobId, Properties props) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);

        try {
            WorkflowJob jobInfo = client.getJobInfo(jobId);
            Properties jobprops = new XConfiguration(new StringReader(jobInfo.getConf())).toProperties();
            if (props == null || props.isEmpty())
                jobprops.put(OozieClient.RERUN_FAIL_NODES, "false");
            else
                for (Entry<Object, Object> entry : props.entrySet()) {
                    jobprops.put(entry.getKey(), entry.getValue());
                }
            jobprops.remove(OozieClient.COORDINATOR_APP_PATH);
            jobprops.remove(OozieClient.BUNDLE_APP_PATH);
            client.reRun(jobId, jobprops);
            LOG.info("Rerun job " + jobId + " on cluster " + cluster.getName());
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.RUN, cluster, jobId, null));
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    public static String run(Cluster cluster, Properties props, Entity entity) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        if (entity != null)
            listener.beforeSchedule(cluster, entity);
        try {
            String jobId = client.run(props);
            LOG.info("Submitted " + jobId + " on cluster " + cluster.getName() + " with properties : " + props);
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.RUN, cluster, jobId, entity));
            return jobId;
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new IvoryException("Unable to schedule workflows", e);
        }
    }

    public static void suspend(Cluster cluster, String jobId, Entity entity) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            if (entity != null)
                listener.beforeSuspend(cluster, entity);
            client.suspend(jobId);
            LOG.info("Suspended job " + jobId + " on cluster " + cluster.getName());
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.SUSPEND, cluster, jobId, entity));
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public static void resume(Cluster cluster, String jobId, Entity entity) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            if (entity != null)
                listener.beforeResume(cluster, entity);
            client.resume(jobId);
            LOG.info("Resumed job " + jobId + " on cluster " + cluster.getName());
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.RESUME, cluster, jobId, entity));
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public static void kill(Cluster cluster, String jobId, Entity entity) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            if (entity != null)
                listener.beforeDelete(cluster, entity);
            client.kill(jobId);
            LOG.info("Killed job " + jobId + " on cluster " + cluster.getName());
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.KILL, cluster, jobId, entity));
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public static void change(Cluster cluster, String jobId, String changeValue) throws IvoryException {
        try {
            OozieWorkflowEngineAction action = new OozieWorkflowEngineAction(Action.CHANGE, cluster, jobId);
            action.preparePayload();
            OozieClient client = OozieClientFactory.get(cluster);
            client.change(jobId, changeValue);
            TransactionManager.performAction(action);
            LOG.info("Changed bundle/coord " + jobId + ": " + changeValue);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void change(Cluster cluster, String id, Integer concurrency, Date endTime, String pauseTime) throws IvoryException {
        StringBuilder changeValue = new StringBuilder();
        if (concurrency != null)
            changeValue.append(OozieClient.CHANGE_VALUE_CONCURRENCY).append("=").append(concurrency.intValue()).append(";");
        if (endTime != null) {
            String endTimeStr = EntityUtil.formatDateUTC(endTime);
            changeValue.append(OozieClient.CHANGE_VALUE_ENDTIME).append("=").append(endTimeStr).append(";");
        }
        if (pauseTime != null)
            changeValue.append(OozieClient.CHANGE_VALUE_PAUSETIME).append("=").append(pauseTime);

        String changeValueStr = changeValue.toString();
        if (changeValue.toString().endsWith(";"))
            changeValueStr = changeValue.substring(0, changeValueStr.length() - 1);

        change(cluster, id, changeValueStr);
    }
}
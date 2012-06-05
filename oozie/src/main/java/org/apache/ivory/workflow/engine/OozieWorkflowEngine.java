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

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.common.TimeUnit;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.resource.InstancesResult.Instance;
import org.apache.ivory.resource.InstancesResult.WorkflowStatus;
import org.apache.ivory.update.UpdateHelper;
import org.apache.ivory.util.OozieUtils;
import org.apache.ivory.workflow.OozieWorkflowBuilder;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;
import org.apache.oozie.client.WorkflowJob.Status;

import java.util.*;
import java.util.Map.Entry;

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
    private static final String IVORY_INSTANCE_ACTION_CLUSTERS = "ivory.instance.action.clusters";

    @Override
    public void schedule(Entity entity) throws IvoryException {
        Map<String, BundleJob> bundleMap = findBundle(entity);
        List<String> schedClusters = new ArrayList<String>();
        for (String cluster : bundleMap.keySet()) {
            if (bundleMap.get(cluster) == MISSING)
                schedClusters.add(cluster);
        }

        if (!schedClusters.isEmpty()) {
            WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            Map<String, Properties> newFlows = builder.newWorkflowSchedule(entity, schedClusters);
            for (String cluster : newFlows.keySet()) {
                LOG.info("Scheduling " + entity.toShortString() + " on cluster " + cluster);
                scheduleEntity(cluster, newFlows.get(cluster), entity);
            }
        }
    }

    @Override
    public boolean isActive(Entity entity) throws IvoryException {
        return isBundleInState(entity, BundleStatus.ACTIVE);
    }

    @Override
    public boolean isSuspended(Entity entity) throws IvoryException {
        return isBundleInState(entity, BundleStatus.SUSPENDED);
    }

    private enum BundleStatus {
        ACTIVE, RUNNING, SUSPENDED
    }

    private boolean isBundleInState(Entity entity, BundleStatus status) throws IvoryException {
        Map<String, BundleJob> bundles = findLatestBundle(entity);
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

    private Map<String, BundleJob> findBundle(Entity entity) throws IvoryException {
        WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        String[] clusters = builder.getClustersDefined(entity);
        Map<String, BundleJob> jobMap = new HashMap<String, BundleJob>();
        for (String cluster : clusters) {
            jobMap.put(cluster, findBundle(entity, cluster));
        }
        return jobMap;
    }

    private BundleJob findBundle(Entity entity, String cluster) throws IvoryException {
        String stPath = EntityUtil.getStagingPath(entity);
        List<BundleJob> bundles = findBundles(entity, cluster);
        for (BundleJob job : bundles)
            if (job.getAppPath().endsWith(stPath))
                return job;
        return MISSING;
    }

    private List<BundleJob> findBundles(Entity entity, String cluster) throws IvoryException {
        try {
            OozieClient client = OozieClientFactory.get(cluster);
            List<BundleJob> jobs = client.getBundleJobsInfo(OozieClient.FILTER_NAME + "=" + EntityUtil.getWorkflowName(entity) + ";",
                    0, 100);
            if (jobs != null)
                return jobs;
            return new ArrayList<BundleJob>();
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private Map<String, List<BundleJob>> findBundles(Entity entity) throws IvoryException {
        WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        String[] clusters = builder.getClustersDefined(entity);
        Map<String, List<BundleJob>> jobMap = new HashMap<String, List<BundleJob>>();

        for (String cluster : clusters) {
            jobMap.put(cluster, findBundles(entity, cluster));
        }
        return jobMap;
    }

    //During update, a new bundle may not be created if next start time >= end time
    //In this case, there will not be a bundle with the latest entity md5
    //So, pick last created bundle
    private Map<String, BundleJob> findLatestBundle(Entity entity) throws IvoryException {
        Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
        Map<String, BundleJob> bundleMap = new HashMap<String, BundleJob>();
        for(String cluster:bundlesMap.keySet()) {
            Date latest = null;
            for(BundleJob job:bundlesMap.get(cluster))
                if(latest == null || latest.before(job.getCreatedTime())) {
                    bundleMap.put(cluster, job);
                    latest = job.getCreatedTime();
                }
        }
        return bundleMap;
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
        Map<String, List<BundleJob>> jobsMap = findBundles(entity);
        for (String cluster : jobsMap.keySet()) {
            List<BundleJob> jobs = jobsMap.get(cluster);
            if (jobs.isEmpty()) {
                LOG.warn("No active job found for " + entity.getName());
                success = false;
                break;
            }

            beforeAction(entity, action, cluster);
            for (BundleJob job : jobs) {
                switch (action) {
                    case SUSPEND:
                        // not already suspended and preconditions are true
                        if (!BUNDLE_SUSPENDED_STATUS.contains(job.getStatus()) && BUNDLE_SUSPEND_PRECOND.contains(job.getStatus())) {
                            suspend(cluster, job.getId());
                            success = true;
                        }
                        break;

                    case RESUME:
                        // not already running and preconditions are true
                        if (!BUNDLE_RUNNING_STATUS.contains(job.getStatus()) && BUNDLE_RESUME_PRECOND.contains(job.getStatus())) {
                            resume(cluster, job.getId());
                            success = true;
                        }
                        break;

                    case KILL:
                        // not already killed and preconditions are true
                        if (!BUNDLE_KILLED_STATUS.contains(job.getStatus()) && BUNDLE_KILL_PRECOND.contains(job.getStatus())) {
                            kill(cluster, job.getId());
                            success = true;
                        }
                        break;
                }
                afterAction(entity, action, cluster);
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }

    private void beforeAction(Entity entity, BundleAction action, String cluster) throws IvoryException {
        switch(action) {
            case SUSPEND:
                listener.beforeSuspend(entity, cluster);
                break;
                
            case RESUME:
                listener.beforeResume(entity, cluster);
                break;
                
            case KILL:
                listener.beforeDelete(entity, cluster);
                break;
        }
    }

    private void afterAction(Entity entity, BundleAction action, String cluster) throws IvoryException {
        switch(action) {
            case SUSPEND:
                listener.afterSuspend(entity, cluster);
                break;
                
            case RESUME:
                listener.afterResume(entity, cluster);
                break;
                
            case KILL:
                listener.afterDelete(entity, cluster);
                break;
        }
    }

    @Override
    public InstancesResult getRunningInstances(Entity entity) throws IvoryException {
        try {
            WorkflowBuilder<Entity> builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String[] clusters = builder.getClustersDefined(entity);
            List<Instance> runInstances = new ArrayList<Instance>();
            String[] wfNames = builder.getWorkflowNames(entity);
            List<String> coordNames = new ArrayList<String>();
            for (String wfName : wfNames) {
                if (EntityUtil.getWorkflowName(Tag.RETENTION, entity).toString().equals(wfName))
                    continue;
                coordNames.add(wfName);
            }

            for (String cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<WorkflowJob> wfs = getRunningWorkflows(cluster, coordNames);
                if (wfs != null) {
                    for (WorkflowJob job : wfs) {
                        WorkflowJob wf = client.getJobInfo(job.getId());
                        if (StringUtils.isEmpty(wf.getParentId()))
                            continue;

                        CoordinatorAction action = client.getCoordActionInfo(wf.getParentId());
                        String nominalTimeStr = EntityUtil.formatDateUTC(action.getNominalTime());
                        Instance instance = new Instance(cluster, nominalTimeStr, WorkflowStatus.RUNNING);
                        instance.startTime = EntityUtil.formatDateUTC(wf.getStartTime());
                        runInstances.add(instance);
                    }
                }
            }
            return new InstancesResult("Running Instances", runInstances.toArray(new Instance[runInstances.size()]));

        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    @Override
    public InstancesResult killInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException {
        return doJobAction(JobAction.KILL, entity, start, end, props);
    }

    @Override
    public InstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException {
        return doJobAction(JobAction.RERUN, entity, start, end, props);
    }

    @Override
    public InstancesResult suspendInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException {
        return doJobAction(JobAction.SUSPEND, entity, start, end, props);
    }

    @Override
    public InstancesResult resumeInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException {
        return doJobAction(JobAction.RESUME, entity, start, end, props);
    }

    @Override
    public InstancesResult getStatus(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.STATUS, entity, start, end, null);
    }

    private static enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS
    }

    private WorkflowJob getWorkflowInfo(String cluster, String wfId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getJobInfo(wfId);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private InstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end, Properties props)
            throws IvoryException {

        Map<String, List<CoordinatorAction>> actionsMap = getCoordActions(entity, start, end);
        String clusters = props == null ? "" : props.getProperty(IVORY_INSTANCE_ACTION_CLUSTERS, "");
        List<String> clusterList = new ArrayList<String>();
        if (StringUtils.isEmpty(clusters)) {
            clusterList.addAll(actionsMap.keySet());
        }

        List<Instance> instances = new ArrayList<Instance>();
        for (String cluster : actionsMap.keySet()) {
            if (!clusterList.contains(cluster))
                continue;

            List<CoordinatorAction> actions = actionsMap.get(cluster);

            for (CoordinatorAction coordinatorAction : actions) {
                String status;
                WorkflowJob jobInfo = null;
                if (coordinatorAction.getExternalId() != null) {
                    jobInfo = getWorkflowInfo(cluster, coordinatorAction.getExternalId());
                }
                if (jobInfo != null) {
                    status = jobInfo.getStatus().name();
                    switch (action) {
                        case KILL:
                            if (!WF_KILL_PRECOND.contains(jobInfo.getStatus()))
                                break;

                            kill(cluster, jobInfo.getId());
                            status = Status.KILLED.name();
                            break;

                        case SUSPEND:
                            if (!WF_SUSPEND_PRECOND.contains(jobInfo.getStatus()))
                                break;

                            suspend(cluster, jobInfo.getId());
                            status = Status.SUSPENDED.name();
                            break;

                        case RESUME:
                            if (!WF_RESUME_PRECOND.contains(jobInfo.getStatus()))
                                break;

                            resume(cluster, jobInfo.getId());
                            status = Status.RUNNING.name();
                            break;

                        case RERUN:
                            if (!WF_RERUN_PRECOND.contains(jobInfo.getStatus()))
                                break;

                            reRun(cluster, jobInfo.getId(), props);
                            status = Status.RUNNING.name();
                            break;

                        case STATUS:
                            break;
                    }
                    if (action != OozieWorkflowEngine.JobAction.STATUS && coordinatorAction.getExternalId() != null) {
                        jobInfo = getWorkflowInfo(cluster, coordinatorAction.getExternalId());
                    }

                    String nominalTimeStr = EntityUtil.formatDateUTC(coordinatorAction.getNominalTime());
                    InstancesResult.Instance instance = new InstancesResult.Instance(cluster, nominalTimeStr,
                            WorkflowStatus.valueOf(status));
                    if (jobInfo != null) {
                        instance.startTime = EntityUtil.formatDateUTC(jobInfo.getStartTime());
                        instance.endTime = EntityUtil.formatDateUTC(jobInfo.getEndTime());
                    }
                    instances.add(instance);
                }
            }
        }
        return new InstancesResult(action.name(), instances.toArray(new Instance[instances.size()]));
    }

    protected Map<String, List<CoordinatorAction>> getCoordActions(Entity entity, Date start, Date end) throws IvoryException {
        Map<String, List<BundleJob>> bundlesMap = findBundles(entity);
        Map<String, List<CoordinatorAction>> actionsMap = new HashMap<String, List<CoordinatorAction>>();

        for (String cluster : bundlesMap.keySet()) {
            List<BundleJob> bundles = bundlesMap.get(cluster);
            OozieClient client = OozieClientFactory.get(cluster);
            List<CoordinatorJob> applicableCoords = getApplicableCoords(entity, client, start, end, bundles);
            List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();

            for (CoordinatorJob coord : applicableCoords) {
                TimeUnit timeUnit = TimeUnit.valueOf(coord.getTimeUnit().name());
                Date iterStart = EntityUtil.getNextStartTime(coord.getStartTime(), timeUnit, coord.getFrequency(),
                        coord.getTimeZone(), start);
                final Date iterEnd = (coord.getEndTime().before(end) ? coord.getEndTime() : end);
                while (!iterStart.after(iterEnd)) {
                    int sequence = EntityUtil.getInstanceSequence(coord.getStartTime(), timeUnit, coord.getFrequency(),
                            coord.getTimeZone(), iterStart);
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
                    startCal.add(timeUnit.getCalendarUnit(), coord.getFrequency());
                    iterStart = startCal.getTime();
                }
            }
            actionsMap.put(cluster, actions);
        }
        return actionsMap;
    }

    private List<CoordinatorJob> getApplicableCoords(Entity entity, OozieClient client, Date start, Date end, List<BundleJob> bundles)
            throws IvoryException {
        List<CoordinatorJob> applicableCoords = new ArrayList<CoordinatorJob>();
        try {
            for (BundleJob bundle : bundles) {
                List<CoordinatorJob> coords = client.getBundleJobInfo(bundle.getId()).getCoordinators();
                for (CoordinatorJob coord : coords) {
                    String coordName = EntityUtil.getWorkflowName(Tag.RETENTION, entity).toString();
                    if (coordName.equals(coord.getAppName()))
                        continue;
                    if ((start.compareTo(coord.getStartTime()) >= 0 && start.compareTo(coord.getEndTime()) <= 0)
                            || (end.compareTo(coord.getStartTime()) >= 0 && end.compareTo(coord.getEndTime()) <= 0)) {
                        applicableCoords.add(coord);
                    }
                }
            }
            sortCoordsByStartTime(applicableCoords);
            return applicableCoords;
        } catch (OozieClientException e) {
            throw new IvoryException(e);
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

    private boolean canUpdateBundle(String cluster, Entity oldEntity, Entity newEntity) throws IvoryException {
        int oldConcurrency = EntityUtil.getConcurrency(oldEntity);
        int newConcurrency = EntityUtil.getConcurrency(newEntity);

        Date oldEndTime = EntityUtil.getEndTime(oldEntity, cluster);
        Date newEndTime = EntityUtil.getEndTime(newEntity, cluster);

        if (oldConcurrency != newConcurrency || !oldEndTime.equals(newEndTime)) {
            Entity clonedOldEntity = oldEntity.clone();
            EntityUtil.setConcurrency(clonedOldEntity, newConcurrency);
            EntityUtil.setEndTime(clonedOldEntity, cluster, newEndTime);
            if (clonedOldEntity.deepEquals(newEntity))
                // only concurrency and end time are changed
                return true;
        }
        return false;
    }

    @Override
    public void update(Entity oldEntity, Entity newEntity) throws IvoryException {
        if (!UpdateHelper.shouldUpdate(oldEntity, newEntity))
            return;

        Map<String, BundleJob> bundleMap = findBundle(oldEntity);

        LOG.info("Updating entity through Workflow Engine" + newEntity.toShortString());
        for (Map.Entry<String, BundleJob> entry : bundleMap.entrySet()) {
            if (entry.getValue() == MISSING)
                continue;

            String cluster = entry.getKey();
            BundleJob bundle = entry.getValue();
            Date newEndTime = EntityUtil.getEndTime(newEntity, cluster);
            if (newEndTime.before(now())) {
                throw new IvoryException("New end time for " + newEntity.getName()
                        + " is past current time. Entity can't be updated. Use remove and add");
            }

            LOG.debug("Updating for cluster : " + cluster + ", bundle: " + bundle.getId());

            if (canUpdateBundle(cluster, oldEntity, newEntity)) {
                // only concurrency and endtime are changed. So, change coords
                LOG.info("Change operation is adequate! : " + cluster + ", bundle: " + bundle.getId());
                updateCoords(cluster, bundle.getId(), EntityUtil.getConcurrency(newEntity),
                        EntityUtil.getEndTime(newEntity, cluster), null);
                return;
            }

            LOG.debug("Going to update ! : " + newEntity.toShortString() + cluster + ", bundle: " + bundle.getId());
            updateInternal(oldEntity, newEntity, cluster, bundle);
            LOG.info("Entity update complete : " + newEntity.toShortString() + cluster + ", bundle: " + bundle.getId());
        }

        Set<Entity> affectedEntities = EntityGraph.get().getDependents(oldEntity);
        for (Entity entity : affectedEntities) {
            if (entity.getEntityType() != EntityType.PROCESS)
                continue;
            LOG.info("Dependent entities need to be updated " + entity.toShortString());
            if (!UpdateHelper.shouldUpdate(oldEntity, newEntity, entity))
                continue;
            Map<String, BundleJob> processBundles = findBundle(entity);
            for (Map.Entry<String, BundleJob> processBundle : processBundles.entrySet()) {
                if (processBundle.getValue() == MISSING)
                    continue;
                LOG.info("Triggering update for " + processBundle.getKey() + ", " + processBundle.getValue().getId());
                updateInternal(entity, entity, processBundle.getKey(), processBundle.getValue());
            }
        }
    }

    private Date now() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    private Date getNextMin(Date date) {
        return new Date(date.getTime() + 60 * 1000);
    }

    private Date getPreviousMin(Date date) {
        return new Date(date.getTime() - 60 * 1000);
    }

    private Date getCoordLastActionTime(CoordinatorJob coord) {
        if (coord.getNextMaterializedTime() != null) {
            Calendar cal = Calendar.getInstance(EntityUtil.getTimeZone(coord.getTimeZone()));
            cal.setTime(coord.getLastActionTime());
            cal.add(TimeUnit.valueOf(coord.getTimeUnit().name()).getCalendarUnit(), -1);
            return cal.getTime();
        }
        return null;
    }

    private void updateCoords(String cluster, String bundleId, int concurrency, Date endTime, Job.Status oldBundleStatus)
            throws IvoryException {
        if (endTime.compareTo(now()) <= 0)
            throw new IvoryException("End time " + EntityUtil.formatDateUTC(endTime) + " can't be in the past");

        BundleJob bundle = getBundleInfo(cluster, bundleId);
        if (oldBundleStatus == null)
            oldBundleStatus = bundle.getStatus();
        suspend(cluster, bundleId);

        List<CoordinatorJob> coords = bundle.getCoordinators();
        for (CoordinatorJob coord : coords) {
            LOG.debug("Updating endtime of coord " + coord.getId() + " to " + EntityUtil.formatDateUTC(endTime) + " on cluster "
                    + cluster);
            Date lastActionTime = getCoordLastActionTime(coord);
            if (lastActionTime == null) { // nothing is materialized
                if (endTime.compareTo(coord.getStartTime()) <= 0)
                    change(cluster, coord.getId(), null, coord.getStartTime(), null);
                else
                    change(cluster, coord.getId(), concurrency, endTime, null);
            } else {
                if (endTime.before(lastActionTime)) {
                    Date pauseTime = getPreviousMin(endTime);
                    // set pause time which deletes future actions
                    change(cluster, coord.getId(), null, null, EntityUtil.formatDateUTC(pauseTime));
                }
                change(cluster, coord.getId(), concurrency, endTime, "");
            }
        }

        if (oldBundleStatus != Job.Status.SUSPENDED && oldBundleStatus != Job.Status.PREPSUSPENDED)
            resume(cluster, bundleId);
    }

    private void updateInternal(Entity oldEntity, Entity newEntity, String cluster, BundleJob bundle) throws IvoryException {
        OozieWorkflowBuilder<Entity> builder = (OozieWorkflowBuilder<Entity>) WorkflowBuilder.getBuilder(ENGINE, oldEntity);

        // Change end time of coords and schedule new bundle
        Job.Status oldBundleStatus = bundle.getStatus();
        suspend(cluster, bundle.getId());

        BundleJob newBundle = findBundle(newEntity, cluster);
        Date endTime;
        if (newBundle == MISSING) { // new entity is not scheduled yet
            endTime = getNextMin(getNextMin(now()));
            Date newStartTime = builder.getNextStartTime(newEntity, cluster, endTime);
            Entity clonedNewEntity = newEntity.clone();
            EntityUtil.setStartDate(clonedNewEntity, cluster, newStartTime);
            schedule(clonedNewEntity);
        } else {
            endTime = getMinStartTime(newBundle);
        }
        if (endTime != null)
            updateCoords(cluster, bundle.getId(), EntityUtil.getConcurrency(oldEntity), endTime, oldBundleStatus);
    }

    private Date getMinStartTime(BundleJob bundle) {
        Date startTime = null;
        if (bundle.getCoordinators() != null)
            for (CoordinatorJob coord : bundle.getCoordinators())
                if (startTime == null || startTime.after(coord.getStartTime()))
                    startTime = coord.getStartTime();
        return startTime;
    }

    private BundleJob getBundleInfo(String cluster, String bundleId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getBundleJobInfo(bundleId);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    // TODO just returns first 1000
    private List<WorkflowJob> getRunningWorkflows(String cluster, List<String> wfNames) throws IvoryException {
        StringBuilder filter = new StringBuilder();
        filter.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.RUNNING.name());
        for (String wfName : wfNames)
            filter.append(';').append(OozieClient.FILTER_NAME).append('=').append(wfName);

        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getJobsInfo(filter.toString(), 1, 1000);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    @Override
    public void reRun(String cluster, String jobId, Properties props) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            WorkflowJob jobInfo = client.getJobInfo(jobId);
            Properties jobprops = OozieUtils.toProperties(jobInfo.getConf());
            if (props == null || props.isEmpty())
                jobprops.put(OozieClient.RERUN_FAIL_NODES, "false");
            else
                for (Entry<Object, Object> entry : props.entrySet()) {
                    jobprops.put(entry.getKey(), entry.getValue());
                }
            jobprops.remove(OozieClient.COORDINATOR_APP_PATH);
            jobprops.remove(OozieClient.BUNDLE_APP_PATH);
            client.reRun(jobId, jobprops);
            LOG.info("Rerun job " + jobId + " on cluster " + cluster);
        } catch (Exception e) {
            LOG.error("Unable to rerun workflows", e);
            throw new IvoryException(e);
        }
    }

    @Override
    public String instanceStatus(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            WorkflowJob jobInfo = client.getJobInfo(jobId);
            Status status = jobInfo.getStatus();
            return status.name();
        } catch (Exception e) {
            LOG.error("Unable to get status of workflows", e);
            throw new IvoryException(e);
        }
    }

    private String scheduleEntity(String cluster, Properties props, Entity entity) throws IvoryException {
        listener.beforeSchedule(entity, cluster);
        String jobId = run(cluster, props);
        listener.afterSchedule(entity, cluster);
        return jobId;
    }

    public String run(String cluster, Properties props) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            String jobId = client.run(props);
            LOG.info("Submitted " + jobId + " on cluster " + cluster + " with properties : " + props);
            return jobId;
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new IvoryException("Unable to schedule workflows", e);
        }
    }

    public void suspend(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            client.suspend(jobId);
            LOG.info("Suspended job " + jobId + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public void resume(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            client.resume(jobId);
            LOG.info("Resumed job " + jobId + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public void kill(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            client.kill(jobId);
            LOG.info("Killed job " + jobId + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public void change(String cluster, String jobId, String changeValue) throws IvoryException {
        try {
            OozieClient client = OozieClientFactory.get(cluster);
            client.change(jobId, changeValue);
            LOG.info("Changed bundle/coord " + jobId + ": " + changeValue + " on cluster " + cluster);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void change(String cluster, String id, Integer concurrency, Date endTime, String pauseTime) throws IvoryException {
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
    
    @Override
	public String getWorkflowProperty(String cluster, String jobId,
			String property) throws IvoryException {
		OozieClient client = new CustomOozieClient(cluster);
		try {
			WorkflowJob jobInfo = client.getJobInfo(jobId);
			String conf = jobInfo.getConf();
			Properties props = OozieUtils.toProperties(conf);
			return props.getProperty(property);
		} catch (Exception e) {
			throw new IvoryException(e);
		}
	}
}

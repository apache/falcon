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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Pair;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.common.TimeUnit;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.resource.ProcessInstancesResult;
import org.apache.ivory.resource.ProcessInstancesResult.ProcessInstance;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.ivory.transaction.TransactionManager;
import org.apache.ivory.update.UpdateHelper;
import org.apache.ivory.util.OozieUtils;
import org.apache.ivory.workflow.OozieWorkflowBuilder;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.ivory.workflow.engine.OozieWorkflowEngineAction.Action;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;

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
        if (newFlows == null || newFlows.size() == 0)
            return;

        List<Properties> workflowProps = (List<Properties>) newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>) newFlows.get(WorkflowBuilder.CLUSTERS);

        for (int index = 0; index < workflowProps.size(); index++) {
            scheduleEntity(clusters.get(index).getName(), workflowProps.get(index), entity);
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

    private Map<String, List<BundleJob>> findActiveBundles(Entity entity) throws IvoryException {
        try {
            String name = entity.getWorkflowName();

            WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String[] clusters = builder.getClustersDefined(entity);
            Map<String, List<BundleJob>> jobArray = new HashMap<String, List<BundleJob>>();

            for (String cluster : clusters) {
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

    private Map<String, BundleJob> findLatestBundle(Entity entity) throws IvoryException {
        Map<String, List<BundleJob>> bundlesMap = findActiveBundles(entity);
        Map<String, BundleJob> returnMap = new HashMap<String, BundleJob>();
        for (String cluster : bundlesMap.keySet()) {
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
        Map<String, List<BundleJob>> jobsMap = findActiveBundles(entity);
        for (String cluster : jobsMap.keySet()) {
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
                            suspendEntity(cluster, job.getId());
                            success = true;
                        }
                        break;

                    case RESUME:
                        // not already running and preconditions are true
                        if (!BUNDLE_RUNNING_STATUS.contains(job.getStatus()) && BUNDLE_RESUME_PRECOND.contains(job.getStatus())) {
                            resumeEntity(cluster, job.getId());
                            success = true;
                        }
                        break;

                    case KILL:
                        // not already killed and preconditions are true
                        if (!BUNDLE_KILLED_STATUS.contains(job.getStatus()) && BUNDLE_KILL_PRECOND.contains(job.getStatus())) {
                            killEntity(cluster, job.getId());
                            success = true;
                        }
                        break;
                }
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }

    @Override
    public ProcessInstancesResult getRunningInstances(Entity entity) throws IvoryException {
        try {
            WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String[] clusters = builder.getClustersDefined(entity);
            List<ProcessInstance> runInstances = new ArrayList<ProcessInstance>();
            for (String cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<WorkflowJob> wfs = getRunningWorkflows(cluster);
                if (wfs != null) {
                    for (WorkflowJob job : wfs) {
                        Tag workflowNameTag = entity.getWorkflowNameTag(job.getAppName());
                        if (!job.getAppName().equals(entity.getWorkflowName(workflowNameTag)))
                            continue;
                        WorkflowJob wf = client.getJobInfo(job.getId());
                        if (StringUtils.isEmpty(wf.getParentId()))
                            continue;

                        CoordinatorAction action = client.getCoordActionInfo(wf.getParentId());
                        String nominalTimeStr = EntityUtil.formatDateUTC(action.getNominalTime());
                        ProcessInstance instance = new ProcessInstance(cluster, nominalTimeStr, WorkflowStatus.RUNNING);
                        instance.startTime = EntityUtil.formatDateUTC(wf.getStartTime());
                        runInstances.add(instance);
                    }
                }
            }
            return new ProcessInstancesResult("Running Instances", runInstances.toArray(new ProcessInstance[runInstances.size()]));

        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    @Override
    public ProcessInstancesResult killInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.KILL, entity, start, end);
    }

    @Override
    public ProcessInstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException {
        return doJobAction(JobAction.KILL, entity, start, end, props);
    }

    @Override
    public ProcessInstancesResult suspendInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.SUSPEND, entity, start, end);
    }

    @Override
    public ProcessInstancesResult resumeInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.RESUME, entity, start, end);
    }

    @Override
    public ProcessInstancesResult getStatus(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.STATUS, entity, start, end);
    }

    private static enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS
    }

    private ProcessInstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(action, entity, start, end, null);
    }
    
    private ProcessInstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end, Properties props) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        String[] clusters = builder.getClustersDefined(entity);
        try {
            List<ProcessInstance> instances = new ArrayList<ProcessInstance>();
            for (String cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<CoordinatorAction> coordinatorActions = getCoordActions(cluster, entity, start, end);
                for (CoordinatorAction coordinatorAction : coordinatorActions) {
                    String status;
                    WorkflowJob jobInfo = null;
                    if (coordinatorAction.getExternalId() != null) {
                        jobInfo = client.getJobInfo(coordinatorAction.getExternalId());
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
                        if (action != OozieWorkflowEngine.JobAction.STATUS) {
                            jobInfo = client.getJobInfo(coordinatorAction.getExternalId());
                        }

                        String nominalTimeStr = EntityUtil.formatDateUTC(coordinatorAction.getNominalTime());

                        ProcessInstance instance = new ProcessInstance(cluster, nominalTimeStr, WorkflowStatus.valueOf(status));
                        instance.startTime = EntityUtil.formatDateUTC(jobInfo.getStartTime());
                        instance.endTime = EntityUtil.formatDateUTC(jobInfo.getEndTime());
                        instances.add(instance);
                    }
                }
            }
            return new ProcessInstancesResult(action.name(), instances.toArray(new ProcessInstance[instances.size()]));
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private List<CoordinatorAction> getCoordActions(String cluster, Entity entity, Date start, Date end) throws IvoryException {
        List<CoordinatorAction> actions = new ArrayList<CoordinatorAction>();

        String name = entity.getWorkflowName();
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            List<BundleJob> bundles = client.getBundleJobsInfo(OozieClient.FILTER_NAME + "=" + name + ";", 0, 1000);
            if (bundles == null)
                return actions;

            List<CoordinatorJob> applicableCoords = getApplicableCoords(client, start, end, bundles);

            sortCoordsByStartTime(applicableCoords);
            List<CoordinatorJob> consideredCoords = getActiveCoords(applicableCoords);

            for (CoordinatorJob coord : consideredCoords) {
                TimeUnit timeUnit = TimeUnit.valueOf(coord.getTimeUnit().name());
                Date iterStart = EntityUtil.getNextStartTime(coord.getStartTime(), timeUnit, coord.getFrequency(),
                        coord.getTimeZone(), start);
                final Date iterEnd = (coord.getEndTime().before(end) ? coord.getEndTime() : end);
                while (!iterStart.after(iterEnd)) {
                    int sequence = EntityUtil.getInstanceSequence(coord.getStartTime(), timeUnit, coord.getFrequency(),
                            coord.getTimeZone(), iterStart);
                    String actionId = coord.getId() + "@" + sequence;
                    CoordinatorAction coordActionInfo = client.getCoordActionInfo(actionId);
                    if (coordActionInfo != null) {
                        actions.add(coordActionInfo);
                    }
                    Calendar startCal = Calendar.getInstance(EntityUtil.getTimeZone(coord.getTimeZone()));
                    startCal.setTime(iterStart);
                    startCal.add(timeUnit.getCalendarUnit(), coord.getFrequency());
                    iterStart = startCal.getTime();
                }
            }
            return actions;
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }

    }

    private List<CoordinatorJob> getApplicableCoords(OozieClient client, Date start, Date end, List<BundleJob> bundles)
            throws IvoryException {
        List<CoordinatorJob> applicableCoords = new ArrayList<CoordinatorJob>();
        try {
            for (BundleJob bundle : bundles) {
                List<CoordinatorJob> coords = client.getBundleJobInfo(bundle.getId()).getCoordinators();
                for (CoordinatorJob coord : coords) {
                    if (!coord.getStartTime().before(end) || !coord.getEndTime().after(start)) {
                        applicableCoords.add(coord);
                    }
                }
            }
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

    protected List<CoordinatorJob> getActiveCoords(List<CoordinatorJob> applicableCoords) {
        List<CoordinatorJob> consideredCoords = new ArrayList<CoordinatorJob>();
        List<CoordinatorJob> killedCoords = new ArrayList<CoordinatorJob>();
        List<Pair<Date, Date>> ranges = new ArrayList<Pair<Date, Date>>();
        Pair<Date, Date> range = null;
        for (CoordinatorJob coord : applicableCoords) {
            if (coord.getStatus() != Job.Status.KILLED) {
                if (range == null) {
                    range = new Pair<Date, Date>(coord.getStartTime(), coord.getEndTime());
                }
                if (coord.getStartTime().after(range.second)) {
                    ranges.add(range);
                    range = new Pair<Date, Date>(coord.getStartTime(), coord.getEndTime());
                } else {
                    range = new Pair<Date, Date>(range.first, coord.getEndTime());
                }
                consideredCoords.add(coord);
            } else {
                killedCoords.add(coord);
            }
        }
        if (range != null)
            ranges.add(range);

        for (CoordinatorJob coord : killedCoords) {
            boolean add = true;
            for (Pair<Date, Date> rangeVal : ranges) {
                if (!coord.getStartTime().before(rangeVal.first) && !coord.getEndTime().after(rangeVal.second)) {
                    add = false;
                    break;
                }
            }
            if (add) {
                consideredCoords.add(coord);
            }
        }

        sortCoordsByStartTime(consideredCoords);
        return consideredCoords;
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

        Map<String, BundleJob> bundleMap = findLatestBundle(oldEntity);

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
                updateCoords(cluster, bundle.getId(), EntityUtil.getConcurrency(newEntity), EntityUtil.getEndTime(newEntity, cluster));
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
            Map<String, BundleJob> processBundles = findLatestBundle(entity);
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

    private void updateCoords(String cluster, String bundleId, int concurrency, Date endTime) throws IvoryException {
        List<CoordinatorJob> coords = getBundleInfo(cluster, bundleId).getCoordinators();

        // Find coord with min start time. Assumes that endTime if wrt coord
        // with min start time
        Date minCoordStartTime = null;
        for (CoordinatorJob coord : coords) {
            if (minCoordStartTime == null || minCoordStartTime.after(coord.getStartTime()))
                minCoordStartTime = coord.getStartTime();
        }

        for (CoordinatorJob coord : coords) {
            // Add offset to end time for late coords
            Date localEndTime = addOffest(endTime, minCoordStartTime, coord.getStartTime());
            LOG.debug("Updating endtime of coord " + coord.getId() + " to " + EntityUtil.formatDateUTC(localEndTime));

            if (localEndTime.before(coord.getStartTime())) {
                change(cluster, coord.getId(), null, null, EntityUtil.formatDateUTC(coord.getStartTime()));
                continue;
            }

            if (coord.getLastActionTime() != null && localEndTime.before(coord.getLastActionTime())) {
                // oozie materializes action for pause time, but not end time.
                // so, take previous min
                Date pauseTime = getPreviousMin(localEndTime);
                if (pauseTime.before(now()))
                    throw new IvoryException("Pause time " + pauseTime + " can't be in the past!");

                change(cluster, coord.getId(), null, null, EntityUtil.formatDateUTC(pauseTime));
            }
            change(cluster, coord.getId(), concurrency, localEndTime, "");
        }
    }

    private void updateInternal(Entity oldEntity, Entity newEntity, String cluster, BundleJob bundle) throws IvoryException {
        OozieWorkflowBuilder<Entity> builder = (OozieWorkflowBuilder<Entity>) WorkflowBuilder.getBuilder(ENGINE, oldEntity);

        // Change end time of coords and schedule new bundle
        Job.Status bundleStatus = bundle.getStatus();
        if (bundleStatus != Job.Status.SUSPENDED && bundleStatus != Job.Status.SUSPENDEDWITHERROR
                && bundleStatus != Job.Status.PREPSUSPENDED) {
            suspendEntity(cluster, bundle.getId());
        }

        Date endTime = getNextMin(getNextMin(now()));
        updateCoords(cluster, bundle.getId(), EntityUtil.getConcurrency(oldEntity), endTime);

        if (bundleStatus != Job.Status.SUSPENDED && bundleStatus != Job.Status.SUSPENDEDWITHERROR
                && bundleStatus != Job.Status.PREPSUSPENDED) {
            resumeEntity(cluster, bundle.getId());
        }

        // schedule new entity
        Date newStartTime = builder.getNextStartTime(newEntity, cluster, endTime);
        Entity clonedNewEntity = newEntity.clone();
        EntityUtil.setStartDate(clonedNewEntity, cluster, newStartTime);
        schedule(clonedNewEntity);
    }

    private Date addOffest(Date target, Date globalTime, Date localTime) {
        long offset = localTime.getTime() - globalTime.getTime();
        return new Date(target.getTime() + offset);
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
    private List<WorkflowJob> getRunningWorkflows(String cluster) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getJobsInfo("status=" + Job.Status.RUNNING, 1, 1000);
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
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.RUN, cluster, jobId));
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
        listener.beforeSchedule(cluster, entity);
        return run(cluster, props);
    }

    public String run(String cluster, Properties props) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            String jobId = client.run(props);
            LOG.info("Submitted " + jobId + " on cluster " + cluster + " with properties : " + props);
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.RUN, cluster, jobId));
            return jobId;
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new IvoryException("Unable to schedule workflows", e);
        }
    }

    private void suspendEntity(String cluster, String jobId) throws IvoryException {
        listener.beforeSuspend(cluster, jobId);
        suspend(cluster, jobId);
    }

    public void suspend(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            client.suspend(jobId);
            LOG.info("Suspended job " + jobId + " on cluster " + cluster);
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.SUSPEND, cluster, jobId));
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void resumeEntity(String cluster, String jobId) throws IvoryException {
        listener.beforeResume(cluster, jobId);
        resume(cluster, jobId);
    }

    public void resume(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            client.resume(jobId);
            LOG.info("Resumed job " + jobId + " on cluster " + cluster);
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.RESUME, cluster, jobId));
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void killEntity(String cluster, String jobId) throws IvoryException {
        listener.beforeDelete(cluster, jobId);
        kill(cluster, jobId);
    }

    public void kill(String cluster, String jobId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            client.kill(jobId);
            LOG.info("Killed job " + jobId + " on cluster " + cluster);
            TransactionManager.performAction(new OozieWorkflowEngineAction(Action.KILL, cluster, jobId));
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    public void change(String cluster, String jobId, String changeValue) throws IvoryException {
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
}
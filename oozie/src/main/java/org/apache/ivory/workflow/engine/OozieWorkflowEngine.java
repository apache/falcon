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
import java.util.Arrays;
import java.util.Calendar;
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
import org.apache.ivory.Pair;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.util.RuntimeProperties;
import org.apache.ivory.workflow.OozieWorkflowBuilder;
import org.apache.ivory.workflow.WorkflowBuilder;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.oozie.coord.TimeUnit;
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

    private static List<Job.Status> BUNDLE_ACTIVE_STATUS = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING, Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED);
    private static List<Job.Status> BUNDLE_SUSPENDED_STATUS = Arrays.asList(Job.Status.PREPSUSPENDED, Job.Status.SUSPENDED);
    private static List<Job.Status> BUNDLE_RUNNING_STATUS = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING);
    private static List<Job.Status> BUNDLE_KILLED_STATUS = Arrays.asList(Job.Status.KILLED);

    private static List<Job.Status> BUNDLE_KILL_PRECOND = BUNDLE_ACTIVE_STATUS;
    private static List<Job.Status> BUNDLE_SUSPEND_PRECOND = Arrays.asList(Job.Status.PREP, Job.Status.RUNNING, Job.Status.DONEWITHERROR);
    private static List<Job.Status> BUNDLE_RESUME_PRECOND = Arrays.asList(Job.Status.SUSPENDED, Job.Status.PREPSUSPENDED);

    @Override
    public String schedule(Entity entity) throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);

        Map<String, Object> newFlows = builder.newWorkflowSchedule(entity);

        List<Properties> workflowProps = (List<Properties>) newFlows.get(WorkflowBuilder.PROPS);
        List<Cluster> clusters = (List<Cluster>) newFlows.get(WorkflowBuilder.CLUSTERS);

        StringBuilder buffer = new StringBuilder();
        try {
            for (int index = 0; index < workflowProps.size(); index++) {
                OozieClient client = OozieClientFactory.get(clusters.get(index));

                listener.beforeSchedule(clusters.get(index), entity);
                LOG.info("**** Submitting with properties : " + workflowProps.get(0));
                String result = client.run(workflowProps.get(0));
                listener.afterSchedule(clusters.get(index), entity);

                buffer.append(result).append(',');
            }
        } catch (OozieClientException e) {
            LOG.error("Unable to schedule workflows", e);
            throw new IvoryException("Unable to schedule workflows", e);
        }
        return buffer.toString();
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
    public boolean exists(Entity entity) throws IvoryException {
        return isBundleInState(entity, BundleStatus.EXISTS);
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
        ACTIVE, RUNNING, SUSPENDED, EXISTS
    }
    
    private boolean isBundleInState(Entity entity, BundleStatus status) throws IvoryException {
        Map<Cluster, BundleJob> bundles = findBundle(entity);
        for(BundleJob bundle:bundles.values()) {
            if(bundle == MISSING)   //There is no bundle
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

    private Map<Cluster, BundleJob> findBundle(Entity entity) throws IvoryException {
        try {
            WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
            String name = entity.getWorkflowName();

            Cluster[] clusters = builder.getScheduledClustersFor(entity);
            Map<Cluster, BundleJob> jobArray = new HashMap<Cluster, BundleJob>();

            for (Cluster cluster : clusters) {
                OozieClient client = OozieClientFactory.get(cluster);
                List<BundleJob> jobs = client.getBundleJobsInfo(OozieClient.FILTER_NAME + "=" + name + ";", 0, 100);
                if (jobs == null || jobs.isEmpty())
                    jobArray.put(cluster, MISSING);
                else { // select recent bundle
                    Date createdTime = null;
                    BundleJob bundle = null;
                    for (BundleJob job : jobs) {
                        if (createdTime == null || (job.getCreatedTime().after(createdTime))) {
                            createdTime = job.getCreatedTime();
                            bundle = job;
                        }
                    }
                    jobArray.put(cluster, bundle);
                }
            }
            return jobArray;
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
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
        Map<Cluster, BundleJob> jobs = findBundle(entity);
        for (Cluster cluster : jobs.keySet()) {
            BundleJob job = jobs.get(cluster);
            if (job == MISSING || !BUNDLE_ACTIVE_STATUS.contains(job.getStatus())) {
                LOG.warn("No active job found for " + entity.getName());
                success = false;
                break;
            } 
            
            switch (action) {
                case SUSPEND:
                    //not already suspended and preconditions are true
                    if (!BUNDLE_SUSPENDED_STATUS.contains(job.getStatus()) && BUNDLE_SUSPEND_PRECOND.contains(job.getStatus())) {
                        suspend(cluster, entity, job.getId());
                        success = true;
                    }
                    break;
                    
                case RESUME:
                    //not already running and preconditions are true
                    if(!BUNDLE_RUNNING_STATUS.contains(job.getStatus()) && BUNDLE_RESUME_PRECOND.contains(job.getStatus())) {
                        resume(cluster, entity, job.getId());
                        success = true;
                    }
                    break;
                    
                case KILL:
                    //not already killed and preconditions are true
                    if(!BUNDLE_KILLED_STATUS.contains(job.getStatus()) && BUNDLE_KILL_PRECOND.contains(job.getStatus())) {
                        kill(cluster, entity, job.getId());
                        success = true;
                    }
                    break;
            }
        }
        return success ? "SUCCESS" : "FAILED";
    }
    
    // TODO just returns first 1000
    private List<WorkflowJob> getRunningWorkflows(Cluster cluster) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getJobsInfo("status="+Job.Status.RUNNING, 1, 1000);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
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
    public Map<String, Set<Pair<String, String>>> killInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.KILL, entity, start, end);
    }

    @Override
    public Map<String, Set<Pair<String, String>>> reRunInstances(Entity entity, Date start, Date end, Properties props)
            throws IvoryException {
        return doJobAction(JobAction.RERUN, entity, start, end, props);
    }

    @Override
    public Map<String, Set<Pair<String, String>>> suspendInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.SUSPEND, entity, start, end);
    }

    @Override
    public Map<String, Set<Pair<String, String>>> resumeInstances(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.RESUME, entity, start, end);
    }

    @Override
    public Map<String, Set<Pair<String, String>>> getStatus(Entity entity, Date start, Date end) throws IvoryException {
        return doJobAction(JobAction.STATUS, entity, start, end);
    }

    private static enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS
    }

    private Map<String, Set<Pair<String, String>>> doJobAction(JobAction action, Entity entity, Date start, Date end)
            throws IvoryException {
        return doJobAction(action, entity, start, end, null);
    }

    private Map<String, Set<Pair<String, String>>> doJobAction(JobAction action, Entity entity, Date start, Date end, Properties props)
            throws IvoryException {
        WorkflowBuilder builder = WorkflowBuilder.getBuilder(ENGINE, entity);
        Cluster[] clusters = builder.getScheduledClustersFor(entity);
        List<ExternalId> extIds = builder.getExternalIds(entity, start, end);
        Map<String, Set<Pair<String, String>>> instMap = new HashMap<String, Set<Pair<String, String>>>();

        try {
            for (Cluster cluster : clusters) {
                Set<Pair<String, String>> insts = new HashSet<Pair<String, String>>();
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

                                client.kill(jobId);
                                status = Status.KILLED.name();
                                break;

                            case RERUN:
                                if (!WF_RERUN_PRECOND.contains(jobInfo.getStatus())) 
                                break;

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
                                status = Status.RUNNING.name();
                                break;

                            case SUSPEND:
                                if (!WF_SUSPEND_PRECOND.contains(jobInfo.getStatus())) 
                                break;

                                client.suspend(jobId);
                                status = Status.SUSPENDED.name();
                                break;

                            case RESUME:
                                if (!WF_RESUME_PRECOND.contains(jobInfo.getStatus())) 
                                break;

                                client.resume(jobId);
                                status = Status.RUNNING.name();
                                break;
                                
                            case STATUS:
                                break;
                        }
                    }
                    insts.add(Pair.of(extId.getDateAsString(), status));
                }
                instMap.put(cluster.getName(), insts);
            }
        } catch (Exception e) {
            throw new IvoryException(e);
        }
        return instMap;
    }

    @Override
    public void update(Entity oldEntity, Entity newEntity) throws IvoryException {
        Map<Cluster, BundleJob> bundleMap = findBundle(oldEntity);
        OozieWorkflowBuilder<Entity> builder = (OozieWorkflowBuilder<Entity>) WorkflowBuilder.getBuilder(ENGINE, oldEntity);

        for (Map.Entry<Cluster, BundleJob> entry : bundleMap.entrySet()) {
            Cluster cluster = entry.getKey();
            BundleJob bundle = entry.getValue();
            String clusterName = cluster.getName();
            
            int oldConcurrency = builder.getConcurrency(oldEntity);
            int newConcurrency = builder.getConcurrency(newEntity);
            String oldEndTime = builder.getEndTime(oldEntity, clusterName);
            String newEndTime = builder.getEndTime(newEntity, clusterName);
            
            if (oldConcurrency != newConcurrency || !oldEndTime.equals(newEndTime)) {
                Entity clonedOldEntity = oldEntity.clone();
                builder.setConcurrency(clonedOldEntity, newConcurrency);
                builder.setEndTime(clonedOldEntity, clusterName, EntityUtil.parseDateUTC(newEndTime));
                if(clonedOldEntity == newEntity) {
                    //only concurrency and endtime are changed. So, change bundle
                    change(cluster, bundle.getId(), newConcurrency, EntityUtil.parseDateUTC(newEndTime), null);
                    return;
                }
            }
            
            //Change end time of coords and schedule new bundle     
            
            // suspend so that no new coord actions are created
            suspend(cluster, oldEntity, bundle.getId());
            List<CoordinatorJob> coords = getBundleInfo(cluster, bundle.getId()).getCoordinators();
            
            //Find default coord's start time(min start time)
            Date minCoordStartTime = null;
            for (CoordinatorJob coord : coords) {
                if(minCoordStartTime == null || minCoordStartTime.after(coord.getStartTime()))
                    minCoordStartTime = coord.getStartTime();
            }
            
            //Pause time should be > now in oozie. So, add offset to pause time to account for time difference between ivory host and oozie host
            long pauseDelay = Integer.valueOf(RuntimeProperties.get().getProperty("oozie.pause.delayInSecs", "45")) * 1000;
            Date pauseTime = new Date(System.currentTimeMillis() + pauseDelay);
            Date newStartTime = null;
            
            for (CoordinatorJob coord : coords) {
                //Add offset to pause time for late coords
                Date localPauseTime = addOffest(pauseTime, coord.getStartTime(), minCoordStartTime);
                
                //Set pause time to now so that future coord actions are deleted
                change(cluster, coord.getId(), coord.getConcurrency(), coord.getLastActionTime(), localPauseTime);
                
                //Change end time and reset pause time
                coord = getCoordinatorInfo(cluster, coord.getId());
                change(cluster, coord.getId(), coord.getConcurrency(), coord.getLastActionTime(), null);

                //calculate start time for updated entity as next schedule time after end date
                if(newStartTime == null || newStartTime.after(coord.getLastActionTime())) {
                    newStartTime = coord.getLastActionTime();
                }
            }
            resume(cluster, oldEntity, bundle.getId());
            
            //schedule new entity
            Entity schedEntity = newEntity.clone();
            builder.setStartDate(schedEntity, clusterName, newStartTime);
            schedule(schedEntity);
        }
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

    //Doesn't return any coord actions
    private CoordinatorJob getCoordinatorInfo(Cluster cluster, String coordId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            return client.getCoordJobInfo(coordId, "", 0, 0);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void suspend(Cluster cluster, Entity entity, String bunldeId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            listener.beforeSuspend(cluster, entity);
            client.suspend(bunldeId);
            listener.afterSuspend(cluster, entity);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void kill(Cluster cluster, Entity entity, String bunldeId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            listener.beforeDelete(cluster, entity);
            client.kill(bunldeId);
            listener.afterDelete(cluster, entity);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void resume(Cluster cluster, Entity entity, String bunldeId) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            listener.beforeResume(cluster, entity);
            client.resume(bunldeId);
            listener.afterResume(cluster, entity);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }

    private void change(Cluster cluster, String id, int concurrency, Date endTime, Date pauseTime) throws IvoryException {
        OozieClient client = OozieClientFactory.get(cluster);
        try {
            StringBuilder changeValue = new StringBuilder();
            changeValue.append(OozieClient.CHANGE_VALUE_CONCURRENCY).append("=").append(concurrency).append(";");
            changeValue.append(OozieClient.CHANGE_VALUE_ENDTIME).append("=").append(EntityUtil.formatDateUTC(endTime)).append(";");
            changeValue.append(OozieClient.CHANGE_VALUE_PAUSETIME).append("=").append(pauseTime == null ? "" : EntityUtil.formatDateUTC(pauseTime));
            client.change(id, changeValue.toString());
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }
}
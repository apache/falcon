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

import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.EntityExecutor;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.execution.FalconExecutionService;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.update.UpdateHelper;
import org.apache.falcon.util.DateUtil;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Workflow engine which uses Falcon's native scheduler.
 */
public class FalconWorkflowEngine extends AbstractWorkflowEngine {

    private static final Logger LOG = LoggerFactory.getLogger(FalconWorkflowEngine.class);
    private static final FalconExecutionService EXECUTION_SERVICE = FalconExecutionService.get();
    private static final StateStore STATE_STORE = AbstractStateStore.get();
    private static final ConfigurationStore CONFIG_STORE = ConfigurationStore.get();
    private static final String FALCON_INSTANCE_ACTION_CLUSTERS = "falcon.instance.action.clusters";
    public static final String FALCON_FORCE_RERUN = "falcon.system.force.rerun";
    public static final String FALCON_RERUN = "falcon.system.rerun";
    public static final String FALCON_SKIP_DRYRUN = "falcon.system.skip.dryrun";
    public static final String FALCON_RESUME = "falcon.system.resume";

    private enum JobAction {
        KILL, SUSPEND, RESUME, RERUN, STATUS, SUMMARY, PARAMS
    }

    public FalconWorkflowEngine() {
        // Registering As it cleans up staging paths and not entirely Oozie Specific.
        registerListener(new OozieHouseKeepingService());
    }

    @Override
    public boolean isAlive(Cluster cluster) throws FalconException {
        return DAGEngineFactory.getDAGEngine(cluster).isAlive();
    }

    @Override
    public void schedule(Entity entity, Boolean skipDryRun, Map<String, String> suppliedProps) throws FalconException {
        Properties props  = new Properties();
        if (suppliedProps != null && !suppliedProps.isEmpty()) {
            props.putAll(suppliedProps);
        }
        if (skipDryRun) {
            props.put(FalconWorkflowEngine.FALCON_SKIP_DRYRUN, "true");
        }
        EXECUTION_SERVICE.schedule(entity, props);
    }

    @Override
    public void dryRun(Entity entity, String clusterName, Boolean skipDryRun) throws FalconException {
        Properties props  = new Properties();
        if (skipDryRun) {
            props.put(FalconWorkflowEngine.FALCON_SKIP_DRYRUN, "true");
        }
        DAGEngineFactory.getDAGEngine(clusterName).submit(entity, props);
    }

    @Override
    public boolean isActive(Entity entity) throws FalconException {
        EntityID id = new EntityID(entity);
        // Ideally state store should have all entities, but, check anyway.
        if (STATE_STORE.entityExists(id)) {
            return STATE_STORE.getEntity(id).getCurrentState() != EntityState.STATE.SUBMITTED;
        }
        return false;
    }

    @Override
    public boolean isSuspended(Entity entity) throws FalconException {
        return STATE_STORE.getEntity(new EntityID(entity))
                .getCurrentState().equals(EntityState.STATE.SUSPENDED);
    }

    @Override
    public boolean isCompleted(Entity entity) throws FalconException {
        return STATE_STORE.isEntityCompleted(new EntityID(entity));
    }

    @Override
    public boolean isMissing(Entity entity) throws FalconException {
        return !STATE_STORE.entityExists(new EntityID(entity));
    }

    @Override
    public String suspend(Entity entity) throws FalconException {
        EXECUTION_SERVICE.suspend(entity);
        return "SUCCESS";
    }

    @Override
    public String resume(Entity entity) throws FalconException {
        EXECUTION_SERVICE.resume(entity);
        return "SUCCESS";
    }

    @Override
    public String delete(Entity entity) throws FalconException {
        if (isActive(entity)) {
            EXECUTION_SERVICE.delete(entity);
        }
        // This should remove it from state store too as state store listens to config store changes.
        CONFIG_STORE.remove(entity.getEntityType(), entity.getName());
        return "SUCCESS";
    }

    @Override
    public String delete(Entity entity, String cluster) throws FalconException {
        EXECUTION_SERVICE.getEntityExecutor(entity, cluster).killAll();
        return "SUCCESS";
    }

    @Override
    public InstancesResult getRunningInstances(Entity entity, List<LifeCycle> lifeCycles) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        List<InstancesResult.Instance> runInstances = new ArrayList<>();

        for (String cluster : clusters) {
            Collection<InstanceState> instances =
                    STATE_STORE.getExecutionInstances(entity, cluster, InstanceState.getRunningStates());
            for (InstanceState state : instances) {
                String instanceTimeStr = state.getInstance().getInstanceTime().toString();
                InstancesResult.Instance instance = new InstancesResult.Instance(cluster, instanceTimeStr,
                        InstancesResult.WorkflowStatus.RUNNING);
                instance.startTime = state.getInstance().getActualStart().toDate();
                runInstances.add(instance);
            }
        }
        InstancesResult result = new InstancesResult(APIResult.Status.SUCCEEDED, "Running Instances");
        result.setInstances(runInstances.toArray(new InstancesResult.Instance[runInstances.size()]));
        return result;
    }

    private InstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end,
                                        Properties props, List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(action, entity, start, end, props, lifeCycles, false);
    }

    private InstancesResult doJobAction(JobAction action, Entity entity, Date start, Date end,
                                        Properties props, List<LifeCycle> lifeCycles,
                                        boolean isForced) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        List<String> clusterList = getIncludedClusters(props, FALCON_INSTANCE_ACTION_CLUSTERS);
        APIResult.Status overallStatus = APIResult.Status.SUCCEEDED;
        int instanceCount = 0;

        Collection<InstanceState.STATE> states;
        switch(action) {
        case KILL:
        case SUSPEND:
            states = InstanceState.getActiveStates();
            break;
        case RESUME:
            states = new ArrayList<>();
            states.add(InstanceState.STATE.SUSPENDED);
            break;
        case PARAMS:
            // Applicable only for running and finished jobs.
            states = InstanceState.getRunningStates();
            states.addAll(InstanceState.getTerminalStates());
            states.add(InstanceState.STATE.SUSPENDED);
            break;
        case STATUS:
            states = InstanceState.getActiveStates();
            states.addAll(InstanceState.getTerminalStates());
            states.add(InstanceState.STATE.SUSPENDED);
            break;
        case RERUN:
            // Applicable only for terminated States
            states = InstanceState.getTerminalStates();
            break;
        default:
            throw new IllegalArgumentException("Unhandled action " + action);
        }

        List<ExecutionInstance> instancesToActOn = new ArrayList<>();
        for (String cluster : clusters) {
            if (clusterList.size() != 0 && !clusterList.contains(cluster)) {
                continue;
            }
            LOG.debug("Retrieving instances for cluster : {} for action {}" , cluster, action);
            Collection<InstanceState> instances =
                    STATE_STORE.getExecutionInstances(entity, cluster, states, new DateTime(start), new DateTime(end));
            for (InstanceState state : instances) {
                instancesToActOn.add(state.getInstance());
            }
        }

        // To ensure compatibility with OozieWorkflowEngine.
        // Also because users would like to see the most recent instances first.
        if (action == JobAction.STATUS || action == JobAction.PARAMS) {
            sortInstancesDescBySequence(instancesToActOn);
        }

        List<InstancesResult.Instance> instances = new ArrayList<>();
        for (ExecutionInstance ins : instancesToActOn) {
            instanceCount++;
            String instanceTimeStr = SchemaHelper.formatDateUTC(ins.getInstanceTime().toDate());

            InstancesResult.Instance instance = null;
            try {
                instance = performAction(ins.getCluster(), entity, action, ins, props, isForced);
                instance.instance = instanceTimeStr;
            } catch (FalconException e) {
                LOG.warn("Unable to perform action {} on cluster", action, e);
                instance = new InstancesResult.Instance(ins.getCluster(), instanceTimeStr, null);
                instance.status = InstancesResult.WorkflowStatus.ERROR;
                instance.details = e.getMessage();
                overallStatus = APIResult.Status.PARTIAL;
            }
            instances.add(instance);
        }
        if (instanceCount < 2 && overallStatus == APIResult.Status.PARTIAL) {
            overallStatus = APIResult.Status.FAILED;
        }
        InstancesResult instancesResult = new InstancesResult(overallStatus, action.name());
        instancesResult.setInstances(instances.toArray(new InstancesResult.Instance[instances.size()]));
        return instancesResult;
    }

    // Sort the instances in descending order of their sequence, so the latest is on top.
    private void sortInstancesDescBySequence(List<ExecutionInstance> instancesToActOn) {
        Collections.sort(instancesToActOn, new Comparator<ExecutionInstance>() {
            @Override
            public int compare(ExecutionInstance o1, ExecutionInstance o2) {
                return o2.getInstanceSequence() - o1.getInstanceSequence();
            }
        });
    }

    private List<String> getIncludedClusters(Properties props, String clustersType) {
        String clusters = props == null ? "" : props.getProperty(clustersType, "");
        List<String> clusterList = new ArrayList<>();
        for (String cluster : clusters.split(",")) {
            if (StringUtils.isNotEmpty(cluster)) {
                clusterList.add(cluster.trim());
            }
        }
        return clusterList;
    }

    private InstancesResult.Instance performAction(String cluster, Entity entity, JobAction action,
                                                   ExecutionInstance instance, Properties userProps,
                                                   boolean isForced) throws FalconException {
        EntityExecutor executor = EXECUTION_SERVICE.getEntityExecutor(entity, cluster);
        InstancesResult.Instance instanceInfo = null;
        LOG.debug("Retrieving information for {} for action {}", instance.getId(), action);
        if (StringUtils.isNotEmpty(instance.getExternalID())) {
            instanceInfo = DAGEngineFactory.getDAGEngine(cluster).info(instance.getExternalID());
        } else {
            instanceInfo = new InstancesResult.Instance();
        }
        switch(action) {
        case KILL:
            executor.kill(instance);
            populateInstanceInfo(instanceInfo, instance);
            break;
        case SUSPEND:
            executor.suspend(instance);
            populateInstanceInfo(instanceInfo, instance);
            break;
        case RESUME:
            executor.resume(instance);
            populateInstanceInfo(instanceInfo, instance);
            break;
        case RERUN:
            executor.rerun(instance, userProps, isForced);
            populateInstanceInfo(instanceInfo, instance);
            break;
        case STATUS:
            populateInstanceInfo(instanceInfo, instance);
            // If already scheduled externally, get details for actions
            if (StringUtils.isNotEmpty(instance.getExternalID())) {
                List<InstancesResult.InstanceAction> instanceActions =
                        DAGEngineFactory.getDAGEngine(cluster).getJobDetails(instance.getExternalID());
                instanceInfo.actions = instanceActions
                        .toArray(new InstancesResult.InstanceAction[instanceActions.size()]);
            }
            break;
        case PARAMS:
            // Mask details, log
            instanceInfo.details = null;
            instanceInfo.logFile = null;
            Properties props = DAGEngineFactory.getDAGEngine(cluster).getConfiguration(instance.getExternalID());
            InstancesResult.KeyValuePair[] keyValuePairs = new InstancesResult.KeyValuePair[props.size()];
            int i=0;
            for (String name : props.stringPropertyNames()) {
                keyValuePairs[i++] = new InstancesResult.KeyValuePair(name, props.getProperty(name));
            }
            instanceInfo.wfParams = keyValuePairs;
            break;
        default:
            throw new IllegalArgumentException("Unhandled action " + action);
        }
        return instanceInfo;
    }

    // Populates the InstancesResult.Instance instance using ExecutionInstance
    private void populateInstanceInfo(InstancesResult.Instance instanceInfo, ExecutionInstance instance)
        throws StateStoreException {
        instanceInfo.cluster = instance.getCluster();
        InstanceState.STATE state = STATE_STORE.getExecutionInstance(instance.getId()).getCurrentState();
        switch (state) {
        case SUCCEEDED:
            instanceInfo.status = InstancesResult.WorkflowStatus.SUCCEEDED;
            break;
        case FAILED:
            instanceInfo.status = InstancesResult.WorkflowStatus.FAILED;
            break;
        case KILLED:
            instanceInfo.status = InstancesResult.WorkflowStatus.KILLED;
            break;
        case READY:
            instanceInfo.status = InstancesResult.WorkflowStatus.READY;
            break;
        case WAITING:
            instanceInfo.status = InstancesResult.WorkflowStatus.WAITING;
            break;
        case SUSPENDED:
            instanceInfo.status = InstancesResult.WorkflowStatus.SUSPENDED;
            break;
        case RUNNING:
            instanceInfo.status = InstancesResult.WorkflowStatus.RUNNING;
            break;
        default:
            instanceInfo.status = InstancesResult.WorkflowStatus.UNDEFINED;
            break;
        }
        // Mask wfParams by default
        instanceInfo.wfParams = null;
    }

    @Override
    public InstancesResult killInstances(Entity entity, Date start, Date end,
                                         Properties props, List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(JobAction.KILL, entity, start, end, props, lifeCycles);
    }

    @Override
    public InstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props,
                                          List<LifeCycle> lifeCycles, Boolean isForced) throws FalconException {
        if (isForced == null) {
            isForced = false;
        }
        return doJobAction(JobAction.RERUN, entity, start, end, props, lifeCycles, isForced);
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
    public InstancesResult getStatus(Entity entity, Date start, Date end, List<LifeCycle> lifeCycles,
                                     Boolean allAttempts) throws FalconException {
        return doJobAction(JobAction.STATUS, entity, start, end, null, lifeCycles);
    }

    @Override
    public InstancesSummaryResult getSummary(Entity entity, Date start, Date end,
                                             List<LifeCycle> lifeCycles) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
        List<InstancesSummaryResult.InstanceSummary> instanceSummaries = new ArrayList<>();

        // Iterate over entity clusters
        for (String cluster : clusters) {
            LOG.debug("Retrieving summary of instances for cluster : {}", cluster);
            Map<InstanceState.STATE, Long> summaries = STATE_STORE.getExecutionInstanceSummary(entity, cluster,
                    new DateTime(start), new DateTime(end));
            Map<String, Long> summaryMap = new HashMap<>();
            // Iterate over the map and convert STATE to String
            for (Map.Entry<InstanceState.STATE, Long> summary : summaries.entrySet()) {
                summaryMap.put(summary.getKey().name(), summary.getValue());
            }
            instanceSummaries.add(new InstancesSummaryResult.InstanceSummary(cluster, summaryMap));
        }

        InstancesSummaryResult instancesSummaryResult =
                new InstancesSummaryResult(APIResult.Status.SUCCEEDED, JobAction.SUMMARY.name());
        instancesSummaryResult.setInstancesSummary(instanceSummaries.
                toArray(new InstancesSummaryResult.InstanceSummary[instanceSummaries.size()]));
        return instancesSummaryResult;
    }

    @Override
    public InstancesResult getInstanceParams(Entity entity, Date start, Date end,
                                             List<LifeCycle> lifeCycles) throws FalconException {
        return doJobAction(JobAction.PARAMS, entity, start, end, null, lifeCycles);
    }

    @Override
    public boolean isNotificationEnabled(String cluster, String jobID) throws FalconException {
        return true;
    }

    @Override
    public String update(Entity oldEntity, Entity newEntity, String cluster, Boolean skipDryRun)
        throws FalconException {
        org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        boolean entityUpdated =
                UpdateHelper.isEntityUpdated(oldEntity, newEntity, cluster,
                        EntityUtil.getLatestStagingPath(clusterEntity, oldEntity));
        StringBuilder result = new StringBuilder();
        if (!entityUpdated) {
            // Ideally should throw an exception, but, keeping it backward-compatible.
            LOG.warn("No relevant updates detected in the new entity definition for entity {}!", newEntity.getName());
            return result.toString();
        }

        Date oldEndTime = EntityUtil.getEndTime(oldEntity, cluster);
        Date newEndTime = EntityUtil.getEndTime(newEntity, cluster);
        if (newEndTime.before(DateUtil.now()) || newEndTime.before(oldEndTime)) {
            throw new FalconException("New Entity's end time " + SchemaHelper.formatDateUTC(newEndTime)
                    + " is before current time or before old end time. Entity can't be updated.");
        }

        // The steps required are the same as touch.
        DAGEngineFactory.getDAGEngine(cluster).touch(newEntity, (skipDryRun == null) ? Boolean.FALSE : skipDryRun);
        // Additionally, update the executor.
        // The update will kick in for new instances created and not for READY/WAITING instances, as with Oozie.
        Collection<InstanceState> instances = new ArrayList<>();
        instances.add(STATE_STORE.getLastExecutionInstance(oldEntity, cluster));
        EXECUTION_SERVICE.getEntityExecutor(oldEntity, cluster).update(newEntity);

        result.append(newEntity.toShortString()).append("/Effective Time: ")
                .append(getEffectiveTime(newEntity, cluster, instances));
        return result.toString();
    }

    @Override
    public String touch(Entity entity, String cluster, Boolean skipDryRun) throws FalconException {
        EntityID id = new EntityID(entity);
        // Ideally state store should have all entities, but, check anyway.
        if (STATE_STORE.entityExists(id)) {
            Date endTime = EntityUtil.getEndTime(entity, cluster);
            if (endTime.before(DateUtil.now())) {
                throw new FalconException("Entity's end time " + SchemaHelper.formatDateUTC(endTime)
                        + " is before current time. Entity can't be touch-ed as it has completed.");
            }
            Collection<InstanceState> instances =
                    STATE_STORE.getExecutionInstances(entity, cluster, InstanceState.getRunningStates());
            // touch should happen irrespective of the state the entity is in.
            DAGEngineFactory.getDAGEngine(cluster).touch(entity, (skipDryRun == null)? Boolean.FALSE : skipDryRun);
            StringBuilder builder = new StringBuilder();
            builder.append(entity.toShortString()).append("/Effective Time: ")
                    .append(getEffectiveTime(entity, cluster, instances));
            return builder.toString();
        }
        throw new FalconException("Could not find entity " + id + " in state store.");
    }

    // Effective time will be right after the last running instance.
    private String getEffectiveTime(Entity entity, String cluster, Collection<InstanceState> instances)
        throws FalconException {
        if (instances == null || instances.isEmpty()) {
            return SchemaHelper.formatDateUTC(DateUtil.now());
        } else {
            List<InstanceState> instanceList = new ArrayList(instances);
            Collections.sort(instanceList, new Comparator<InstanceState>() {
                @Override
                public int compare(InstanceState x, InstanceState y) {
                    return (x.getInstance().getInstanceSequence() < y.getInstance().getInstanceSequence()) ? -1
                            : (x.getInstance().getInstanceSequence() == y.getInstance().getInstanceSequence() ? 0 : 1);
                }
            });
            // Get the last element as the list is sorted in ascending order
            Date lastRunningInstanceTime = instanceList.get(instanceList.size() - 1)
                    .getInstance().getInstanceTime().toDate();
            Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
            // Offset the time by a few seconds, else nextStartTime will be same as the reference time.
            Date effectiveTime = EntityUtil
                    .getNextStartTime(entity, clusterEntity, DateUtil.offsetTime(lastRunningInstanceTime, 10));
            return SchemaHelper.formatDateUTC(effectiveTime);
        }
    }

    @Override
    public String reRun(String cluster, String jobId, Properties props, boolean isForced) throws FalconException {
        InstanceState instanceState = STATE_STORE.getExecutionInstance(jobId);
        ExecutionInstance instance = instanceState.getInstance();
        EntityExecutor executor = EXECUTION_SERVICE.getEntityExecutor(instance.getEntity(), cluster);
        executor.rerun(instance, props, isForced);
        return DAGEngineFactory.getDAGEngine(cluster).info(jobId).getStatus().name();
    }

    @Override
    public String getWorkflowStatus(String cluster, String jobId) throws FalconException {
        return DAGEngineFactory.getDAGEngine(cluster).info(jobId).getStatus().name();
    }

    @Override
    public Properties getWorkflowProperties(String cluster, String jobId) throws FalconException {
        return DAGEngineFactory.getDAGEngine(cluster).getConfiguration(jobId);
    }

    @Override
    public InstancesResult getJobDetails(String cluster, String jobId) throws FalconException {
        InstancesResult.Instance[] instances = new InstancesResult.Instance[1];
        InstancesResult result = new InstancesResult(APIResult.Status.SUCCEEDED,
                "Instance for workflow id:" + jobId);
        instances[0] = DAGEngineFactory.getDAGEngine(cluster).info(jobId);
        result.setInstances(instances);
        return result;
    }

    @Override
    public Boolean isWorkflowKilledByUser(String cluster, String jobId) throws FalconException {
        throw new UnsupportedOperationException("Not yet Implemented");
    }

    @Override
    public String getName() {
        return "native";
    }
}


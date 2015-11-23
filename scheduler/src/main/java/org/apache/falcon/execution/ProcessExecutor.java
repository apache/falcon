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
package org.apache.falcon.execution;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.exception.InvalidStateTransitionException;
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.event.EventType;
import org.apache.falcon.notification.service.event.JobCompletedEvent;
import org.apache.falcon.notification.service.event.TimeElapsedEvent;
import org.apache.falcon.notification.service.impl.AlarmService;
import org.apache.falcon.notification.service.impl.JobCompletionService;
import org.apache.falcon.notification.service.impl.SchedulerService;
import org.apache.falcon.predicate.Predicate;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.StateService;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

/**
 * This class is responsible for managing execution instances of a process.
 * It caches the active process instances in memory and handles notification events.
 * It intercepts all the notification events intended for its instances and passes them along to the instance after
 * acting on it, where applicable.
 */
public class ProcessExecutor extends EntityExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessExecutor.class);
    protected LoadingCache<InstanceID, ProcessExecutionInstance> instances;
    private Predicate triggerPredicate;
    private final Process process;
    private final StateService stateService = StateService.get();
    private final FalconExecutionService executionService = FalconExecutionService.get();

    /**
     * Constructor per entity, per cluster.
     *
     * @param proc
     * @param clusterName
     * @throws FalconException
     */
    public ProcessExecutor(Process proc, String clusterName) throws FalconException {
        process = proc;
        cluster = clusterName;
        id = new EntityClusterID(proc, clusterName);
    }

    @Override
    public void schedule() throws FalconException {
        // Lazy instantiation
        if (instances == null) {
            initInstances();
        }
        // Check to handle restart and restoration from state store.
        if (STATE_STORE.getEntity(id.getEntityID()).getCurrentState() != EntityState.STATE.SCHEDULED) {
            dryRun();
        } else {
            LOG.info("Process, {} was already scheduled on cluster, {}.", process.getName(), cluster);
            LOG.info("Loading instances for process {} from state store.", process.getName());
            reloadInstances();
        }
        registerForNotifications();
    }

    private void dryRun() throws FalconException {
        DAGEngineFactory.getDAGEngine(cluster).submit(process);
    }

    // Initializes the cache of execution instances. Cache is backed by the state store.
    private void initInstances() throws FalconException {
        int cacheSize = Integer.parseInt(StartupProperties.get().getProperty("scheduler.instance.cache.size",
                DEFAULT_CACHE_SIZE));

        instances = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .build(new CacheLoader<InstanceID, ProcessExecutionInstance>() {
                    @Override
                    public ProcessExecutionInstance load(InstanceID id) throws Exception {
                        return (ProcessExecutionInstance) STATE_STORE.getExecutionInstance(id).getInstance();
                    }
                });
    }

    // Re-load any active instances from state
    private void reloadInstances() throws FalconException {
        for (InstanceState instanceState : STATE_STORE.getExecutionInstances(process, cluster,
                InstanceState.getActiveStates())) {
            ExecutionInstance instance = instanceState.getInstance();
            LOG.debug("Loading instance {} from state.", instance.getId());
            switch (instanceState.getCurrentState()) {
            case RUNNING:
                onSchedule(instance);
                break;
            case READY:
                onConditionsMet(instance);
                break;
            case WAITING:
                instance.resume();
                break;
            default: // skip
            }
            instances.put(instance.getId(), (ProcessExecutionInstance) instance);
        }
    }

    @Override
    public void suspendAll() throws FalconException {
        NotificationServicesRegistry.unregister(executionService, getId());
        StringBuffer errMsg = new StringBuffer();
        // Only active instances are in memory. Suspend them first.
        for (ExecutionInstance instance : instances.asMap().values()) {
            try {
                suspend(instance);
            } catch (FalconException e) {
                // Proceed with next
                errMsg.append("Instance suspend failed for : " + instance.getId() + " due to " + e.getMessage());
                LOG.error("Instance suspend failed for : " + instance.getId(), e);
            }
        }
        for (InstanceState instanceState : STATE_STORE.getExecutionInstances(process, cluster,
                InstanceState.getActiveStates())) {
            ExecutionInstance instance = instanceState.getInstance();
            try {
                suspend(instance);
            } catch (FalconException e) {
                errMsg.append("Instance suspend failed for : " + instance.getId() + " due to " + e.getMessage());
                LOG.error("Instance suspend failed for : " + instance.getId(), e);
            }
        }
        // Some errors
        if (errMsg.length() != 0) {
            throw new FalconException("Some instances failed to suspend : " + errMsg.toString());
        }
    }

    @Override
    public void resumeAll() throws FalconException {
        if (instances == null) {
            initInstances();
        }
        StringBuffer errMsg = new StringBuffer();
        ArrayList<InstanceState.STATE> states = new ArrayList<InstanceState.STATE>();
        // TODO : Distinguish between individually suspended instance versus suspended entity?
        states.add(InstanceState.STATE.SUSPENDED);
        // Load cache with suspended instances
        for (InstanceState instanceState : STATE_STORE.getExecutionInstances(process, cluster, states)) {
            ExecutionInstance instance = instanceState.getInstance();
            try {
                resume(instance);
            } catch (FalconException e) {
                errMsg.append("Instance suspend failed for : " + instance.getId() + " due to " + e.getMessage());
                LOG.error("Instance suspend failed for : " + instance.getId(), e);
            }
        }
        registerForNotifications();
        // Some errors
        if (errMsg.length() != 0) {
            throw new FalconException("Some instances failed to resume : " + errMsg.toString());
        }
    }

    @Override
    public void killAll() throws FalconException {
        NotificationServicesRegistry.unregister(executionService, getId());
        StringBuffer errMsg = new StringBuffer();
        // Only active instances are in memory. Kill them first.
        for (ExecutionInstance instance : instances.asMap().values()) {
            try {
                kill(instance);
            } catch (FalconException e) {
                // Proceed with next
                errMsg.append("Instance kill failed for : " + instance.getId() + " due to " + e.getMessage());
                LOG.error("Instance kill failed for : " + instance.getId(), e);
            }
        }
        for (InstanceState instanceState : STATE_STORE.getExecutionInstances(process, cluster,
                InstanceState.getActiveStates())) {
            ExecutionInstance instance = instanceState.getInstance();
            try {
                kill(instance);
            } catch (FalconException e) {
                errMsg.append("Instance kill failed for : " + instance.getId() + " due to " + e.getMessage());
                LOG.error("Instance kill failed for : " + instance.getId(), e);
            }
        }
        // Some errors
        if (errMsg.length() != 0) {
            throw new FalconException("Some instances failed to kill : " + errMsg.toString());
        }
    }

    @Override
    public void suspend(ExecutionInstance instance) throws FalconException {
        try {
            instance.suspend();
            stateService.handleStateChange(instance, InstanceState.EVENT.SUSPEND, this);
        } catch (Exception e) {
            LOG.error("Suspend failed for instance id : " + instance.getId(), e);
            throw new FalconException("Suspend failed for instance : " + instance.getId(), e);
        }

    }

    @Override
    public void resume(ExecutionInstance instance) throws FalconException {

        try {
            instance.resume();
            if (((ProcessExecutionInstance) instance).isScheduled()) {
                stateService.handleStateChange(instance, InstanceState.EVENT.RESUME_RUNNING, this);
                onSchedule(instance);
            } else if (((ProcessExecutionInstance) instance).isReady()) {
                stateService.handleStateChange(instance, InstanceState.EVENT.RESUME_READY, this);
                onConditionsMet(instance);
            } else {
                stateService.handleStateChange(instance, InstanceState.EVENT.RESUME_WAITING, this);
            }
        } catch (Exception e) {
            LOG.error("Resume failed for instance id : " + instance.getId(), e);
            throw new FalconException("Resume failed for instance : " + instance.getId(), e);
        }
    }

    @Override
    public void kill(ExecutionInstance instance) throws FalconException {
        try {
            // Kill will de-register from notification services
            instance.kill();
            stateService.handleStateChange(instance, InstanceState.EVENT.KILL, this);
        } catch (Exception e) {
            LOG.error("Kill failed for instance id : " + instance.getId(), e);
            throw new FalconException("Kill failed for instance : " + instance.getId(), e);
        }
    }

    @Override
    public Entity getEntity() {
        return process;
    }

    private ProcessExecutionInstance buildInstance(Event event) throws FalconException {
        // If a time triggered instance, use nominal time from event
        if (event.getType() == EventType.TIME_ELAPSED) {
            TimeElapsedEvent timeEvent = (TimeElapsedEvent) event;
            LOG.debug("Creating a new process instance for nominal time {}.", timeEvent.getInstanceTime());
            return new ProcessExecutionInstance(process, timeEvent.getInstanceTime(), cluster);
        } else {
            return new ProcessExecutionInstance(process, DateTime.now(), cluster);
        }
    }

    @Override
    public void onEvent(Event event) throws FalconException {
        try {
            // Handle event if applicable
            if (shouldHandleEvent(event)) {
                handleEvent(event);
            } else {
                // Else, pass it along to the execution instance
                if (event.getTarget() instanceof InstanceID) {
                    InstanceID instanceID = (InstanceID) event.getTarget();
                    ProcessExecutionInstance instance = instances.get(instanceID);
                    if (instance != null) {
                        instance.onEvent(event);
                        if (instance.isReady()) {
                            stateService.handleStateChange(instance, InstanceState.EVENT.CONDITIONS_MET, this);
                        } else if (instance.hasTimedout()) {
                            stateService.handleStateChange(instance, InstanceState.EVENT.TIME_OUT, this);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new FalconException("Unable to handle event of type : " + event.getType() + " with target:"
                    + event.getTarget(), e);
        }
    }

    private void handleEvent(Event event) throws FalconException {
        ProcessExecutionInstance instance;
        InstanceID instanceID;
        try {
            switch (event.getType()) {
            // TODO : Handle cases where scheduling fails.
            case JOB_SCHEDULED:
                instance = instances.get((InstanceID)event.getTarget());
                instance.onEvent(event);
                stateService.handleStateChange(instance, InstanceState.EVENT.SCHEDULE, this);
                break;
            case JOB_COMPLETED:
                instance = instances.get((InstanceID)event.getTarget());
                instance.onEvent(event);
                switch (((JobCompletedEvent) event).getStatus()) {
                case SUCCEEDED:
                    stateService.handleStateChange(instance, InstanceState.EVENT.SUCCEED, this);
                    break;
                case FAILED:
                    stateService.handleStateChange(instance, InstanceState.EVENT.FAIL, this);
                    break;
                case KILLED:
                    stateService.handleStateChange(instance, InstanceState.EVENT.KILL, this);
                    break;
                case SUSPENDED:
                    stateService.handleStateChange(instance, InstanceState.EVENT.SUSPEND, this);
                    break;
                default:
                    throw new InvalidStateTransitionException(
                            "Job seems to be have been managed outside Falcon.");
                }
                break;
            default:
                if (isTriggerEvent(event)) {
                    instance = buildInstance(event);
                    stateService.handleStateChange(instance, InstanceState.EVENT.TRIGGER, this);
                    // This happens where are no conditions the instance is waiting on (for example, no data inputs).
                    if (instance.isReady()) {
                        stateService.handleStateChange(instance, InstanceState.EVENT.CONDITIONS_MET, this);
                    }
                }
            }
        } catch (Exception ee) {
            throw new FalconException("Unable to cache execution instance", ee);
        }
    }

    // Evaluates the trigger predicate against the current event, to determine if a new instance needs to be triggered.
    private boolean isTriggerEvent(Event event) {
        try {
            return triggerPredicate.evaluate(Predicate.getPredicate(event));
        } catch (FalconException e) {
            return false;
        }
    }

    // Registers for all notifications that should trigger an instance.
    // Currently, only time based triggers are handled.
    protected void registerForNotifications() throws FalconException {
        AlarmService.AlarmRequestBuilder requestBuilder =
                (AlarmService.AlarmRequestBuilder)
                NotificationServicesRegistry.getService(NotificationServicesRegistry.SERVICE.TIME)
                        .createRequestBuilder(executionService, getId());
        Cluster processCluster = ProcessHelper.getCluster(process, cluster);

        InstanceState instanceState = STATE_STORE.getLastExecutionInstance(process, cluster);
        // If there are no instances, use process's start, else, use last materialized instance's nominal time
        Date startTime = (instanceState == null) ? processCluster.getValidity().getStart()
                : EntityUtil.getNextInstanceTime(instanceState.getInstance().getInstanceTime().toDate(),
                    EntityUtil.getFrequency(process), EntityUtil.getTimeZone(process), 1);
        Date endTime = processCluster.getValidity().getEnd();
        // TODO : Handle cron based and calendar based time triggers
        // TODO : Set execution order details.
        requestBuilder.setFrequency(process.getFrequency())
                .setStartTime(new DateTime(startTime))
                .setEndTime(new DateTime(endTime))
                .setTimeZone(TimeZone.getTimeZone("UTC"));
        NotificationServicesRegistry.register(requestBuilder.build());
        LOG.info("Registered for a time based notification for process {}  with frequency: {}, "
                + "start time: {}, end time: {}", process.getName(), process.getFrequency(), startTime, endTime);
        triggerPredicate = Predicate.createTimePredicate(startTime.getTime(), endTime.getTime(), -1);
    }

    // This executor must handle any events intended for itself.
    // Or, if it is job run or job complete notifications, so it can handle the instance's state transition.
    private boolean shouldHandleEvent(Event event) {
        return event.getTarget().equals(id)
                || event.getType() == EventType.JOB_COMPLETED
                || event.getType() == EventType.JOB_SCHEDULED;
    }

    @Override
    public void onTrigger(ExecutionInstance instance) throws FalconException {
        instances.put(new InstanceID(instance), (ProcessExecutionInstance) instance);
    }

    @Override
    public void onConditionsMet(ExecutionInstance instance) throws FalconException {
        // Put process in run queue and register for notification
        SchedulerService.JobScheduleRequestBuilder requestBuilder = (SchedulerService.JobScheduleRequestBuilder)
                NotificationServicesRegistry.getService(NotificationServicesRegistry.SERVICE.JOB_SCHEDULE)
                        .createRequestBuilder(executionService, getId());
        requestBuilder.setInstance(instance);
        NotificationServicesRegistry.register(requestBuilder.build());
    }

    @Override
    public void onSchedule(ExecutionInstance instance) throws FalconException {
        JobCompletionService.JobCompletionRequestBuilder completionRequestBuilder =
                (JobCompletionService.JobCompletionRequestBuilder)
                NotificationServicesRegistry.getService(NotificationServicesRegistry.SERVICE.JOB_COMPLETION)
                        .createRequestBuilder(executionService, getId());
        completionRequestBuilder.setExternalId(instance.getExternalID());
        completionRequestBuilder.setCluster(instance.getCluster());
        NotificationServicesRegistry.register(completionRequestBuilder.build());
    }

    @Override
    public void onSuspend(ExecutionInstance instance) throws FalconException {
        instances.invalidate(instance.getId());
    }

    @Override
    public void onResume(ExecutionInstance instance) throws FalconException {
        instances.put(instance.getId(), (ProcessExecutionInstance) instance);
    }

    @Override
    public void onKill(ExecutionInstance instance) throws FalconException {
        instances.invalidate(instance.getId());
    }

    @Override
    public void onSuccess(ExecutionInstance instance) throws FalconException {
        instance.destroy();
        instances.invalidate(instance.getId());
    }

    @Override
    public void onFailure(ExecutionInstance instance) throws FalconException {
        instance.destroy();
        instances.invalidate(instance.getId());
    }

    @Override
    public void onTimeOut(ExecutionInstance instance) throws FalconException {
        instance.destroy();
        instances.invalidate(instance.getId());
    }
}

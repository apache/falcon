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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.notification.service.event.DataEvent;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.event.JobCompletedEvent;
import org.apache.falcon.notification.service.event.JobScheduledEvent;
import org.apache.falcon.notification.service.impl.DataAvailabilityService;
import org.apache.falcon.predicate.Predicate;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.engine.DAGEngine;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * Represents an execution instance of a process.
 * Responsible for user actions such as suspend, resume, kill on individual instances.
 */

public class ProcessExecutionInstance extends ExecutionInstance {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessExecutionInstance.class);
    private final Process process;
    private List<Predicate> awaitedPredicates = new ArrayList<>();
    private DAGEngine dagEngine = null;
    private boolean hasTimedOut = false;
    private InstanceID id;
    private int instanceSequence;
    private final FalconExecutionService executionService = FalconExecutionService.get();

    /**
     * Constructor.
     *
     * @param process
     * @param instanceTime
     * @param cluster
     * @throws FalconException
     */
    public ProcessExecutionInstance(Process process, DateTime instanceTime, String cluster,
                                    DateTime creationTime) throws FalconException {
        super(instanceTime, cluster, creationTime);
        this.process = process;
        this.id = new InstanceID(process, cluster, getInstanceTime());
        computeInstanceSequence();
        dagEngine = DAGEngineFactory.getDAGEngine(cluster);
        registerForNotifications(false);
    }

    /**
     *
     * @param process
     * @param instanceTime
     * @param cluster
     * @throws FalconException
     */
    public ProcessExecutionInstance(Process process, DateTime instanceTime, String cluster) throws FalconException {
        this(process, instanceTime, cluster, DateTime.now(UTC));
    }

    // Computes the instance number based on the instance Time.
    // Method can be extended to assign instance numbers for non-time based instances.
    private void computeInstanceSequence() {
        for (Cluster processCluster : process.getClusters().getClusters()) {
            if (processCluster.getName().equals(getCluster())) {
                Date start = processCluster.getValidity().getStart();
                instanceSequence = EntityUtil.getInstanceSequence(start, process.getFrequency(),
                        process.getTimezone(), getInstanceTime().toDate());
                break;
            }
        }
    }

    // Currently, registers for only data notifications to ensure gating conditions are met.
    // Can be extended to register for other notifications.
    private void registerForNotifications(boolean isResume) throws FalconException {
        if (process.getInputs() == null) {
            return;
        }
        for (Input input : process.getInputs().getInputs()) {
            // Register for notification for every required input
            if (input.isOptional()) {
                continue;
            }
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed.getClusters().getClusters()) {
                List<Location> locations = FeedHelper.getLocations(cluster, feed);
                for (Location loc : locations) {
                    if (loc.getType() != LocationType.DATA) {
                        continue;
                    }

                    Predicate predicate = Predicate.createDataPredicate(loc);
                    // To ensure we evaluate only predicates not evaluated before when an instance is resumed.
                    if (isResume && !awaitedPredicates.contains(predicate)) {
                        continue;
                    }
                    // TODO : Revisit this once the Data Availability Service has been built
                    DataAvailabilityService.DataRequestBuilder requestBuilder =
                            (DataAvailabilityService.DataRequestBuilder)
                            NotificationServicesRegistry.getService(NotificationServicesRegistry.SERVICE.DATA)
                                    .createRequestBuilder(executionService, getId());
                    requestBuilder.setDataLocation(new Path(loc.getPath()));
                    NotificationServicesRegistry.register(requestBuilder.build());
                    LOG.info("Registered for a data notification for process {} for data location {}",
                            process.getName(), loc.getPath());
                    awaitedPredicates.add(predicate);
                }
            }
        }
    }

    @Override
    public void onEvent(Event event) throws FalconException {
        switch (event.getType()) {
        case JOB_SCHEDULED:
            JobScheduledEvent jobScheduleEvent = (JobScheduledEvent) event;
            setExternalID(jobScheduleEvent.getExternalID());
            setActualStart(jobScheduleEvent.getStartTime());
            break;
        case JOB_COMPLETED:
            setActualEnd(((JobCompletedEvent)event).getEndTime());
            break;
        case DATA_AVAILABLE:
            // Data has not become available and the wait time has passed
            if (((DataEvent) event).getStatus() == DataEvent.STATUS.UNAVAILABLE) {
                if (getTimeOutInMillis() <= (System.currentTimeMillis() - getCreationTime().getMillis())) {
                    hasTimedOut = true;
                }
            } else {
                // If the event matches any of the awaited predicates, remove the predicate of the awaited list
                Predicate toRemove = null;
                for (Predicate predicate : awaitedPredicates) {
                    if (predicate.evaluate(Predicate.getPredicate(event))) {
                        toRemove = predicate;
                        break;
                    }
                }
                if (toRemove != null) {
                    awaitedPredicates.remove(toRemove);
                }
            }
            break;
        default:
        }
    }

    /**
     * Is the instance ready to be scheduled?
     *
     * @return true when it is not already scheduled or is gated on some conditions.
     */
    public boolean isReady() {
        if (getExternalID() != null) {
            return false;
        }
        if (awaitedPredicates.isEmpty()) {
            return true;
        } else {
            // If it is waiting to be scheduled, it is in ready.
            for (Predicate predicate : awaitedPredicates) {
                if (!predicate.getType().equals(Predicate.TYPE.JOB_COMPLETION)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Is the instance scheduled for execution?
     *
     * @return - true if it is scheduled and has not yet completed.
     * @throws FalconException
     */
    public boolean isScheduled() throws FalconException {
        return getExternalID() != null && dagEngine.isScheduled(this);
    }

    /**
     * Has the instance timed out waiting for gating conditions to be met?
     *
     * @return
     */
    public boolean hasTimedout() {
        return hasTimedOut || (getTimeOutInMillis() <= (System.currentTimeMillis() - getCreationTime().getMillis()));
    }

    @Override
    public InstanceID getId() {
        return id;
    }

    @Override
    public Entity getEntity() {
        return process;
    }

    @Override
    public int getInstanceSequence() {
        return instanceSequence;
    }

    @Override
    public void setAwaitingPredicates(List<Predicate> predicates) {
        this.awaitedPredicates = predicates;
    }

    @Override
    public List<Predicate> getAwaitingPredicates() {
        return awaitedPredicates;
    }

    @Override
    public void setInstanceSequence(int sequence) {
        this.instanceSequence = sequence;
    }

    @Override
    public void suspend() throws FalconException {
        if (getExternalID() != null) {
            dagEngine.suspend(this);
        }
        destroy();
    }

    @Override
    public void resume() throws FalconException {
        // Was already scheduled on the DAGEngine, so resume on DAGEngine if suspended
        if (getExternalID() != null) {
            dagEngine.resume(this);
        } else if (awaitedPredicates != null && !awaitedPredicates.isEmpty()) {
            // Evaluate any remaining predicates
            registerForNotifications(true);
        }
    }

    @Override
    public void kill() throws FalconException {
        if (getExternalID() != null) {
            dagEngine.kill(this);
        }
        destroy();
    }

    // If timeout specified in process, uses it.
    // Else, defaults to frequency of the entity * timeoutFactor
    private long getTimeOutInMillis() {
        if (process.getTimeout() == null) {
            // Default timeout is the frequency of the entity
            int timeoutFactor = Integer.parseInt(RuntimeProperties.get().getProperty("instance.timeout.factor",
                    "1"));
            return SchedulerUtil.getFrequencyInMillis(DateTime.now(), process.getFrequency()) * timeoutFactor;
        } else {
            // TODO : Should timeout = 0 have a special meaning or should it be disallowed?
            return SchedulerUtil.getFrequencyInMillis(DateTime.now(), process.getTimeout());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !o.getClass().equals(this.getClass())) {
            return false;
        }

        ProcessExecutionInstance processExecutionInstance = (ProcessExecutionInstance) o;

        return  this.getId().equals(processExecutionInstance.getId())
                && Predicate.isEqualAwaitingPredicates(this.getAwaitingPredicates(),
                    processExecutionInstance.getAwaitingPredicates())
                && this.getInstanceSequence() == (processExecutionInstance.getInstanceSequence());
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (awaitedPredicates != null ? awaitedPredicates.hashCode() : 0);
        result = 31 * result + instanceSequence;
        return result;
    }

    @Override
    public void destroy() throws FalconException {
        NotificationServicesRegistry.unregister(executionService, getId());
    }
}

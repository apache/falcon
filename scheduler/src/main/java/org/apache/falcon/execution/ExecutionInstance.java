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
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.predicate.Predicate;
import org.apache.falcon.state.InstanceID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

/**
 * Represents an execution instance of an entity.
 */
public abstract class ExecutionInstance implements NotificationHandler {

    // TODO : Add more fields
    private final String cluster;
    // External ID is the ID used to identify the Job submitted to the DAG Engine, as returned by the DAG Engine.
    // For example, for Oozie this would be the workflow Id.
    private String externalID;
    // Time at which instance has to be run.
    private final DateTime instanceTime;
    // Time at which instance is created.
    private final DateTime creationTime;
    private DateTime actualStart;
    private DateTime actualEnd;
    protected static final DateTimeZone UTC = DateTimeZone.UTC;

    /**
     * @param instanceTime Time at which instance has to be run.
     * @param cluster
     * @param creationTime Time at which instance is created to run.
     */
    public ExecutionInstance(DateTime instanceTime, String cluster, DateTime creationTime) {
        this.instanceTime = new DateTime(instanceTime, UTC);
        this.cluster = cluster;
        this.creationTime = new DateTime(creationTime, UTC);
    }

    /**
     * @param instanceTime
     * @param cluster
     */
    public ExecutionInstance(DateTime instanceTime, String cluster) {
        this(instanceTime, cluster, DateTime.now());
    }

    /**
     * For a-periodic instances.
     * @param cluster
     */
    public ExecutionInstance(String cluster) {
        this.instanceTime = DateTime.now();
        this.cluster = cluster;
        this.creationTime = DateTime.now(UTC);
    }

    /**
     * @return - The external id corresponding to this instance.
     * If the instance is executed on Oozie, externalID will the Oozie workflow ID.
     */
    public String getExternalID() {
        return externalID;
    }

    /**
     * Setter for external ID, Oozie workflow ID, for example.
     *
     * @param jobID
     */
    public void setExternalID(String jobID) {
        this.externalID = jobID;
    }

    /**
     * @return The unique ID of this instance. The instance is referred using this ID inside the system.
     */
    public abstract InstanceID getId();

    /**
     * @return - The entity to which this instance belongs.
     */
    public abstract Entity getEntity();

    /**
     * @return - The instance time of the instance.
     */
    public DateTime getInstanceTime() {
        return instanceTime;
    }

    /**
     * @return - The name of the cluster on which this instance is running
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * @return - The sequential numerical id of the instance
     */
    public abstract int getInstanceSequence();

    /**
     * @return - Actual start time of instance.
     */
    public DateTime getActualStart() {
        return actualStart;
    }

    /**
     * @param actualStart
     */
    public void setActualStart(DateTime actualStart) {
        this.actualStart = actualStart;
    }

    /**
     * @return - Completion time of the instance
     */
    public DateTime getActualEnd() {
        return actualEnd;
    }

    /**
     * @param actualEnd
     */
    public void setActualEnd(DateTime actualEnd) {
        this.actualEnd = actualEnd;
    }

    /**
     * Creation time of an instance.
     * @return
     */
    public DateTime getCreationTime() {
        return creationTime;
    }

    /**
     * Set the gating conditions on which this instance is waiting before it is scheduled for execution.
     * @param predicates
     */
    public abstract void setAwaitingPredicates(List<Predicate> predicates);

    /**
     * @return - The gating conditions on which this instance is waiting before it is scheduled for execution.
     * @throws FalconException
     */
    public abstract List<Predicate> getAwaitingPredicates();

    /**
     * set the sequential numerical id of the instance.
     */
    public abstract void setInstanceSequence(int sequence);


    /**
     * Suspends the instance if it is in one of the active states, waiting, ready or running.
     *
     * @throws FalconException
     */
    public abstract void suspend() throws FalconException;

    /**
     * Resumes a previously suspended instance.
     *
     * @throws FalconException
     */
    public abstract void resume() throws FalconException;

    /**
     * Kills an instance if it is in one of the active states, waiting, ready or running.
     *
     * @throws FalconException
     */
    public abstract void kill() throws FalconException;

    /**
     * Handles any clean up and de-registration of notification subscriptions.
     * Invoked when the instance reaches one of its terminal states.
     *
     * @throws FalconException
     */
    public abstract void destroy() throws FalconException;
}

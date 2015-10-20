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
import org.apache.falcon.state.ID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.TimeZone;

/**
 * Represents an execution instance of an entity.
 */
public abstract class ExecutionInstance implements NotificationHandler {

    // TODO : Add more fields
    private final String cluster;
    // External ID is the ID used to identify the Job submitted to the DAG Engine, as returned by the DAG Engine.
    // For example, for Oozie this would be the workflow Id.
    private String externalID;
    private final DateTime instanceTime;
    private final DateTime creationTime;
    private DateTime actualStart;
    private DateTime actualEnd;
    private static final DateTimeZone UTC = DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC"));

    /**
     * @param instanceTime
     * @param cluster
     */
    public ExecutionInstance(DateTime instanceTime, String cluster) {
        this.instanceTime = new DateTime(instanceTime, UTC);
        this.cluster = cluster;
        this.creationTime = DateTime.now(UTC);
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
    public abstract ID getId();

    /**
     * @return - The entity to which this instance belongs.
     */
    public abstract Entity getEntity();

    /**
     * @return - The nominal time of the instance.
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


    public DateTime getCreationTime() {
        return creationTime;
    }

    /**
     * @return - The gating conditions on which this instance is waiting before it is scheduled for execution.
     * @throws FalconException
     */
    public abstract List<Predicate> getAwaitingPredicates() throws FalconException;

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

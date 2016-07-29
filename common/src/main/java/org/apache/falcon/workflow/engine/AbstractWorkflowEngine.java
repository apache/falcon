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

import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * Workflow engine should minimally support the
 * following operations.
 */
public abstract class AbstractWorkflowEngine {

    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    protected Set<WorkflowEngineActionListener> listeners = new HashSet<WorkflowEngineActionListener>();

    public void registerListener(WorkflowEngineActionListener listener) {
        listeners.add(listener);
    }

    public abstract boolean isAlive(Cluster cluster) throws FalconException;

    public abstract void schedule(Entity entity, Boolean skipDryRun, Map<String, String> properties)
        throws FalconException;

    public abstract String suspend(Entity entity) throws FalconException;

    public abstract String resume(Entity entity) throws FalconException;

    public abstract String delete(Entity entity) throws FalconException;

    public abstract String delete(Entity entity, String cluster) throws FalconException;

    public abstract String reRun(String cluster, String wfId, Properties props, boolean isForced)
        throws FalconException;

    public abstract void dryRun(Entity entity, String clusterName, Boolean skipDryRun) throws FalconException;

    public abstract boolean isActive(Entity entity) throws FalconException;

    public abstract boolean isSuspended(Entity entity) throws FalconException;

    public abstract boolean isCompleted(Entity entity) throws FalconException;

    public abstract boolean isMissing(Entity entity) throws FalconException;

    public abstract InstancesResult getRunningInstances(Entity entity,
                                                        List<LifeCycle> lifeCycles) throws FalconException;

    public abstract InstancesResult killInstances(Entity entity, Date start, Date end, Properties props,
                                                  List<LifeCycle> lifeCycles) throws FalconException;

    public abstract InstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props,
                                                   List<LifeCycle> lifeCycles, Boolean isForced) throws FalconException;

    public abstract InstancesResult suspendInstances(Entity entity, Date start, Date end, Properties props,
                                                     List<LifeCycle> lifeCycles) throws FalconException;

    public abstract InstancesResult resumeInstances(Entity entity, Date start, Date end, Properties props,
                                                    List<LifeCycle> lifeCycles) throws FalconException;

    public abstract InstancesResult getStatus(Entity entity, Date start, Date end,
                                              List<LifeCycle> lifeCycles, Boolean allAttempts) throws FalconException;

    public abstract InstancesSummaryResult getSummary(Entity entity, Date start, Date end,
                                                      List<LifeCycle> lifeCycles) throws FalconException;

    public abstract String update(Entity oldEntity, Entity newEntity,
                                  String cluster, Boolean skipDryRun) throws FalconException;

    public abstract String touch(Entity entity, String cluster, Boolean skipDryRun) throws FalconException;

    public abstract String getWorkflowStatus(String cluster, String jobId) throws FalconException;

    public abstract Properties getWorkflowProperties(String cluster, String jobId) throws FalconException;

    public abstract InstancesResult getJobDetails(String cluster, String jobId) throws FalconException;

    public abstract InstancesResult getInstanceParams(Entity entity, Date start, Date end,
                                                      List<LifeCycle> lifeCycles) throws FalconException;

    public abstract boolean isNotificationEnabled(String cluster, String jobID) throws FalconException;

    public abstract Boolean isWorkflowKilledByUser(String cluster, String jobId) throws FalconException;


    /**
     * Returns the short name of the Workflow Engine.
     * @return
     */
    public abstract String getName();
}

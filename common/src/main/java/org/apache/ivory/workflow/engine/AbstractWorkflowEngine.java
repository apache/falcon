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

import java.util.*;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.resource.InstancesResult;

/**
 * Workflow engine should minimally support the
 * following operations
 */
public abstract class AbstractWorkflowEngine {
	
    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    protected Set<WorkflowEngineActionListener> listeners = new HashSet<WorkflowEngineActionListener>();

    public void registerListener(WorkflowEngineActionListener listener) {
        listeners.add(listener);
    }

    public abstract void schedule(Entity entity) throws IvoryException;

    public abstract String suspend(Entity entity) throws IvoryException;

    public abstract String resume(Entity entity) throws IvoryException;

    public abstract String delete(Entity entity) throws IvoryException;
    
    public abstract void reRun(String cluster, String wfId, Properties props) throws IvoryException;

    public abstract boolean isActive(Entity entity) throws IvoryException;

    public abstract boolean isSuspended(Entity entity) throws IvoryException;

    public abstract InstancesResult getRunningInstances(Entity entity) throws IvoryException;
    
    public abstract InstancesResult killInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;
    
    public abstract InstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    public abstract InstancesResult suspendInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    public abstract InstancesResult resumeInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    public abstract InstancesResult getStatus(Entity entity, Date start, Date end) throws IvoryException;

	public abstract void update(Entity oldEntity, Entity newEntity) throws IvoryException;

	public abstract String getWorkflowStatus(String cluster, String jobId) throws IvoryException;
	
	public abstract String getWorkflowProperty(String cluster, String jobId , String property) throws IvoryException;
	
	public abstract InstancesResult  getJobDetails(String cluster, String jobId) throws IvoryException;
}

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

import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.resource.InstancesResult;

/**
 * Workflow engine should minimally support the
 * following operations
 */
public interface WorkflowEngine {
	
    public static final String NAME_NODE = "nameNode";
    public static final String JOB_TRACKER = "jobTracker";

    void schedule(Entity entity) throws IvoryException;

    String suspend(Entity entity) throws IvoryException;

    String resume(Entity entity) throws IvoryException;

    String delete(Entity entity) throws IvoryException;
    
    void reRun(String cluster, String wfId, Properties props) throws IvoryException;

    boolean isActive(Entity entity) throws IvoryException;

    boolean isSuspended(Entity entity) throws IvoryException;

    InstancesResult getRunningInstances(Entity entity) throws IvoryException;
    
    InstancesResult killInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;
    
    InstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    InstancesResult suspendInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    InstancesResult resumeInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    InstancesResult getStatus(Entity entity, Date start, Date end) throws IvoryException;

	void update(Entity oldEntity, Entity newEntity) throws IvoryException;

	String getWorkflowStatus(String cluster, String jobId) throws IvoryException;
	
	String getWorkflowProperty(String cluster, String jobId , String property) throws IvoryException;
	
	InstancesResult  getJobDetails(String cluster, String jobId) throws IvoryException;

}

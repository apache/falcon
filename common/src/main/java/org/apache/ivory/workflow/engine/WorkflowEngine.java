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
import java.util.Properties;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.resource.ProcessInstancesResult;

/**
 * Workflow engine should minimally support the
 * following operations
 */
public interface WorkflowEngine {

    void schedule(Entity entity) throws IvoryException;

    String suspend(Entity entity) throws IvoryException;

    String resume(Entity entity) throws IvoryException;

    String delete(Entity entity) throws IvoryException;
    
    void reRun(String cluster, String wfId, Properties props) throws IvoryException;

    boolean isActive(Entity entity) throws IvoryException;

    boolean isSuspended(Entity entity) throws IvoryException;

    ProcessInstancesResult getRunningInstances(Entity entity) throws IvoryException;
    
    ProcessInstancesResult killInstances(Entity entity, Date start, Date end) throws IvoryException;
    
    ProcessInstancesResult reRunInstances(Entity entity, Date start, Date end, Properties props) throws IvoryException;

    ProcessInstancesResult suspendInstances(Entity entity, Date start, Date end) throws IvoryException;

    ProcessInstancesResult resumeInstances(Entity entity, Date start, Date end) throws IvoryException;

    ProcessInstancesResult getStatus(Entity entity, Date start, Date end) throws IvoryException;

	void update(Entity oldEntity, Entity newEntity) throws IvoryException;

	String instanceStatus(String cluster, String jobId) throws IvoryException;
}
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

package org.apache.ivory.workflow;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.util.ReflectionUtils;

public abstract class WorkflowBuilder<T extends Entity> {

    public static final String PROPS = "PROPS";
    public static final String CLUSTERS = "CLUSTERS";
    
    public static WorkflowBuilder getBuilder(String engine, Entity entity)
            throws IvoryException {
        String classKey = engine + "." + entity.getEntityType().name().toLowerCase() +
                ".workflow.builder";
        return ReflectionUtils.getInstance(classKey);
    }

    public abstract Map<String, Object> newWorkflowSchedule(T entity)
            throws IvoryException;

    //TODO add methods for re-run, whenever additional work is required
    //beyond just firing an action in the actual workflow engine

    public abstract List<ExternalId> getExternalIds(T entity, Date start, Date end) throws IvoryException;
    
    public abstract List<ExternalId> getExternalIdsForRerun(T entity, Date start, Date end) throws IvoryException;

    public abstract List<ExternalId> getMappedExternalIds(Entity entity, ExternalId extId) throws IvoryException;
}
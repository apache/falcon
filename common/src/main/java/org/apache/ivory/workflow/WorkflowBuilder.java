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

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.util.StartupProperties;

import java.util.Map;

@SuppressWarnings("unchecked")
public abstract class WorkflowBuilder {

    public static final String PROPS = "PROPS";
    public static final String CLUSTERS = "CLUSTERS";

    public static WorkflowBuilder getBuilder(String engine, Entity entity)
            throws IvoryException {
        String classKey = engine + "." + entity.getEntityType().name().toLowerCase() +
                ".workflow.builder";
        try {
            String clazzName = StartupProperties.get().getProperty(classKey);
            Class<WorkflowBuilder> clazz = (Class<WorkflowBuilder>)
                    WorkflowBuilder.class.getClassLoader().loadClass(clazzName);
            return clazz.newInstance();
        } catch (Exception e) {
            throw new IvoryException("Unable to get workflow builder for " +
                    classKey, e);
        }
    }

    public abstract Map<String, Object> newWorkflowSchedule(Entity entity)
            throws IvoryException;

    public abstract Cluster[] getScheduledClustersFor(Entity entity) throws IvoryException;
    //TODO add methods for re-run, whenever additional work is required
    //beyond just firing an action in the actual workflow engine
}

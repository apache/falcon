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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.util.DeploymentUtil;
import org.apache.ivory.util.ReflectionUtils;

public abstract class WorkflowBuilder<T extends Entity> {

    public static final String PROPS = "PROPS";
    public static final String CLUSTERS = "CLUSTERS";

    public static WorkflowBuilder getBuilder(String engine, Entity entity) throws IvoryException {
        String classKey = engine + "." + entity.getEntityType().name().toLowerCase() + ".workflow.builder";
        return ReflectionUtils.getInstance(classKey);
    }

    public abstract Map<String, Properties> newWorkflowSchedule(T entity, List<String> clusters) throws IvoryException;

    public String[] getClustersDefined(T entity) {
        String[] entityClusters = EntityUtil.getClustersDefined(entity);
        if (DeploymentUtil.isEmbeddedMode())
            return entityClusters;

        Set<String> myClusters = DeploymentUtil.getCurrentClusters();
        Set<String> applicableClusters = new HashSet<String>();
        for (String cluster : entityClusters)
            if (myClusters.contains(cluster))
                applicableClusters.add(cluster);
        return applicableClusters.toArray(new String[] {});

    }
    
    public abstract String[] getWorkflowNames(T entity);
}
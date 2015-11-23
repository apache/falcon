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

package org.apache.falcon.state;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;

/**
 * A unique ID for an entity, cluster pair.
 * This is required in scenarios like scheduling an entity per cluster.
 */
public class EntityClusterID extends ID {

    private final String clusterName;

    public EntityClusterID(Entity entity, String clusterName) {
        this(entity.getEntityType(), entity.getName(), clusterName);
    }

    public EntityClusterID(EntityType entityType, String entityName, String clusterName) {
        this.entityName = entityName;
        this.entityType = entityType;
        this.clusterName = clusterName;
        this.key = this.entityType + KEY_SEPARATOR + this.entityName + KEY_SEPARATOR + clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public EntityID getEntityID() {
        return new EntityID(entityType, entityName);
    }
}

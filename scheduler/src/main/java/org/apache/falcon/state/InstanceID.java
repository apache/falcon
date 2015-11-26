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
import org.apache.falcon.execution.ExecutionInstance;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A unique ID for a given(wrapped) instance.
 * An instance is the execution unit of an entity and can be uniquely defined by
 * (entity, cluster, instanceTime).
 */
public class InstanceID extends ID {
    public static final String INSTANCE_FORMAT = "yyyy-MM-dd-HH-mm";

    /**
     * Name of the cluster for the instance.
     */
    private String clusterName;

    /**
     *
     */
    private DateTime instanceTime;

    public InstanceID(EntityType entityType, String entityName, String clusterName, DateTime instanceTime) {
        this.entityType = entityType;
        this.entityName = entityName;
        this.clusterName = clusterName;
        this.instanceTime = new DateTime(instanceTime);
        DateTimeFormatter fmt = DateTimeFormat.forPattern(INSTANCE_FORMAT);
        this.key = this.entityType + KEY_SEPARATOR + this.entityName + KEY_SEPARATOR + this.clusterName
                + KEY_SEPARATOR + fmt.print(instanceTime);
    }

    public InstanceID(Entity entity, String clusterName, DateTime instanceTime) {
        this(entity.getEntityType(), entity.getName(), clusterName, instanceTime);
    }


    public InstanceID(ExecutionInstance instance) {
        this(instance.getEntity(), instance.getCluster(), instance.getInstanceTime());
        assert instance.getInstanceTime() != null : "Instance time cannot be null.";
    }

    public String getClusterName() {
        return clusterName;
    }

    public DateTime getInstanceTime() {
        return instanceTime;
    }

    @Override
    public EntityID getEntityID() {
        return new EntityID(entityType, entityName);
    }


    public EntityClusterID getEntityClusterID() {
        return new EntityClusterID(entityType, entityName, clusterName);
    }

    public static EntityType getEntityType(String id) {
        if (id == null) {
            return null;
        }
        String[] values = id.split(KEY_SEPARATOR);
        String entityType = values[0];
        return EntityType.valueOf(entityType);
    }

    public static String getEntityName(String id) {
        if (id == null) {
            return null;
        }
        String[] values = id.split(KEY_SEPARATOR);
        String entityName = values[1];
        return entityName;
    }

}

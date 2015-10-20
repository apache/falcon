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
import org.datanucleus.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

/**
 * A serializable, comparable ID used to uniquely represent an entity or an instance.
 */
public final class ID implements Serializable, Comparable<ID> {
    public static final String KEY_SEPARATOR = "/";
    public static final String INSTANCE_FORMAT = "yyyy-MM-dd-HH-mm";

    private String entityName;
    private EntityType entityType;
    private String entityKey;
    private String cluster;
    private DateTime instanceTime;

    /**
     * Default Constructor.
     */
    public ID(){}

    /**
     * Constructor.
     *
     * @param type
     * @param name
     */
    public ID(EntityType type, String name) {
        assert type != null : "Entity type must be present.";
        assert !StringUtils.isEmpty(name) : "Entity name must be present.";
        this.entityName = name;
        this.entityType = type;
        this.entityKey = entityType + KEY_SEPARATOR + entityName;
    }

    /**
     * Constructor.
     *
     * @param entity
     */
    public ID(Entity entity) {
        this(entity.getEntityType(), entity.getName());
    }

    /**
     * Constructor.
     *
     * @param entity
     * @param cluster
     */
    public ID(Entity entity, String cluster) {
        this(entity.getEntityType(), entity.getName());
        assert !StringUtils.isEmpty(cluster) : "Cluster cannot be empty.";
        this.cluster = cluster;
    }

    /**
     * Constructor.
     *
     * @param instance
     */
    public ID(ExecutionInstance instance) {
        this(instance.getEntity(), instance.getCluster());
        assert instance.getInstanceTime() != null : "Nominal time cannot be null.";
        this.instanceTime = instance.getInstanceTime();
    }

    /**
     * Constructor.
     *
     * @param entity
     * @param cluster
     * @param instanceTime
     */
    public ID(Entity entity, String cluster, DateTime instanceTime) {
        this(entity, cluster);
        assert instanceTime != null : "Nominal time cannot be null.";
        this.instanceTime = instanceTime;
    }

    /**
     * @return cluster name
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * @param cluster name
     */
    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    /**
     * @return nominal time
     */
    public DateTime getInstanceTime() {
        return instanceTime;
    }

    /**
     * @param instanceTime
     */
    public void setInstanceTime(DateTime instanceTime) {
        this.instanceTime = instanceTime;
    }

    /**
     * @return entity name
     */
    public String getEntityName() {
        return entityName;
    }

    /**
     * @return entity type
     */
    public EntityType getEntityType() {
        return entityType;
    }

    @Override
    public String toString() {
        String val = entityKey;
        if (!StringUtils.isEmpty(cluster)) {
            val = val + KEY_SEPARATOR + cluster;
        }

        if (instanceTime != null) {
            DateTimeFormatter fmt = DateTimeFormat.forPattern(INSTANCE_FORMAT);
            val = val + KEY_SEPARATOR + fmt.print(instanceTime);
        }
        return val;
    }

    /**
     * @return An ID without the cluster name
     */
    public String getEntityKey() {
        return entityKey;
    }

    /**
     * @return ID without the instance information
     */
    public ID getEntityID() {
        ID newID = new ID(this.entityType, this.entityName);
        newID.setCluster(this.cluster);
        newID.setInstanceTime(null);
        return newID;
    }

    @Override
    public boolean equals(Object id) {
        if (id == null || id.getClass() != getClass()) {
            return false;
        }
        return compareTo((ID)id) == 0;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public int compareTo(ID id) {
        if (id == null) {
            return -1;
        }
        return this.toString().compareTo(id.toString());
    }
}

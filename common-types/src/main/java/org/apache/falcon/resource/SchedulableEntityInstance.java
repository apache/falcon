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

package org.apache.falcon.resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;

import java.util.Date;

/**
 * Instance of a Schedulable Entity (Feed/Process).
 */
public class SchedulableEntityInstance implements Comparable<SchedulableEntityInstance> {

    public static final String INPUT = "Input";
    public static final String OUTPUT = "Output";

    private String entityName;

    private String cluster;

    private Date instanceTime;

    private EntityType entityType;

    private String tags;

    //for JAXB
    private SchedulableEntityInstance() {

    }

    public SchedulableEntityInstance(String entityName, String cluster, Date instanceTime, EntityType type) {
        this.entityName = entityName;
        this.cluster = cluster;
        this.entityType = type;
        if (instanceTime != null) {
            this.instanceTime = new Date(instanceTime.getTime());
        }
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public Date getInstanceTime() {
        return new Date(instanceTime.getTime());
    }

    public void setInstanceTime(Date instanceTime) {
        this.instanceTime = new Date(instanceTime.getTime());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: " + entityName
                + ", type: " + entityType
                + ", cluster: " + cluster
                + ", instanceTime: " + SchemaHelper.formatDateUTC(instanceTime));
        sb.append(", tags: " + ((tags != null) ? tags : ""));
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchedulableEntityInstance that = (SchedulableEntityInstance) o;

        if (instanceTime == null ? that.instanceTime != null : !instanceTime.equals(that.instanceTime)) {
            return false;
        }

        if (!entityType.equals(that.entityType)) {
            return false;
        }

        if (!StringUtils.equals(entityName, that.entityName)) {
            return false;
        }

        if (!StringUtils.equals(cluster, that.cluster)) {
            return false;
        }

        if (!StringUtils.equals(tags, that.tags)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = instanceTime.hashCode();
        result = 31 * result + entityName.hashCode();
        result = 31 * result + entityType.hashCode();
        result = 31 * result + cluster.hashCode();
        if (tags != null) {
            result = 31 * result + tags.hashCode();
        }
        return result;
    }

    @Override
    public int compareTo(SchedulableEntityInstance o) {
        int result = this.cluster.compareTo(o.cluster);
        if (result != 0) {
            return result;
        }

        result = this.entityType.compareTo(o.entityType);
        if (result != 0) {
            return result;
        }

        result = this.entityName.compareToIgnoreCase(o.entityName);
        if (result != 0) {
            return result;
        }

        return this.instanceTime.compareTo(o.instanceTime);
    }
}

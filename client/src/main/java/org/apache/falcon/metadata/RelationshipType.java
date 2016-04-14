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

package org.apache.falcon.metadata;

import org.apache.falcon.entity.v0.EntityType;

/**
 * Enumerates Relationship types.
 */
public enum RelationshipType {

    // entity vertex types
    CLUSTER_ENTITY("cluster-entity"),
    FEED_ENTITY("feed-entity"),
    PROCESS_ENTITY("process-entity"),
    DATASOURCE_ENTITY("datasource-entity"),

    // instance vertex types
    FEED_INSTANCE("feed-instance"),
    PROCESS_INSTANCE("process-instance"),
    IMPORT_INSTANCE("import-instance"),

    // Misc vertex types
    USER("user"),
    COLO("data-center"),
    TAGS("classification"),
    GROUPS("group"),
    PIPELINES("pipelines"),
    REPLICATION_METRICS("replication-metrics");


    private final String name;

    RelationshipType(java.lang.String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static RelationshipType fromString(String value) {
        if (value != null) {
            for (RelationshipType type : RelationshipType.values()) {
                if (value.equals(type.getName())) {
                    return type;
                }
            }
        }

        throw new IllegalArgumentException("No constant with value " + value + " found");
    }

    public static RelationshipType fromSchedulableEntityType(String type) {
        EntityType entityType = EntityType.getEnum(type);
        switch (entityType) {
        case FEED:
            return RelationshipType.FEED_ENTITY;
        case PROCESS:
            return RelationshipType.PROCESS_ENTITY;
        default:
            throw new IllegalArgumentException("Invalid schedulable entity type: " + entityType.name());
        }
    }
}

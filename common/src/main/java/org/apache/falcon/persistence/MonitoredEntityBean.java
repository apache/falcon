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
package org.apache.falcon.persistence;

import org.apache.falcon.entity.v0.EntityType;

import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Column;
import javax.persistence.Basic;
import javax.validation.constraints.NotNull;
import java.util.Date;

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
* The Entities that are to be monitored will be stored in MONITORED_ENTITY table.
* */

@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_MONITORED_ENTITY, query = "select OBJECT(a) from "
                + "MonitoredEntityBean a where a.entityName = :entityName and a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.DELETE_MONITORED_ENTITIES, query = "delete from MonitoredEntityBean "
                + "a where a.entityName = :entityName and a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.GET_ALL_MONITORING_ENTITIES_FOR_TYPE, query = "select OBJECT(a) "
                + "from MonitoredEntityBean a where a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.GET_ALL_MONITORING_ENTITY, query = "select OBJECT(a) "
                + "from MonitoredEntityBean a"),
        @NamedQuery(name = PersistenceConstants.UPDATE_LAST_MONITORED_TIME, query = "update MonitoredEntityBean a "
                + "set a.lastMonitoredTime = :lastMonitoredTime where a.entityName = :entityName and a.entityType = "
                + ":entityType")
})
@Table(name="MONITORED_ENTITY")
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class MonitoredEntityBean {
    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @Basic
    @NotNull
    @Column(name = "entity_name")
    private String entityName;

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        EntityType.assertSchedulable(entityType);
        this.entityType = entityType.toLowerCase();
    }

    @Basic
    @NotNull
    @Column(name = "entity_type")
    private String entityType;

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    @Basic
    @NotNull
    @Column(name = "last_monitored_time")
    private Date lastMonitoredTime;

    public Date getLastMonitoredTime() {
        return lastMonitoredTime;
    }

    public void setLastMonitoredTime(Date lastMonitoredTime) {
        this.lastMonitoredTime = lastMonitoredTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public static final String ENTITY_NAME = "entityName";

    public static final String ENTITY_TYPE = "entityType";

    public static final String LAST_MONITORED_TIME = "lastMonitoredTime";

}

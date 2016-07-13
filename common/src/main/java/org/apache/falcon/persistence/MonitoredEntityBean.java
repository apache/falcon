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

import org.apache.falcon.FalconException;
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

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
* The Feeds that are to be monitered will be stored in the db.
* */

@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_MONITERED_INSTANCE, query = "select OBJECT(a) from "
                + "MonitoredEntityBean a where a.entityName = :entityName and a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.DELETE_MONITORED_INSTANCES, query = "delete from MonitoredEntityBean "
                + "a where a.entityName = :entityName and a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.GET_ALL_MONITORING_ENTITY_FOR_TYPE, query = "select OBJECT(a) "
                + "from MonitoredEntityBean a where a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.GET_ALL_MONITORING_ENTITY, query = "select OBJECT(a) "
                + "from MonitoredEntityBean a")
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

    public void setEntityType(String entityType) throws FalconException {
        checkEntityType(entityType);
        this.entityType = entityType;
    }

    @Basic
    @NotNull
    @Column(name = "entity_type")
    private String entityType;

    public String getFeedName() {
        return entityName;
    }

    public void setEntityName(String feedName) {
        this.entityName = feedName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public static final String ENTITYNAME = "entityName";

    public static final String ENTITYTYPE = "entityType";

    void checkEntityType(String entityType)throws FalconException {
        if (entityType.equals(EntityType.PROCESS.toString()) || entityType.equals(EntityType.FEED.toString())){
            return;
        } else {
            throw new FalconException("EntityType"+ entityType
                    + " is not valid,Feed and Process are the valid input type.");
        }
    }
}

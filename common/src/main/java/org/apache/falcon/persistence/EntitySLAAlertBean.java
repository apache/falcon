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

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
 * Feed SLA monitoring.
 * */
@Entity
@NamedQueries({
@NamedQuery(name = PersistenceConstants.GET_ENTITY_ALERTS, query = "select OBJECT(a) from EntitySLAAlertBean a where a.entityName = :entityName and a.entityType = :entityType"),
@NamedQuery(name = PersistenceConstants.GET_ALL_ENTITY_ALERTS, query = "OBJECT(a) from PendingInstanceBean a "),
@NamedQuery(name = PersistenceConstants.GET_SLA_HIGH_CANDIDATES, query = "select OBJECT(a) from EntitySLAAlertBean a where a.isSLALowMissed = true and a.isSLAHighMissed = false "),
    @NamedQuery(name = PersistenceConstants.UPDATE_SLA_HIGH, query = "update EntitySLAAlertBean a set a.isSLAHighMissed = true where a.entityName = :entityName and a.clusterName = :clusterName and a.nominalTime = :nominalTime and a.entityType = :entityType"),
@NamedQuery(name = PersistenceConstants.GET_ENTITY_ALERT_INSTANCE, query = "select OBJECT(a) from EntitySLAAlertBean a where a.entityName = :entityName and a.clusterName = :clusterName and a.nominalTime = :nominalTime and a.entityType = :entityType"),
 @NamedQuery(name = PersistenceConstants.DELETE_ENTITY_ALERT_INSTANCE, query = "delete from EntitySLAAlertBean a where a.entityName = :entityName and a.clusterName = :clusterName and a.nominalTime = :nominalTime and a.entityType = :entityType")
})
@Table(name = "ENTITY_SLA_ALERTS")
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class EntitySLAAlertBean {
    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @Basic
    @NotNull
    @Column(name = "entity_name")
    private String entityName;

    @Basic
    @NotNull
    @Column(name = "cluster_name")
    private String clusterName;

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

    @Basic
    @NotNull
    @Column(name = "nominal_time")
    private Date nominalTime;

    @Basic
    @Column(name = "sla_low_missed")
    private Boolean isSLALowMissed = false;

    @Basic
    @Column(name = "sla_high_missed")
    private Boolean isSLAHighMissed = false;

    @Basic
    @Column(name = "sla_low_alert_sent")
    private Boolean slaLowAlertSent;


    @Basic
    @Column(name = "sla_high_alert_sent")
    private Boolean slaHighAlertSent;

    public Date getNominalTime() {
        return new Date(nominalTime.getTime());
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTime = new Date(nominalTime.getTime());
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public Boolean getIsSLALowMissed() {
        return isSLALowMissed;
    }

    public void setIsSLALowMissed(Boolean isSLALowMissed) {
        this.isSLALowMissed = isSLALowMissed;
    }

    public Boolean getIsSLAHighMissed() {
        return isSLAHighMissed;
    }

    public void setIsSLAHighMissed(Boolean isSLAHighMissed) {
        this.isSLAHighMissed = isSLAHighMissed;
    }

    public static final String ENTITYNAME = "entityName";

    public static final String CLUSTERNAME = "clusterName";

    public static final String ENTITYTYPE = "entityType";

    public static final String NOMINALTIME = "nominalTime";

}

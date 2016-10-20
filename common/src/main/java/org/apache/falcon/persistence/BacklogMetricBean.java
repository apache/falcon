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

import org.apache.openjpa.persistence.jdbc.Index;

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
import java.util.Date;

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
 * Backlog Metric Object stored in DB.
 */
@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_ALL_BACKLOG_INSTANCES, query = "select  OBJECT(a) from BacklogMetricBean a "),
        @NamedQuery(name = PersistenceConstants.DELETE_BACKLOG_METRIC_INSTANCE, query = "delete from BacklogMetricBean a where a.entityName = :entityName and a.clusterName = :clusterName and a.nominalTime = :nominalTime and a.entityType = :entityType"),
        @NamedQuery(name = PersistenceConstants.DELETE_ALL_BACKLOG_ENTITY_INSTANCES, query = "delete from BacklogMetricBean a where a.entityName = :entityName")
})
//RESUME CHECKSTYLE CHECK  LineLengthCheck

@Table(name = "BACKLOG_METRIC")
public class BacklogMetricBean {

    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @Basic
    @NotNull
    @Index
    @Column(name = "entity_name")
    private String entityName;

    @Basic
    @NotNull
    @Column(name = "cluster_name")
    private String clusterName;

    @Basic
    @NotNull
    @Index
    @Column(name = "nominal_time")
    private Date nominalTime;

    @Basic
    @NotNull
    @Index
    @Column(name = "entity_type")
    private String entityType;


    public String getId() {
        return id;
    }

    public String getEntityName() {
        return entityName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Date getNominalTime() {
        return nominalTime;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }
}

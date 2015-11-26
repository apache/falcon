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
package org.apache.falcon.state.store.jdbc;

import org.apache.openjpa.persistence.jdbc.Index;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.sql.Timestamp;

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
 * Instance State which will be stored in DB.
 */
@Entity
@NamedQueries({
        @NamedQuery(name = "GET_INSTANCE", query = "select OBJECT(a) from InstanceBean a where a.id = :id"),
        @NamedQuery(name = "DELETE_INSTANCE", query = "delete from InstanceBean a where a.id = :id"),
        @NamedQuery(name = "DELETE_INSTANCE_FOR_ENTITY", query = "delete from InstanceBean a where a.entityId = :entityId"),
        @NamedQuery(name = "UPDATE_INSTANCE", query = "update InstanceBean a set a.cluster = :cluster, a.externalID = :externalID, a.instanceTime = :instanceTime, a.creationTime = :creationTime, a.actualEndTime = :actualEndTime, a.currentState = :currentState, a.actualStartTime = :actualStartTime, a.instanceSequence = :instanceSequence, a.awaitedPredicates = :awaitedPredicates where a.id = :id"),
        @NamedQuery(name = "GET_INSTANCES_FOR_ENTITY_CLUSTER", query = "select OBJECT(a) from InstanceBean a where a.entityId = :entityId AND a.cluster = :cluster"),
        @NamedQuery(name = "GET_INSTANCES_FOR_ENTITY_CLUSTER_FOR_STATES", query = "select OBJECT(a) from InstanceBean a where a.entityId = :entityId AND a.cluster = :cluster AND a.currentState IN (:currentState)"),
        @NamedQuery(name = "GET_INSTANCES_FOR_ENTITY_FOR_STATES", query = "select OBJECT(a) from InstanceBean a where a.entityId = :entityId AND a.currentState IN (:currentState)"),
        @NamedQuery(name = "GET_INSTANCES_FOR_ENTITY_FOR_STATES_WITH_RANGE", query = "select OBJECT(a) from InstanceBean a where a.entityId = :entityId AND a.currentState IN (:currentState) AND a.instanceTime >= :startTime AND a.instanceTime < :endTime"),
        @NamedQuery(name = "GET_LAST_INSTANCE_FOR_ENTITY_CLUSTER", query = "select OBJECT(a) from InstanceBean a where a.entityId = :entityId AND a.cluster = :cluster order by a.instanceTime desc"),
        @NamedQuery(name = "DELETE_INSTANCES_TABLE", query = "delete from InstanceBean a")
})
//RESUME CHECKSTYLE CHECK  LineLengthCheck
@Table(name = "INSTANCES")
public class InstanceBean {

    @Id
    @NotNull
    private String id;

    @Basic
    @Index
    @NotNull
    @Column(name = "entity_id")
    private String entityId;

    @Basic
    @Index
    @NotNull
    @Column(name = "cluster")
    private String cluster;

    @Basic
    @Index
    @Column(name = "external_id")
    private String externalID;

    @Basic
    @Index
    @Column(name = "instance_time")
    private Timestamp instanceTime;

    @Basic
    @Index
    @NotNull
    @Column(name = "creation_time")
    private Timestamp creationTime;

    @Basic
    @Column(name = "actual_start_time")
    private Timestamp actualStartTime;

    @Basic
    @Column(name = "actual_end_time")
    private Timestamp actualEndTime;

    @Basic
    @Index
    @NotNull
    @Column(name = "current_state")
    private String currentState;

    @Basic
    @Index
    @NotNull
    @Column(name = "instance_sequence")
    private Integer instanceSequence;


    @Column(name = "awaited_predicates", columnDefinition = "BLOB")
    @Lob
    private byte[] awaitedPredicates;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getExternalID() {
        return externalID;
    }

    public void setExternalID(String externalID) {
        this.externalID = externalID;
    }

    public Timestamp getInstanceTime() {
        return instanceTime;
    }

    public void setInstanceTime(Timestamp instanceTime) {
        this.instanceTime = instanceTime;
    }

    public Timestamp getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Timestamp creationTime) {
        this.creationTime = creationTime;
    }

    public Timestamp getActualStartTime() {
        return actualStartTime;
    }

    public void setActualStartTime(Timestamp actualStartTime) {
        this.actualStartTime = actualStartTime;
    }

    public Timestamp getActualEndTime() {
        return actualEndTime;
    }

    public void setActualEndTime(Timestamp actualEndTime) {
        this.actualEndTime = actualEndTime;
    }

    public String getCurrentState() {
        return currentState;
    }

    public void setCurrentState(String currentState) {
        this.currentState = currentState;
    }

    public byte[] getAwaitedPredicates() {
        return awaitedPredicates;
    }

    public void setAwaitedPredicates(byte[] awaitedPredicates) {
        this.awaitedPredicates = awaitedPredicates;
    }

    public Integer getInstanceSequence() {
        return instanceSequence;
    }

    public void setInstanceSequence(Integer instanceSequence) {
        this.instanceSequence = instanceSequence;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
}

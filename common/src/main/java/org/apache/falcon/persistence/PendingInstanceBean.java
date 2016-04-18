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

import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.GenerationType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.validation.constraints.NotNull;
import java.util.Date;

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
* The instances of feed to be monitored will be stored in db.
* */
@Entity
@NamedQueries({
    @NamedQuery(name = PersistenceConstants.GET_LATEST_INSTANCE_TIME, query = "select max(a.nominalTime) from PendingInstanceBean a where a.feedName = :feedName"),
    @NamedQuery(name = PersistenceConstants.GET_PENDING_INSTANCES, query = "select OBJECT(a) from PendingInstanceBean a where a.feedName = :feedName"),
    @NamedQuery(name = PersistenceConstants.DELETE_PENDING_NOMINAL_INSTANCES , query = "delete from PendingInstanceBean a where a.feedName = :feedName and a.clusterName = :clusterName and a.nominalTime = :nominalTime"),
    @NamedQuery(name = PersistenceConstants.DELETE_ALL_INSTANCES_FOR_FEED, query = "delete from PendingInstanceBean a where a.feedName = :feedName and a.clusterName = :clusterName"),
    @NamedQuery(name = PersistenceConstants.GET_DATE_FOR_PENDING_INSTANCES , query = "select a.nominalTime from PendingInstanceBean a where a.feedName = :feedName and a.clusterName = :clusterName"),
    @NamedQuery(name= PersistenceConstants.GET_ALL_PENDING_INSTANCES , query = "select  OBJECT(a) from PendingInstanceBean a ")
})
@Table(name = "PENDING_INSTANCES")
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class PendingInstanceBean {
    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @Basic
    @NotNull
    @Column(name = "feed_name")
    private String feedName;

    @Basic
    @NotNull
    @Column(name = "cluster_name")
    private String clusterName;

    @Basic
    @NotNull
    @Column(name = "nominal_time")
    private Date nominalTime;

    public Date getNominalTime() {
        return nominalTime;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
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

    public String getFeedName() {
        return feedName;
    }

    public void setFeedName(String feedName) {
        this.feedName = feedName;
    }
}

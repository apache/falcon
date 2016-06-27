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
@NamedQuery(name = PersistenceConstants.GET_FEED_ALERTS, query = "select OBJECT(a) from FeedSLAAlertBean a where a.feedName = :feedName"),
@NamedQuery(name = PersistenceConstants.GET_ALL_FEED_ALERTS, query = "OBJECT(a) from PendingInstanceBean a "),
@NamedQuery(name = PersistenceConstants.GET_SLA_HIGH_CANDIDATES, query = "select OBJECT(a) from FeedSLAAlertBean a where a.isSLALowMissed = true and a.isSLAHighMissed = false "),
    @NamedQuery(name = PersistenceConstants.UPDATE_SLA_HIGH, query = "update FeedSLAAlertBean a set a.isSLAHighMissed = true where a.feedName = :feedName and a.clusterName = :clusterName and a.nominalTime = :nominalTime"),
@NamedQuery(name = PersistenceConstants.GET_FEED_ALERT_INSTANCE, query = "select OBJECT(a) from FeedSLAAlertBean a where a.feedName = :feedName and a.clusterName = :clusterName and a.nominalTime = :nominalTime "),
 @NamedQuery(name = PersistenceConstants.DELETE_FEED_ALERT_INSTANCE, query = "delete from FeedSLAAlertBean a where a.feedName = :feedName and a.clusterName = :clusterName and a.nominalTime = :nominalTime")
})
@Table(name = "FEED_SLA_ALERTS")
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class FeedSLAAlertBean {
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

    public String getFeedName() {
        return feedName;
    }

    public void setFeedName(String feedName) {
        this.feedName = feedName;
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
}

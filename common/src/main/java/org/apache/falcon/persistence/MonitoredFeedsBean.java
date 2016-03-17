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
                + "MonitoredFeedsBean a where a.feedName = :feedName"),
        @NamedQuery(name = PersistenceConstants.DELETE_MONITORED_INSTANCES, query = "delete from MonitoredFeedsBean "
                + "a where a.feedName = :feedName"),
        @NamedQuery(name = PersistenceConstants.GET_ALL_MONITORING_FEEDS, query = "select OBJECT(a) "
                + "from MonitoredFeedsBean a")
})
@Table(name="MONITORED_FEEDS")
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class MonitoredFeedsBean {
    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @Basic
    @NotNull
    @Column(name = "feed_name")
    private String feedName;

    public String getFeedName() {
        return feedName;
    }

    public void setFeedName(String feedName) {
        this.feedName = feedName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}

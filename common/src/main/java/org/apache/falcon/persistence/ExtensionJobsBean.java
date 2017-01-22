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


//SUSPEND CHECKSTYLE CHECK LineLengthCheck

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Table to store extension jobs.
 */

@Table(name = "EXTENSION_JOBS")
@Entity
@NamedQueries({
        @NamedQuery(name = PersistenceConstants.GET_ALL_EXTENSION_JOBS, query = "select OBJECT(a) from ExtensionJobsBean a "),
        @NamedQuery(name = PersistenceConstants.DELETE_EXTENSION_JOB, query = "delete from ExtensionJobsBean a where a.jobName = :jobName "),
        @NamedQuery(name = PersistenceConstants.GET_EXTENSION_JOB, query = "select OBJECT(a) from ExtensionJobsBean a where a.jobName = :jobName"),
        @NamedQuery(name = PersistenceConstants.GET_JOBS_FOR_AN_EXTENSION, query = "select OBJECT(a) from ExtensionJobsBean a where a.extensionName = :extensionName")
})
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class ExtensionJobsBean {

    @Basic
    @NotNull
    @Id
    @Column(name = "job_name")
    private String jobName;

    @Basic
    @NotNull
    @Column(name = "extension_name")
    private String extensionName;

    @Basic
    @NotNull
    @Column(name = "feeds")
    private String[] feeds;

    @Basic
    @NotNull
    @Column(name = "processes")
    private String[] processes;

    @Lob
    @Basic(fetch= FetchType.LAZY)
    @Column(name = "config")
    private byte[] config;


    @Basic
    @NotNull
    @Column(name = "creation_time")
    private Date creationTime;

    @Basic
    @NotNull
    @Column(name = "last_updated_time")
    private Date lastUpdatedTime;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
    }

    public byte[] getConfig() {
        return config;
    }

    public void setConfig(byte[] config) {
        this.config = config;
    }

    public String getExtensionName() {
        return extensionName;
    }

    public void setExtensionName(String extensionName) {
        this.extensionName = extensionName;
    }

    public Date getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Date lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public List<String> getFeeds() {
        return Arrays.asList(feeds);
    }

    public void setFeeds(List<String> feeds) {
        this.feeds = feeds.toArray(new String[feeds.size()]);
    }

    public List<String> getProcesses() {
        return Arrays.asList(processes);
    }

    public void setProcesses(List<String> processes) {
        this.processes = processes.toArray(new String[processes.size()]);
    }
}

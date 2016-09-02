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
import javax.validation.constraints.NotNull;
import java.util.Date;

//SUSPEND CHECKSTYLE CHECK LineLengthCheck
/**
 * Class to store info regarding process history.
 */
@Entity
@NamedQueries({
        @NamedQuery(name= PersistenceConstants.GET_ALL_PROCESS_INFO_INSTANCES , query = "select  OBJECT(a) from ProcessInstanceInfoBean a ")
})
@Table(name = "ProcessInstanceInfo")
//RESUME CHECKSTYLE CHECK  LineLengthCheck
public class ProcessInstanceInfoBean {
    @NotNull
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Id
    private String id;

    @NotNull
    @Column(name = "process_name")
    private String processName;

    @NotNull
    @Column(name = "colo")
    private String colo;

    public String getPipeline() {
        return pipeline;
    }

    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    @NotNull
    @Column(name = "pipeline")
    private String pipeline;

    @NotNull
    @Column(name = "status")
    private String status;

    @NotNull
    @Column(name = "nominal_time")
    private Date nominalTime;

    @NotNull
    @Column(name = "start_delay")
    private long startDelay;

    @NotNull
    @Column(name = "processing_time")
    private long processingTime;

    public Date getNominalTime() {
        return nominalTime;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTime = nominalTime;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getColo() {
        return colo;
    }

    public void setColo(String colo) {
        this.colo = colo;
    }

    public long getStartDelay() {
        return startDelay;
    }

    public void setStartDelay(long startDelay) {
        this.startDelay = startDelay;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(long processingTime) {
        this.processingTime = processingTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

}

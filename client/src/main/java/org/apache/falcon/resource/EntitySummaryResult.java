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

package org.apache.falcon.resource;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;
import java.util.Date;

/**
 * Pojo for JAXB marshalling / unmarshalling.
 */
//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class EntitySummaryResult extends APIResult {

    /**
     * Workflow status as being set in result object.
     */
    public static enum WorkflowStatus {
        WAITING, RUNNING, SUSPENDED, KILLED, FAILED, SUCCEEDED, ERROR
    }

    @XmlElement
    private EntitySummary[] entitySummaries;

    //For JAXB
    public EntitySummaryResult() {
        super();
    }

    public EntitySummaryResult(String message, EntitySummary[] entitySummaries) {
        this(Status.SUCCEEDED, message, entitySummaries);
    }

    public EntitySummaryResult(Status status, String message, EntitySummary[] entitySummaries) {
        super(status, message);
        this.entitySummaries = entitySummaries;
    }

    public EntitySummaryResult(Status status, String message) {
        super(status, message);
    }

    public EntitySummary[] getEntitySummaries() {
        return this.entitySummaries;
    }

    public void setEntitySummaries(EntitySummary[] entitySummaries) {
        this.entitySummaries = entitySummaries;
    }

    /**
     * A single entity object inside entity summary result.
     */
    @XmlRootElement(name = "entitySummary")
    public static class EntitySummary {

        @XmlElement
        public String type;
        @XmlElement
        public String name;
        @XmlElement
        public String status;
        @XmlElement
        public String[] tags;
        @XmlElement
        public String[] pipelines;
        @XmlElement
        public Instance[] instances;

        public EntitySummary() {
        }

        public EntitySummary(String entityName, String entityType) {
            this.name = entityName;
            this.type = entityType;
        }

        public EntitySummary(String name, String type, String status,
                             String[] tags, String[] pipelines,
                             Instance[] instances) {
            this.name = name;
            this.type = type;
            this.status = status;
            this.pipelines = pipelines;
            this.tags = tags;
            this.instances = instances;
        }

        public String getName() {
            return this.name;
        }

        public String getType() {
            return this.type;
        }

        public String getStatus() {
            return this.status;
        }

        public String[] getTags() {
            return this.tags;
        }

        public String[] getPipelines() {
            return this.pipelines;
        }

        public Instance[] getInstances() {
            return this.instances;
        }

        @Override
        public String toString() {
            return "{Entity: " + (this.name == null ? "" : this.name)
                    + ", Type: " + (this.type == null ? "" : this.type)
                    + ", Status: " + (this.status == null ? "" : this.status)
                    + ", Tags: " + (this.tags == null ? "[]" : Arrays.toString(this.tags))
                    + ", Pipelines: " + (this.pipelines == null ? "[]" : Arrays.toString(this.pipelines))
                    + ", InstanceSummary: " + (this.instances == null ? "[]" : Arrays.toString(this.instances))
                    +"}";
        }
    }

    /**
     * A single instance object inside instance result.
     */
    @XmlRootElement(name = "instances")
    public static class Instance {
        @XmlElement
        public String instance;

        @XmlElement
        public WorkflowStatus status;

        @XmlElement
        public String logFile;

        @XmlElement
        public String cluster;

        @XmlElement
        public String sourceCluster;

        @XmlElement
        public Date startTime;

        @XmlElement
        public Date endTime;

        public Instance() {
        }

        public Instance(String cluster, String instance, WorkflowStatus status) {
            this.cluster = cluster;
            this.instance = instance;
            this.status = status;
        }

        public String getInstance() {
            return instance;
        }

        public WorkflowStatus getStatus() {
            return status;
        }

        public String getLogFile() {
            return logFile;
        }

        public String getCluster() {
            return cluster;
        }

        public String getSourceCluster() {
            return sourceCluster;
        }

        public Date getStartTime() {
            return startTime;
        }

        public Date getEndTime() {
            return endTime;
        }

        @Override
        public String toString() {
            return "{instance: " + (this.instance == null ? "" : this.instance)
                    + ", status: " + (this.status == null ? "" : this.status)
                    + (this.logFile == null ? "" : ", log: " + this.logFile)
                    + (this.sourceCluster == null ? "" : ", source-cluster: " + this.sourceCluster)
                    + (this.cluster == null ? "" : ", cluster: " + this.cluster)
                    + (this.startTime == null ? "" : ", startTime: " + this.startTime)
                    + (this.endTime == null ? "" : ", endTime: " + this.endTime)
                    + "}";
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck

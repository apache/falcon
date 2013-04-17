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
import java.util.Date;

/**
 * Pojo for JAXB marshalling / unmarshalling.
 */
//SUSPEND CHECKSTYLE CHECK
@XmlRootElement
public class InstancesResult extends APIResult {

    /**
     * Workflow status as being set in result object.
     */
    public static enum WorkflowStatus {
        WAITING, RUNNING, SUSPENDED, KILLED, FAILED, SUCCEEDED, ERROR
    }

    @XmlElement
    private Instance[] instances;

    private InstancesResult() { // for jaxb
        super();
    }

    public InstancesResult(String message, Instance[] instances) {
        this(Status.SUCCEEDED, message, instances);
    }

    public InstancesResult(Status status, String message,
                           Instance[] instanceExes) {
        super(status, message);
        this.instances = instanceExes;
    }

    public InstancesResult(Status status, String message) {
        super(status, message);
    }


    public Instance[] getInstances() {
        return instances;
    }

    public void setInstances(Instance[] instances) {
        this.instances = instances;
    }

    /**
     * A single instance object inside instance result.
     */
    @XmlRootElement(name = "instance")
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

        @XmlElement
        public String details;

        @XmlElement
        public InstanceAction[] actions;

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

        public InstanceAction[] getActions() {
            return actions;
        }

        public String getDetails() {
            return details;
        }


        @Override
        public String toString() {
            return "{instance:"
                    + this.instance
                    + ", status:"
                    + this.status
                    + (this.logFile == null ? "" : ", log:" + this.logFile)
                    + (this.sourceCluster == null ? "" : ", source-cluster:"
                    + this.sourceCluster)
                    + (this.cluster == null ? "" : ", cluster:"
                    + this.cluster) + "}";
        }
    }

    /**
     * Instance action inside an instance object.
     */
    @XmlRootElement(name = "actions")
    public static class InstanceAction {
        @XmlElement
        public String action;
        @XmlElement
        public String status;
        @XmlElement
        public String logFile;

        public InstanceAction() {
        }

        public InstanceAction(String action, String status, String logFile) {
            this.action = action;
            this.status = status;
            this.logFile = logFile;
        }

        public String getAction() {
            return action;
        }

        public String getStatus() {
            return status;
        }

        public String getLogFile() {
            return logFile;
        }

        @Override
        public String toString() {
            return "{action:" + this.action + ", status:" + this.status
                    + (this.logFile == null ? "" : ", log:" + this.logFile)
                    + "}";
        }
    }
}
//RESUME CHECKSTYLE CHECK

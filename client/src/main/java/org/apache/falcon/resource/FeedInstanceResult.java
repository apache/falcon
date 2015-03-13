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

/**
 * Pojo for JAXB marshalling / unmarshalling.
 */
//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class FeedInstanceResult extends APIResult {

    @XmlElement
    private Instance[] instances;

    private FeedInstanceResult() { // for jaxb
        super();
    }

    public FeedInstanceResult(String message, Instance[] instances) {
        this(Status.SUCCEEDED, message, instances);
    }

    public FeedInstanceResult(Status status, String message,
                              Instance[] inInstances) {
        super(status, message);
        this.instances = inInstances;
    }

    public FeedInstanceResult(Status status, String message) {
        super(status, message);
    }

    public Instance[] getInstances() {
        return instances;
    }

    public void setInstances(Instance[] instances) {
        this.instances = instances;
    }

    @Override
    public Object[] getCollection() {
        return getInstances();
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setInstances(new Instance[0]);
        } else {
            Instance[] newInstances = new Instance[items.length];
            for (int index = 0; index < items.length; index++) {
                newInstances[index] = (Instance)items[index];
            }
            setInstances(newInstances);
        }
    }

    /**
     * A single instance object inside instance result.
     */
    @XmlRootElement(name = "instance")
    public static class Instance {
        @XmlElement
        public String cluster;

        @XmlElement
        public String instance;

        @XmlElement
        public String status;

        @XmlElement
        public String uri;

        @XmlElement
        public long creationTime;

        @XmlElement
        public long size;

        public Instance() {
        }

        public Instance(String cluster, String instance, String status) {
            this.cluster = cluster;
            this.instance = instance;
            this.status = status;
        }

        public String getInstance() {
            return instance;
        }

        public String getStatus() {
            return status;
        }

        public String getUri() {
            return uri;
        }

        public String getCluster() {
            return cluster;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public Long getSize() {
            return size;
        }

        @Override
        public String toString() {
            return "{instance:"
                    + this.instance
                    + ", status:"
                    + this.status
                    + (this.uri == null ? "" : ", uri: " + this.uri)
                    + (this.cluster == null ? "" : ", cluster:" + this.cluster) + "}";
        }
    }
}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck

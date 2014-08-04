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

package org.apache.falcon.regression.core.response;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

/**
 * Pojo for JAXB marshalling / unmarshalling.
 */

//SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
@XmlRootElement
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class InstancesSummaryResult extends APIResult {

    @XmlElement
    private InstanceSummary[] instancesSummary;

    private InstancesSummaryResult() { // for jaxb
        super();
    }

    public InstancesSummaryResult(String message, InstanceSummary[] instancesSummary) {
        this(Status.SUCCEEDED, message, instancesSummary);
    }

    public InstancesSummaryResult(Status status, String message,
                                  InstanceSummary[] instancesSummary) {
        super(status, message);
        this.instancesSummary = instancesSummary;
    }

    public InstancesSummaryResult(Status status, String message) {
        super(status, message);
    }

    public InstanceSummary[] getInstancesSummary() {
        return instancesSummary;
    }

    public void setInstancesSummary(InstanceSummary[] instancesSummary) {
        this.instancesSummary = instancesSummary;
    }

    /**
     * A single instance object inside instance result.
     */
    @XmlRootElement(name = "instance-summary")
    public static class InstanceSummary {

        @XmlElement
        public String cluster;
        @XmlElementWrapper(name = "map")
        public Map<String, Long> summaryMap;

        public InstanceSummary() {
        }

        public InstanceSummary(String cluster, Map<String, Long> summaryMap) {
            this.cluster = cluster;
            this.summaryMap = summaryMap;
        }

        public Map<String, Long> getSummaryMap() {
            return summaryMap;
        }

        public String getCluster() {
            return cluster;
        }

        @Override
        public String toString() {
            return "cluster: " + (this.cluster == null ? "" : this.cluster)
                + "summaryMap: " + summaryMap.toString();
        }
    }

}
//RESUME CHECKSTYLE CHECK VisibilityModifierCheck

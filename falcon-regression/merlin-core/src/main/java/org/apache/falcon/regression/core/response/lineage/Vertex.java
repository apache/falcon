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

package org.apache.falcon.regression.core.response.lineage;

import com.google.gson.annotations.SerializedName;

/** Class for representing a vertex. */
public class Vertex extends GraphEntity {

    /** Enum for all the allowed filter keys. */
    public static enum FilterKey {
        name, type, timestamp, version,
        userWorkflowEngine, userWorkflowName, userWorkflowVersion,
        workflowId, runId, status, workflowEngineUrl, subflowId,
    }

    /** Enum for all the allowed vertex types. */
    public static enum VERTEX_TYPE {
        @SerializedName("cluster-entity")CLUSTER_ENTITY("cluster-entity"),
        @SerializedName("feed-entity")FEED_ENTITY("feed-entity"),
        @SerializedName("process-entity")PROCESS_ENTITY("process-entity"),

        @SerializedName("feed-instance")FEED_INSTANCE("feed-instance"),
        @SerializedName("process-instance")PROCESS_INSTANCE("process-instance"),

        @SerializedName("user")USER("user"),
        @SerializedName("data-center")COLO("data-center"),
        @SerializedName("classification")TAGS("classification"),
        @SerializedName("group")GROUPS("group");

        private final String value;
        VERTEX_TYPE(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private int _id;
    private String name;
    private VERTEX_TYPE type;
    private String timestamp;
    private String version;

    private String userWorkflowEngine;
    private String userWorkflowName;
    private String userWorkflowVersion;

    private String workflowId;
    private String runId;
    private String status;
    private String workflowEngineUrl;
    private String subflowId;

    public int get_id() {
        return _id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public VERTEX_TYPE getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getNominalTime() {
        return name.split("/")[1];
    }

    @Override
    public String toString() {
        return "Vertex{"
                + "_id=" + _id
                + ", _type=" + _type
                + ", name='" + name + '\''
                + ", type=" + type
                + ", timestamp='" + timestamp + '\''
                + ", version='" + version + '\''
                + ", userWorkflowEngine='" + userWorkflowEngine + '\''
                + ", userWorkflowName='" + userWorkflowName + '\''
                + ", userWorkflowVersion='" + userWorkflowVersion + '\''
                + ", workflowId='" + workflowId + '\''
                + ", runId='" + runId + '\''
                + ", status='" + status + '\''
                + ", workflowEngineUrl='" + workflowEngineUrl + '\''
                + ", subflowId='" + subflowId + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Vertex)) {
            return false;
        }

        Vertex vertex = (Vertex) o;

        if (_id != vertex._id || !name.equals(vertex.name)
                || (runId != null ? !runId.equals(vertex.runId) : vertex.runId != null)
                || (status != null ? !status.equals(vertex.status) : vertex.status != null)
                || (subflowId != null ? !subflowId.equals(vertex.subflowId)
                        : vertex.subflowId != null)
                || !timestamp.equals(vertex.timestamp)
                || type != vertex.type
                || (userWorkflowEngine != null
                        ? !userWorkflowEngine.equals(vertex.userWorkflowEngine)
                        : vertex.userWorkflowEngine != null)
                || (userWorkflowName != null ? !userWorkflowName.equals(vertex.userWorkflowName)
                        : vertex.userWorkflowName != null)
                || (userWorkflowVersion != null
                        ? !userWorkflowVersion.equals(vertex.userWorkflowVersion)
                        : vertex.userWorkflowVersion != null)
                || (version != null ? !version.equals(vertex.version) : vertex.version != null)
                || (workflowEngineUrl != null
                        ? !workflowEngineUrl.equals(vertex.workflowEngineUrl)
                        : vertex.workflowEngineUrl != null)
                || (workflowId != null ? !workflowId.equals(vertex.workflowId)
                        : vertex.workflowId != null)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = _id;
        result = 31 * result + name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + timestamp.hashCode();
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (userWorkflowEngine != null ? userWorkflowEngine.hashCode() : 0);
        result = 31 * result + (userWorkflowName != null ? userWorkflowName.hashCode() : 0);
        result = 31 * result + (userWorkflowVersion != null ? userWorkflowVersion.hashCode() : 0);
        result = 31 * result + (workflowId != null ? workflowId.hashCode() : 0);
        result = 31 * result + (runId != null ? runId.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (workflowEngineUrl != null ? workflowEngineUrl.hashCode() : 0);
        result = 31 * result + (subflowId != null ? subflowId.hashCode() : 0);
        return result;
    }

}

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

package org.apache.falcon.metadata;

/**
 * Enumerates Relationship property keys.
 */
public enum RelationshipProperty {

    // vertex/edge property keys - indexed
    NAME("name"),
    TYPE("type"),
    TIMESTAMP("timestamp"),
    VERSION("version"),

    // workflow properties
    USER_WORKFLOW_ENGINE("userWorkflowEngine", "user workflow engine type"),
    USER_WORKFLOW_NAME("userWorkflowName", "user workflow name"),
    USER_WORKFLOW_VERSION("userWorkflowVersion", "user workflow version"),

    // workflow instance properties
    WORKFLOW_ID("workflowId", "current workflow-id of the instance"),
    RUN_ID("runId", "current run-id of the instance"),
    STATUS("status", "status of the user workflow instance"),
    WF_ENGINE_URL("workflowEngineUrl", "url of workflow engine server, ex: oozie"),
    USER_SUBFLOW_ID("subflowId", "external id of user workflow"),

    // instance-entity edge property
    NOMINAL_TIME("nominalTime");

    private final String name;
    private final String description;

    RelationshipProperty(String name) {
        this(name, name);
    }

    RelationshipProperty(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}

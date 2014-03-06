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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Args for data Lineage.
 */
public enum LineageArgs {
    // process instance
    NOMINAL_TIME("nominalTime", "instance time"),
    ENTITY_TYPE("entityType", "type of the entity"),
    ENTITY_NAME("entityName", "name of the entity"),
    TIMESTAMP("timeStamp", "current timestamp"),

    // where
    CLUSTER("cluster", "name of the current cluster"),
    OPERATION("operation", "operation like generate, delete, replicate"),

    // who
    WORKFLOW_USER("workflowUser", "user who owns the feed instance (partition)"),

    // workflow details
    WORKFLOW_ID("workflowId", "current workflow-id of the instance"),
    RUN_ID("runId", "current run-id of the instance"),
    STATUS("status", "status of the user workflow isnstance"),
    WF_ENGINE_URL("workflowEngineUrl", "url of workflow engine server, ex:oozie"),
    USER_SUBFLOW_ID("subflowId", "external id of user workflow"),
    USER_WORKFLOW_ENGINE("userWorkflowEngine", "user workflow engine type"),
    USER_WORKFLOW_NAME("userWorkflowName", "user workflow name"),
    USER_WORKFLOW_VERSION("userWorkflowVersion", "user workflow version"),

    // what inputs
    INPUT_FEED_NAMES("falconInputFeeds", "name of the feeds which are used as inputs"),
    INPUT_FEED_PATHS("falconInputPaths", "comma separated input feed instance paths"),

    // what outputs
    FEED_NAMES("feedNames", "name of the feeds which are generated/replicated/deleted"),
    FEED_INSTANCE_PATHS("feedInstancePaths", "comma separated feed instance paths"),

    // lineage data recorded
    LOG_DIR("logDir", "log dir where lineage can be recorded");

    private String name;
    private String description;

    LineageArgs(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public Option getOption() {
        return new Option(this.name, true, this.description);
    }

    public String getOptionName() {
        return this.name;
    }

    public String getOptionValue(CommandLine cmd) {
        return cmd.getOptionValue(this.name);
    }
}

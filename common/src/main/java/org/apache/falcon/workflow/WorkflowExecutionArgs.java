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

package org.apache.falcon.workflow;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Arguments for workflow execution.
 */
public enum WorkflowExecutionArgs {

    // instance details
    NOMINAL_TIME("nominalTime", "instance time"),
    ENTITY_TYPE("entityType", "type of the entity"),
    ENTITY_NAME("entityName", "name of the entity"),
    TIMESTAMP("timeStamp", "current timestamp"),

    // where
    CLUSTER_NAME("cluster", "name of the current cluster"),
    OPERATION("operation", "operation like generate, delete, replicate"),
    // Exactly same as the above. Introduced to ensure compatibility between messages produced by POST-PROCESSING and
    // the values in conf.
    DATA_OPERATION("falconDataOperation", "operation like generate, delete, replicate", false),
    DATASOURCE_NAME("datasource", "name of the datasource", false),

    // who
    WORKFLOW_USER("workflowUser", "user who ran the instance"),

    // what
    // workflow details
    USER_WORKFLOW_ENGINE("userWorkflowEngine", "user workflow engine type", false),
    USER_WORKFLOW_NAME("userWorkflowName", "user workflow name", false),
    USER_WORKFLOW_VERSION("userWorkflowVersion", "user workflow version", false),

    // workflow execution details
    WORKFLOW_ID("workflowId", "current workflow-id of the instance"),
    RUN_ID("runId", "current run-id of the instance"),
    STATUS("status", "status of the user workflow isnstance"),
    WF_ENGINE_URL("workflowEngineUrl", "url of workflow engine server, ex:oozie", false),
    USER_SUBFLOW_ID("subflowId", "external id of user workflow", false),
    PARENT_ID("parentId", "The parent of the current workflow, typically coord action", false),

    WF_START_TIME("workflowStartTime", "workflow start time", false),
    WF_END_TIME("workflowEndTime", "workflow end time", false),
    WF_DURATION("workflowDuration", "workflow duration", false),

    // what inputs
    INPUT_FEED_NAMES("falconInputFeeds", "name of the feeds which are used as inputs", false),
    INPUT_FEED_PATHS("falconInPaths", "comma separated input feed instance paths", false),
    INPUT_NAMES("falconInputNames", "name of the inputs", false),
    INPUT_STORAGE_TYPES("falconInputFeedStorageTypes", "input storage types", false),

    // what outputs
    OUTPUT_FEED_NAMES("feedNames", "name of the feeds which are generated/replicated/deleted"),
    OUTPUT_FEED_PATHS("feedInstancePaths", "comma separated feed instance paths"),

    // broker related parameters
    TOPIC_NAME("topicName", "name of the topic to be used to send JMS message", false),
    BRKR_IMPL_CLASS("brokerImplClass", "falcon message broker Implementation class"),
    BRKR_URL("brokerUrl", "falcon message broker url"),
    USER_BRKR_IMPL_CLASS("userBrokerImplClass", "user broker Impl class", false),
    USER_BRKR_URL("userBrokerUrl", "user broker url", false),
    BRKR_TTL("brokerTTL", "time to live for broker message in sec", false),
    USER_JMS_NOTIFICATION_ENABLED("userJMSNotificationEnabled", "Is User notification via JMS enabled?", false),
    SYSTEM_JMS_NOTIFICATION_ENABLED("systemJMSNotificationEnabled", "Is system notification via JMS enabled?", false),

    // state maintained
    LOG_FILE("logFile", "log file path where feeds to be deleted are recorded", false),
    // execution context data recorded
    LOG_DIR("logDir", "log dir where lineage can be recorded"),

    CONTEXT_FILE("contextFile", "wf execution context file path where wf properties are recorded", false),
    CONTEXT_TYPE("contextType", "wf execution context type, pre or post processing", false),
    COUNTERS("counters", "store job counters", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    WorkflowExecutionArgs(String name, String description) {
        this(name, description, true);
    }

    WorkflowExecutionArgs(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public Option getOption() {
        return new Option(this.name, true, this.description);
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    public String getOptionValue(CommandLine cmd) {
        return cmd.getOptionValue(this.name);
    }

    @Override
    public String toString() {
        return getName();
    }
}

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

package org.apache.falcon.recipe;

import java.util.Map;
import java.util.HashMap;

/**
 * Recipe tool options.
 */
public enum RecipeToolOptions {
    RECIPE_NAME("falcon.recipe.name", "Recipe name", false),
    CLUSTER_NAME("falcon.recipe.cluster.name", "Cluster name where replication job should run", false),
    CLUSTER_HDFS_WRITE_ENDPOINT(
            "falcon.recipe.cluster.hdfs.writeEndPoint", "Cluster HDFS write endpoint"),
    CLUSTER_VALIDITY_START("falcon.recipe.cluster.validity.start", "Source cluster validity start", false),
    CLUSTER_VALIDITY_END("falcon.recipe.cluster.validity.end", "Source cluster validity end", false),
    WORKFLOW_NAME("falcon.recipe.workflow.name", "Workflow name", false),
    WORKFLOW_PATH("falcon.recipe.workflow.path", "Workflow path", false),
    WORKFLOW_LIB_PATH("falcon.recipe.workflow.lib.path", "WF lib path", false),
    PROCESS_FREQUENCY("falcon.recipe.process.frequency", "Process frequency", false),
    RETRY_POLICY("falcon.recipe.retry.policy", "Retry policy", false),
    RETRY_DELAY("falcon.recipe.retry.delay", "Retry delay", false),
    RETRY_ATTEMPTS("falcon.recipe.retry.attempts", "Retry attempts", false),
    RETRY_ON_TIMEOUT("falcon.recipe.retry.onTimeout", "Retry onTimeout", false),
    RECIPE_TAGS("falcon.recipe.tags", "Recipe tags", false),
    RECIPE_ACL_OWNER("falcon.recipe.acl.owner", "Recipe acl owner", false),
    RECIPE_ACL_GROUP("falcon.recipe.acl.group", "Recipe acl group", false),
    RECIPE_ACL_PERMISSION("falcon.recipe.acl.permission", "Recipe acl permission", false),
    RECIPE_NN_PRINCIPAL("falcon.recipe.nn.principal", "Recipe DFS NN principal", false),
    RECIPE_NOTIFICATION_TYPE("falcon.recipe.notification.type", "Recipe Notification Type", false),
    RECIPE_NOTIFICATION_ADDRESS("falcon.recipe.notification.receivers", "Recipe Email Notification receivers", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    private static Map<String, RecipeToolOptions> optionsMap = new HashMap<>();
    static {
        for (RecipeToolOptions c : RecipeToolOptions.values()) {
            optionsMap.put(c.getName(), c);
        }
    }

    public static Map<String, RecipeToolOptions> getOptionsMap() {
        return optionsMap;
    }

    RecipeToolOptions(String name, String description) {
        this(name, description, true);
    }

    RecipeToolOptions(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
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

    @Override
    public String toString() {
        return getName();
    }
}

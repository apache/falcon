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

package org.apache.falcon.extensions;

import java.util.Map;
import java.util.HashMap;

/**
 * Extension options.
 */
public enum ExtensionProperties {
    JOB_NAME("jobName", "job name"),
    RESOURCE_NAME("resourceName", "resource name", false),
    //Can't use default as target as extension is generic and need not be used only for replication
    CLUSTER_NAME("jobClusterName", "Cluster name where job should run"),
    VALIDITY_START("jobValidityStart", "Job validity start"),
    VALIDITY_END("jobValidityEnd", "Job validity end"),
    FREQUENCY("jobFrequency", "Job frequency"),
    TIMEZONE("jobTimezone", "Time zone", false),
    // Use defaults for retry
    RETRY_POLICY("jobRetryPolicy", "Retry policy", false),
    RETRY_DELAY("jobRetryDelay", "Retry delay", false),
    RETRY_ATTEMPTS("jobRetryAttempts", "Retry attempts", false),
    RETRY_ON_TIMEOUT("jobRetryOnTimeout", "Retry onTimeout", false),
    JOB_TAGS("jobTags", "Job tags", false),
    JOB_ACL_OWNER("jobAclOwner", "Job acl owner", false),
    JOB_ACL_GROUP("jobAclGroup", "Job acl group", false),
    JOB_ACL_PERMISSION("jobAclPermission", "Job acl permission", false),
    JOB_NOTIFICATION_TYPE("jobNotificationType", "Notification Type", false),
    JOB_NOTIFICATION_ADDRESS("jobNotificationReceivers", "Email Notification receivers", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    private static Map<String, ExtensionProperties> optionsMap = new HashMap<>();
    static {
        for (ExtensionProperties c : ExtensionProperties.values()) {
            optionsMap.put(c.getName(), c);
        }
    }

    public static Map<String, ExtensionProperties> getOptionsMap() {
        return optionsMap;
    }

    ExtensionProperties(String name, String description) {
        this(name, description, true);
    }

    ExtensionProperties(String name, String description, boolean isRequired) {
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

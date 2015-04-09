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

package org.apache.falcon;

import java.util.Date;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.EntitySummaryResult;

/**
 * Helpers for response object to string conversion.
 */

public final class ResponseHelper {

    private ResponseHelper() { }

    public static String getString(EntitySummaryResult result) {
        StringBuilder sb = new StringBuilder();
        String toAppend;
        sb.append("Consolidated Status: ").append(result.getStatus())
            .append("\n");
        sb.append("\nEntity Summary Result :\n");
        if (result.getEntitySummaries() != null) {
            for (EntitySummaryResult.EntitySummary entitySummary : result.getEntitySummaries()) {
                toAppend = entitySummary.toString();
                sb.append(toAppend).append("\n");
            }
        }
        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    public static String getString(InstancesResult result, String runid) {
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus())
            .append("\n");

        sb.append("\nInstances:\n");
        sb.append("Instance\t\tCluster\t\tSourceCluster\t\tStatus\t\tRunID\t\t\tLog\n");
        sb.append("-----------------------------------------------------------------------------------------------\n");
        if (result.getInstances() != null) {
            for (InstancesResult.Instance instance : result.getInstances()) {

                toAppend =
                    (instance.getInstance() != null) ? instance.getInstance()
                        : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getCluster() != null ? instance.getCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getSourceCluster() != null ? instance
                        .getSourceCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    (instance.getStatus() != null ? instance.getStatus()
                        .toString() : "-");
                sb.append(toAppend).append("\t");

                toAppend = (runid != null ? runid : "latest");
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getLogFile() != null ? instance.getLogFile() : "-";
                sb.append(toAppend).append("\n");

                if (instance.actions != null) {
                    sb.append("actions:\n");
                    for (InstancesResult.InstanceAction action : instance.actions) {
                        sb.append("    ").append(action.getAction())
                            .append("\t");
                        sb.append(action.getStatus()).append("\t")
                            .append(action.getLogFile()).append("\n");
                    }
                }
            }
        }
        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    public static String getString(FeedInstanceResult result) {
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus())
            .append("\n");

        sb.append("\nInstances:\n");
        sb.append("Cluster\t\tInstance\t\tStatus\t\tSize\t\tCreationTime\t\tDetails\n");
        sb.append("-----------------------------------------------------------------------------------------------\n");
        if (result.getInstances() != null) {
            for (FeedInstanceResult.Instance instance : result.getInstances()) {

                toAppend =
                    instance.getCluster() != null ? instance.getCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getInstance() != null ? instance.getInstance()
                        : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getStatus() != null ? instance.getStatus() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getSize() != -1 ? String.valueOf(instance
                        .getSize()) : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getCreationTime() != 0
                        ? SchemaHelper.formatDateUTC(new Date(instance
                            .getCreationTime())) : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    StringUtils.isEmpty(instance.getUri()) ? "-" : instance
                        .getUri();
                sb.append(toAppend).append("\n");
            }
        }
        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    public static String getString(InstancesResult result) {
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus())
            .append("\n");

        sb.append("\nInstances:\n");
        sb.append("Instance\t\tCluster\t\tSourceCluster\t\tStatus\t\tStart\t\tEnd\t\tDetails\t\t\t\t\tLog\n");
        sb.append("-----------------------------------------------------------------------------------------------\n");
        if (result.getInstances() != null) {
            for (InstancesResult.Instance instance : result.getInstances()) {

                toAppend =
                    instance.getInstance() != null ? instance.getInstance()
                        : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getCluster() != null ? instance.getCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getSourceCluster() != null ? instance
                        .getSourceCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    (instance.getStatus() != null ? instance.getStatus()
                        .toString() : "-");
                sb.append(toAppend).append("\t");

                toAppend = instance.getStartTime() != null
                    ? SchemaHelper.formatDateUTC(instance.getStartTime()) : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getEndTime() != null
                    ? SchemaHelper.formatDateUTC(instance.getEndTime()) : "-";
                sb.append(toAppend).append("\t");

                toAppend = (!StringUtils.isEmpty(instance.getDetails()))
                    ? instance.getDetails() : "-";
                sb.append(toAppend).append("\t");

                toAppend =
                    instance.getLogFile() != null ? instance.getLogFile() : "-";
                sb.append(toAppend).append("\n");

                if (instance.getWfParams() != null) {
                    InstancesResult.KeyValuePair[] props = instance.getWfParams();
                    sb.append("Workflow params").append("\n");
                    for (InstancesResult.KeyValuePair entry : props) {
                        sb.append(entry.getKey()).append("=")
                            .append(entry.getValue()).append("\n");
                    }
                    sb.append("\n");
                }

                if (instance.actions != null) {
                    sb.append("actions:\n");
                    for (InstancesResult.InstanceAction action : instance.actions) {
                        sb.append(" ").append(action.getAction()).append("\t");
                        sb.append(action.getStatus()).append("\t")
                            .append(action.getLogFile()).append("\n");
                    }
                }
            }
        }
        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    public static String getString(InstancesSummaryResult result) {
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus())
            .append("\n");
        sb.append("\nInstances Summary:\n");

        if (result.getInstancesSummary() != null) {
            for (InstancesSummaryResult.InstanceSummary summary : result
                .getInstancesSummary()) {
                toAppend =
                    summary.getCluster() != null ? summary.getCluster() : "-";
                sb.append("Cluster: ").append(toAppend).append("\n");

                sb.append("Status\t\tCount\n");
                sb.append("-------------------------\n");

                for (Map.Entry<String, Long> entry : summary.getSummaryMap()
                    .entrySet()) {
                    sb.append(entry.getKey()).append("\t\t")
                        .append(entry.getValue()).append("\n");
                }
            }
        }

        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    public static String getString(FeedLookupResult feedLookupResult) {
        StringBuilder sb = new StringBuilder();
        String results = feedLookupResult.toString();
        if (StringUtils.isEmpty(results)) {
            sb.append("No matching feeds found!");
        } else {
            sb.append(results);
        }
        sb.append("\n\nResponse: ").append(feedLookupResult.getMessage());
        sb.append("\nRequest Id: ").append(feedLookupResult.getRequestId());
        return sb.toString();
    }
}

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
package org.apache.falcon.aspect;

import org.apache.falcon.monitors.Alert;
import org.apache.falcon.monitors.Auditable;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.monitors.TimeTaken;

/**
 * Create a method with params you want to monitor/alert/audit via Aspect
 * and log in metric, invoke this method from code.
 */
@SuppressWarnings("UnusedParameters")
public final class GenericAlert {

    private GenericAlert() {}

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @Monitored(event = "retry-instance-failed")
    public static String alertRetryFailed(
            @Dimension(value = "entity-type") String entityType,
            @Dimension(value = "entity-name") String entityName,
            @Dimension(value = "nominal-name") String nominalTime,
            @Dimension(value = "wf-id") String wfId,
            @Dimension(value = "wf-user") String workflowUser,
            @Dimension(value = "run-id") String runId,
            @Dimension(value = "error-message") String message) {
        return "IGNORE";
    }

    @Monitored(event = "late-rerun-failed")
    public static String alertLateRerunFailed(
            @Dimension(value = "entity-type") String entityType,
            @Dimension(value = "entity-name") String entityName,
            @Dimension(value = "nominal-name") String nominalTime,
            @Dimension(value = "wf-id") String wfId,
            @Dimension(value = "wf-user") String workflowUser,
            @Dimension(value = "run-id") String runId,
            @Dimension(value = "error-message") String message) {
        return "IGNORE";
    }

    @Monitored(event = "wf-instance-failed")
    public static String instrumentFailedInstance(
            @Dimension(value = "cluster") String cluster,
            @Dimension(value = "entity-type") String entityType,
            @Dimension(value = "entity-name") String entityName,
            @Dimension(value = "nominal-time") String nominalTime,
            @Dimension(value = "wf-id") String workflowId,
            @Dimension(value = "wf-user") String workflowUser,
            @Dimension(value = "run-id") String runId,
            @Dimension(value = "operation") String operation,
            @Dimension(value = "start-time") String startTime,
            @Dimension(value = "error-message") String errorMessage,
            @Dimension(value = "message") String message,
            @TimeTaken long timeTaken) {
        return "IGNORE";
    }

    @Monitored(event = "wf-instance-succeeded")
    public static String instrumentSucceededInstance(
            @Dimension(value = "cluster") String cluster,
            @Dimension(value = "entity-type") String entityType,
            @Dimension(value = "entity-name") String entityName,
            @Dimension(value = "nominal-time") String nominalTime,
            @Dimension(value = "wf-id") String workflowId,
            @Dimension(value = "wf-user") String workflowUser,
            @Dimension(value = "run-id") String runId,
            @Dimension(value = "operation") String operation,
            @Dimension(value = "start-time") String startTime,
            @TimeTaken long timeTaken) {
        return "IGNORE";
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    @Monitored(event = "init-kerberos-failed")
    public static String initializeKerberosFailed(
            @Dimension(value = "message") String message,
            @Dimension(value = "exception") Throwable throwable) {
        return "IGNORE";
    }

    @Monitored(event = "rerun-queue-failed")
    public static String alertRerunConsumerFailed(
            @Dimension(value = "message") String message,
            @Dimension(value = "exception") Exception exception) {
        return "IGNORE";
    }

    @Alert(event = "log-cleanup-service-failed")
    public static String alertLogCleanupServiceFailed(
            @Dimension(value = "message") String message,
            @Dimension(value = "exception") Throwable throwable) {
        return "IGNORE";
    }

    @Alert(event = "jms-message-consumer-failed")
    public static String alertJMSMessageConsumerFailed(
            @Dimension(value = "message") String message,
            @Dimension(value = "exception") Throwable throwable) {
        return "IGNORE";
    }

    @Auditable(operation = "record-audit")
    public static String audit(
            @Dimension(value = "request-user")    String user,          // who
            @Dimension(value = "remote-address")  String remoteAddress, // who
            @Dimension(value = "remote-host")     String remoteHost,    // who
            @Dimension(value = "request-url")     String requestUrl,    // what
            @Dimension(value = "server-address")  String serverAddress, // what server
            @Dimension(value = "request-time")    String time) {        // when
        return "IGNORE";
    }
}

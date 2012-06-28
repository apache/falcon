/*
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
package org.apache.ivory.aspect;

import org.apache.ivory.IvoryException;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.monitors.TimeTaken;

/**
 * Create a method with params you want to monitor via Aspect and log in metric
 * and iMon, invoke this method from code.
 */
public class GenericAlert {

	@Monitored(event = "retry-instance-failed")
	public static String alertRetryFailed(
			@Dimension(value = "entity-type") String entityType,
			@Dimension(value = "entity-name") String entityName,
			@Dimension(value = "nominal-name") String nominalTime,
			@Dimension(value = "wf-id") String wfId,
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
			@Dimension(value = "run-id") String runId,
			@Dimension(value = "operation") String operation,
			@TimeTaken long timeTaken)
			throws IvoryException {
		return "IGNORE";
	}
	
	@Monitored(event = "wf-instance-succeeded")
	public static String instrumentSucceededInstance(
			@Dimension(value = "cluster") String cluster,
			@Dimension(value = "entity-type") String entityType,
			@Dimension(value = "entity-name") String entityName,
			@Dimension(value = "nominal-time") String nominalTime,
			@Dimension(value = "wf-id") String workflowId,
			@Dimension(value = "run-id") String runId,
			@Dimension(value = "operation") String operation,
			@TimeTaken long timeTaken)
			throws IvoryException {
		return "IGNORE";
	}
}

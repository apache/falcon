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
package org.apache.ivory.util;

import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;

/**
 * Create a method with params you want to monitor via Aspect and log in metric
 * and iMon, invoke this method from code.
 */
public class GenericAlert {
	@Monitored(event = "transaction-rollback-failed")
	public static String alertRollbackFailure(
			@Dimension(value = "transaction-Id") String transactionId) {
		return transactionId;
	}

	@Monitored(event = "process-instance-failed")
	public static String alertWFfailed(
			@Dimension(value = "process-name") String processName,
			@Dimension(value = "nominal-time") String nominalTime) {
		return "IGNORE";
	}

	@Monitored(event = "retry-instance-failed")
	public static String alertRetryFailed(
			@Dimension(value = "process-name") String processName,
			@Dimension(value = "nominal-name") String processInstance,
			@Dimension(value = "current-run-id") int runId,
			@Dimension(value = "error-message") String message) {
		return "IGNORE";

	}

}

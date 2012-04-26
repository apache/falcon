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
package org.apache.ivory.logging;

import org.apache.ivory.IvoryException;
import org.apache.ivory.resource.ProcessInstancesResult;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.testng.annotations.Test;

public class LogProviderTest {

	@Test(enabled=false)
	public void testLogProvider() throws IvoryException {

		ProcessInstancesResult.ProcessInstance processInstance = new ProcessInstancesResult.ProcessInstance();
		processInstance.instance = "2012-04-26T07:01Z";
		processInstance.status = WorkflowStatus.RUNNING;
		System.out
				.println(LogProvider
						.getLogUrl(
								"agregator-coord16-ce988633-6bea-44a2-8f4d-af997a0c6bf2-712c1bb2-f60a-4dc7-8d25-2850d60e4e4b",
								processInstance, "LATE1"));
	}
}

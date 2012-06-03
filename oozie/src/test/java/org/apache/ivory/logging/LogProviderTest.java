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
import org.apache.ivory.Tag;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.resource.InstancesResult.WorkflowStatus;
import org.testng.annotations.Test;

public class LogProviderTest {

	@Test(enabled=false)
	public void testLogProvider() throws IvoryException {

		InstancesResult.Instance instance = new InstancesResult.Instance();
		instance.instance = "2012-04-26T07:01Z";
		instance.status = WorkflowStatus.RUNNING;
		Process process = new Process();
		process.setName("agg-coord");
		System.out
				.println(LogProvider.getLogUrl(process, instance, Tag.DEFAULT, "0"));
	}
}

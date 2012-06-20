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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.entity.parser.ProcessEntityParser;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.resource.InstancesResult.Instance;
import org.apache.ivory.resource.InstancesResult.InstanceAction;
import org.apache.ivory.resource.InstancesResult.WorkflowStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LogProviderTest {

	private static final ConfigurationStore store = ConfigurationStore.get();
	private static EmbeddedCluster testCluster = null;
	private static Process testProcess = null;
	private static String processName = "testProcess";
	private static FileSystem fs;
	private Instance instance;

	@BeforeClass
	public void setup() throws Exception {
		testCluster = EmbeddedCluster.newCluster("testCluster", false);
		store.publish(EntityType.CLUSTER, testCluster.getCluster());
		fs = FileSystem.get(testCluster.getConf());
		Path instanceLogPath = new Path(
				"/workflow/staging/ivory/workflows/process/" + processName
						+ "/logs/job-2010-01-01-01-00/000");
		fs.mkdirs(instanceLogPath);
		fs.createNewFile(new Path(instanceLogPath, "oozie.log"));
		fs.createNewFile(new Path(instanceLogPath, "pigAction_SUCCEEDED.log"));
		fs.createNewFile(new Path(instanceLogPath, "mr_Action_FAILED.log"));
		fs.createNewFile(new Path(instanceLogPath, "mr_Action2_SUCCEEDED.log"));

		fs.mkdirs(new Path("/workflow/staging/ivory/workflows/process/"
				+ processName + "/logs/job-2010-01-01-01-00/001"));
		fs.mkdirs(new Path("/workflow/staging/ivory/workflows/process/"
				+ processName + "/logs/job-2010-01-01-01-00/002"));
		Path run3 = new Path("/workflow/staging/ivory/workflows/process/"
				+ processName + "/logs/job-2010-01-01-01-00/003");
		fs.mkdirs(run3);
		fs.createNewFile(new Path(run3, "oozie.log"));

		testProcess = new ProcessEntityParser().parse(LogMoverTest.class
				.getResourceAsStream("/org/apache/ivory/logging/process.xml"));
		testProcess.setName(processName);
		store.publish(EntityType.PROCESS, testProcess);
	}

	@BeforeMethod
	public void setInstance() {
		instance = new Instance();
		instance.status = WorkflowStatus.SUCCEEDED;
		instance.instance = "2010-01-01T01:00Z";
		instance.cluster = "testCluster";
		instance.logFile = "http://localhost:15000/oozie/wflog";
	}

	@Test
	public void testLogProviderWithValidRunId() throws IvoryException {
		LogProvider provider = new LogProvider();
		Instance instanceWithLog = provider.populateLogUrls(testProcess,
				instance, "0");
		Assert.assertEquals(
				instance.logFile,
				"http://localhost:50070/data/workflow/staging/ivory/workflows/process/testProcess/logs/job-2010-01-01-01-00/000/oozie.log");

		InstanceAction action = instanceWithLog.actions[0];
		Assert.assertEquals(action.action, "mr_Action2");
		Assert.assertEquals(action.status, "SUCCEEDED");
		Assert.assertEquals(
				action.logFile,
				"http://localhost:50070/data/workflow/staging/ivory/workflows/process/testProcess/logs/job-2010-01-01-01-00/000/mr_Action2_SUCCEEDED.log");

		action = instanceWithLog.actions[1];
		Assert.assertEquals(action.action, "mr_Action");
		Assert.assertEquals(action.status, "FAILED");
		Assert.assertEquals(
				action.logFile,
				"http://localhost:50070/data/workflow/staging/ivory/workflows/process/testProcess/logs/job-2010-01-01-01-00/000/mr_Action_FAILED.log");

	}

	@Test
	public void testLogProviderWithInvalidRunId() throws IvoryException {
		LogProvider provider = new LogProvider();
		provider.populateLogUrls(testProcess, instance, "x");
		Assert.assertEquals(instance.logFile,
				"http://localhost:15000/oozie/wflog");
	}

	@Test
	public void testLogProviderWithUnavailableRunId() throws IvoryException {
		LogProvider provider = new LogProvider();
		instance.logFile = null;
		provider.populateLogUrls(testProcess, instance, "7");
		Assert.assertEquals(instance.logFile, "-");
	}

	@Test
	public void testLogProviderWithEmptyRunId() throws IvoryException {
		LogProvider provider = new LogProvider();
		instance.logFile = null;
		provider.populateLogUrls(testProcess, instance, null);
		Assert.assertEquals(
				instance.logFile,
				"http://localhost:50070/data/workflow/staging/ivory/workflows/process/testProcess/logs/job-2010-01-01-01-00/003/oozie.log");
	}
}

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
package org.apache.falcon.logging;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.InstanceAction;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;

/**
 * Test for LogProvider.
 */
public class LogProviderTest {

    private static final ConfigurationStore STORE = ConfigurationStore.get();
    private static EmbeddedCluster testCluster = null;
    private static Process testProcess = null;
    private static final String PROCESS_NAME = "testProcess";
    private static FileSystem fs;
    private Instance instance;

    @BeforeClass
    public void setup() throws Exception {
        testCluster = EmbeddedCluster.newCluster("testCluster", false);
        cleanupStore();
        STORE.publish(EntityType.CLUSTER, testCluster.getCluster());
        fs = FileSystem.get(testCluster.getConf());
        Path instanceLogPath = new Path(
                "/workflow/staging/falcon/workflows/process/" + PROCESS_NAME
                        + "/logs/job-2010-01-01-01-00/000");
        fs.mkdirs(instanceLogPath);
        fs.createNewFile(new Path(instanceLogPath, "oozie.log"));
        fs.createNewFile(new Path(instanceLogPath, "pigAction_SUCCEEDED.log"));
        fs.createNewFile(new Path(instanceLogPath, "mr_Action_FAILED.log"));
        fs.createNewFile(new Path(instanceLogPath, "mr_Action2_SUCCEEDED.log"));

        fs.mkdirs(new Path("/workflow/staging/falcon/workflows/process/"
                + PROCESS_NAME + "/logs/job-2010-01-01-01-00/001"));
        fs.mkdirs(new Path("/workflow/staging/falcon/workflows/process/"
                + PROCESS_NAME + "/logs/job-2010-01-01-01-00/002"));
        Path run3 = new Path("/workflow/staging/falcon/workflows/process/"
                + PROCESS_NAME + "/logs/job-2010-01-01-01-00/003");
        fs.mkdirs(run3);
        fs.createNewFile(new Path(run3, "oozie.log"));

        testProcess = new ProcessEntityParser().parse(LogMoverTest.class
                .getResourceAsStream("/org/apache/falcon/logging/process.xml"));
        testProcess.setName(PROCESS_NAME);
        STORE.publish(EntityType.PROCESS, testProcess);
    }

    @BeforeMethod
    public void setInstance() {
        instance = new Instance();
        instance.status = WorkflowStatus.SUCCEEDED;
        instance.instance = "2010-01-01T01:00Z";
        instance.cluster = "testCluster";
        instance.logFile = "http://localhost:15000/oozie/wflog";
    }

    private void cleanupStore() throws FalconException {
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = STORE.getEntities(type);
            for (String entity : entities) {
                STORE.remove(type, entity);
            }
        }
    }

    @Test
    public void testLogProviderWithValidRunId() throws FalconException {
        LogProvider provider = new LogProvider();
        Instance instanceWithLog = provider.populateLogUrls(testProcess,
                instance, "0");
        Assert.assertEquals(
                instance.logFile,
                "http://localhost:50070/data/workflow/staging/falcon/workflows/process/testProcess/logs/job-2010-01"
                        + "-01-01-00/000/oozie.log");

        InstanceAction action = instanceWithLog.actions[0];
        Assert.assertEquals(action.action, "mr_Action2");
        Assert.assertEquals(action.status, "SUCCEEDED");
        Assert.assertEquals(
                action.logFile,
                "http://localhost:50070/data/workflow/staging/falcon/workflows/process/testProcess/logs/job-2010-01"
                        + "-01-01-00/000/mr_Action2_SUCCEEDED.log");

        action = instanceWithLog.actions[1];
        Assert.assertEquals(action.action, "mr_Action");
        Assert.assertEquals(action.status, "FAILED");
        Assert.assertEquals(
                action.logFile,
                "http://localhost:50070/data/workflow/staging/falcon/workflows/process/testProcess/logs/job-2010-01"
                        + "-01-01-00/000/mr_Action_FAILED.log");
    }

    @Test
    public void testLogProviderWithInvalidRunId() throws FalconException {
        LogProvider provider = new LogProvider();
        provider.populateLogUrls(testProcess, instance, "x");
        Assert.assertEquals(instance.logFile,
                "http://localhost:15000/oozie/wflog");
    }

    @Test
    public void testLogProviderWithUnavailableRunId() throws FalconException {
        LogProvider provider = new LogProvider();
        instance.logFile = null;
        provider.populateLogUrls(testProcess, instance, "7");
        Assert.assertEquals(instance.logFile, "-");
    }

    @Test
    public void testLogProviderWithEmptyRunId() throws FalconException {
        LogProvider provider = new LogProvider();
        instance.logFile = null;
        provider.populateLogUrls(testProcess, instance, null);
        Assert.assertEquals(
                instance.logFile,
                "http://localhost:50070/data/workflow/staging/falcon/workflows/process/testProcess/logs/"
                        + "job-2010-01-01-01-00/003/oozie.log");
    }
}

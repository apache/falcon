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
import org.apache.falcon.cluster.util.StandAloneCluster;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.OozieWorkflowEngine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for LogMover.
 * Requires Oozie to be running on localhost.
 */
@Test
public class LogMoverIT {

    private static final ConfigurationStore STORE = ConfigurationStore.get();
    private static final String PROCESS_NAME = "testProcess" + System.currentTimeMillis();
    private static EmbeddedCluster testCluster = null;
    private static Process testProcess = null;
    private static FileSystem fs;

    @BeforeClass
    public void setup() throws Exception {
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("cluster", "testCluster");
        TestContext context = new TestContext();
        String file = context.
                overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        testCluster = StandAloneCluster.newCluster(file);
        STORE.publish(EntityType.CLUSTER, testCluster.getCluster());
/*
        new File("target/libs").mkdirs();
        StartupProperties.get().setProperty("system.lib.location", "target/libs");
        SharedLibraryHostingService listener = new SharedLibraryHostingService();
        listener.onAdd(testCluster.getCluster());
*/
        fs = FileSystem.get(testCluster.getConf());
        fs.mkdirs(new Path("/workflow/lib"));

        fs.copyFromLocalFile(
                new Path(LogMoverIT.class.getResource(
                        "/org/apache/falcon/logging/workflow.xml").toURI()),
                new Path("/workflow"));
        fs.copyFromLocalFile(
                new Path(LogMoverIT.class.getResource(
                        "/org/apache/falcon/logging/java-test.jar").toURI()),
                new Path("/workflow/lib"));

        testProcess = new ProcessEntityParser().parse(LogMoverIT.class
                .getResourceAsStream("/org/apache/falcon/logging/process.xml"));
        testProcess.setName(PROCESS_NAME);
    }

    @AfterClass
    public void tearDown() {
        testCluster.shutdown();
    }

    @Test (enabled = false)
    public void testLogMover() throws Exception {
        CurrentUser.authenticate(System.getProperty("user.name"));
        OozieWorkflowEngine engine = new OozieWorkflowEngine();
        String path = StartupProperties.get().getProperty("system.lib.location");
        if (!new File("target/libs").exists()) {
            Assert.assertTrue(new File("target/libs").mkdirs());
        }
        StartupProperties.get().setProperty("system.lib.location", "target/libs");
        engine.schedule(testProcess);
        StartupProperties.get().setProperty("system.lib.location", path);

        OozieClient client = new OozieClient(
                ClusterHelper.getOozieUrl(testCluster.getCluster()));
        List<WorkflowJob> jobs;
        while (true) {
            jobs = client.getJobsInfo(OozieClient.FILTER_NAME + "="
                    + "FALCON_PROCESS_DEFAULT_" + PROCESS_NAME);
            if (jobs.size() > 0) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }

        WorkflowJob job = jobs.get(0);
        while (true) {
            if (!(job.getStatus() == WorkflowJob.Status.RUNNING || job
                    .getStatus() == WorkflowJob.Status.PREP)) {
                break;
            } else {
                Thread.sleep(1000);
                job = client.getJobInfo(job.getId());
            }
        }

        Path oozieLogPath = new Path(getLogPath(),
                "job-2010-01-01-01-00/000/oozie.log");
        Assert.assertTrue(fs.exists(oozieLogPath));

        testLogMoverWithNextRunId(job.getId());
        testLogMoverWithNextRunIdWithEngine(job.getId());
    }

    private Path getLogPath() throws FalconException {
        Path stagingPath = EntityUtil.getLogPath(testCluster.getCluster(), testProcess);
        return new Path(ClusterHelper.getStorageUrl(testCluster
                .getCluster()), stagingPath);
    }

    private void testLogMoverWithNextRunId(String jobId) throws Exception {
        LogMover.main(new String[]{"-workflowEngineUrl",
                                   ClusterHelper.getOozieUrl(testCluster.getCluster()),
                                   "-subflowId", jobId + "@user-workflow", "-runId", "1",
                                   "-logDir", getLogPath().toString() + "/job-2010-01-01-01-00",
                                   "-status", "SUCCEEDED", "-entityType", "process", });

        Path oozieLogPath = new Path(getLogPath(),
                "job-2010-01-01-01-00/001/oozie.log");
        Assert.assertTrue(fs.exists(oozieLogPath));
    }

    private void testLogMoverWithNextRunIdWithEngine(String jobId) throws Exception {
        LogMover.main(new String[]{"-workflowEngineUrl",
                                   ClusterHelper.getOozieUrl(testCluster.getCluster()),
                                   "-subflowId", jobId + "@user-workflow", "-runId", "1",
                                   "-logDir", getLogPath().toString() + "/job-2010-01-01-01-00",
                                   "-status", "SUCCEEDED", "-entityType", "process",
                                   "-userWorkflowEngine", "oozie", });

        Path oozieLogPath = new Path(getLogPath(),
                "job-2010-01-01-01-00/001/oozie.log");
        Assert.assertTrue(fs.exists(oozieLogPath));
    }
}

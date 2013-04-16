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

import junit.framework.Assert;
import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.SharedLibraryHostingService;
import org.apache.falcon.workflow.engine.OozieWorkflowEngine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;

/**
 * Requires Oozie to be running on localhost
 */
public class LogMoverTest {

    private static final ConfigurationStore store = ConfigurationStore.get();
    private static EmbeddedCluster testCluster = null;
    private static Process testProcess = null;
    private static String processName = "testProcess"
            + System.currentTimeMillis();
    FileSystem fs;

    @BeforeClass
    public void setup() throws Exception {
        cleanupStore();
        testCluster = EmbeddedCluster.newCluster("testCluster", true);
        store.publish(EntityType.CLUSTER, testCluster.getCluster());
        SharedLibraryHostingService listener = new SharedLibraryHostingService();
        listener.onAdd(testCluster.getCluster());
        fs = FileSystem.get(testCluster.getConf());
        fs.mkdirs(new Path("/workflow/lib"));

        fs.copyFromLocalFile(
                new Path(LogMoverTest.class.getResource(
                        "/org/apache/falcon/logging/workflow.xml").toURI()),
                new Path("/workflow"));
        fs.copyFromLocalFile(
                new Path(LogMoverTest.class.getResource(
                        "/org/apache/falcon/logging/java-test.jar").toURI()),
                new Path("/workflow/lib"));

        testProcess = new ProcessEntityParser().parse(LogMoverTest.class
                .getResourceAsStream("/org/apache/falcon/logging/process.xml"));
        testProcess.setName(processName);
        store.publish(EntityType.PROCESS, testProcess);
    }

    @AfterClass
    public void tearDown() {
        testCluster.shutdown();
    }

    private void cleanupStore() throws FalconException {
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }

    @Test
    public void testLogMover() throws Exception {
        CurrentUser.authenticate(System.getProperty("user.name"));
        OozieWorkflowEngine engine = new OozieWorkflowEngine();
        engine.schedule(testProcess);

        OozieClient client = new OozieClient(
                ClusterHelper.getOozieUrl(testCluster.getCluster()));
        List<WorkflowJob> jobs;
        while (true) {
            jobs = client.getJobsInfo(OozieClient.FILTER_NAME + "="
                    + "FALCON_PROCESS_DEFAULT_" + processName);
            if (jobs.size() > 0) {
                break;
            } else {
                Thread.sleep(100);
            }
        }

        WorkflowJob job = jobs.get(0);
        while (true) {
            if (!(job.getStatus() == WorkflowJob.Status.RUNNING || job
                    .getStatus() == WorkflowJob.Status.PREP)) {
                break;
            } else {
                Thread.sleep(100);
                job = client.getJobInfo(job.getId());
            }
        }

        Path oozieLogPath = new Path(getLogPath(),
                "job-2010-01-01-01-00/000/oozie.log");
        Assert.assertTrue(fs.exists(oozieLogPath));

        testLogMoverWithNextRunId(job.getId());

    }

    private Path getLogPath() throws FalconException {
        Path stagingPath = new Path(ClusterHelper.getLocation(
                testCluster.getCluster(), "staging"),
                EntityUtil.getStagingPath(testProcess) + "/../logs");
        Path logPath = new Path(ClusterHelper.getStorageUrl(testCluster
                .getCluster()), stagingPath);
        return logPath;
    }

    private void testLogMoverWithNextRunId(String jobId) throws Exception {
        LogMover.main(new String[]{"-workflowEngineUrl",
                                   ClusterHelper.getOozieUrl(testCluster.getCluster()),
                                   "-subflowId", jobId + "@user-workflow", "-runId", "1",
                                   "-logDir", getLogPath().toString() + "/job-2010-01-01-01-00",
                                   "-status", "SUCCEEDED", "-entityType", "process"});

        Path oozieLogPath = new Path(getLogPath(),
                "job-2010-01-01-01-00/001/oozie.log");
        Assert.assertTrue(fs.exists(oozieLogPath));

    }

}

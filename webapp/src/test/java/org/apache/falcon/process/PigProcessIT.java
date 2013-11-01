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

package org.apache.falcon.process;

import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.FSUtils;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

/**
 * Integration tests for Pig Processing Engine.
 *
 * This test is disabled as it heavily depends on oozie sharelibs for
 * pig and hcatalog being made available on HDFS. captured in FALCON-139.
 */
@Test (enabled = false)
public class PigProcessIT {

    private static final String CLUSTER_TEMPLATE = "/table/primary-cluster.xml";

    private final TestContext context = new TestContext();
    private Map<String, String> overlay;


    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare(CLUSTER_TEMPLATE);

        overlay = context.getUniqueOverlay();

        String filePath = context.overlayParametersOverTemplate(CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);

        final Cluster cluster = context.getCluster().getCluster();
        final String storageUrl = ClusterHelper.getStorageUrl(cluster);

        copyDataAndScriptsToHDFS(storageUrl);
        copyLibsToHDFS(cluster, storageUrl);
    }

    private void copyDataAndScriptsToHDFS(String storageUrl) throws IOException {
        // copyPigScriptToHDFS
        FSUtils.copyResourceToHDFS(
                "/apps/pig/id.pig", "id.pig", storageUrl + "/falcon/test/apps/pig");
        // copyTestDataToHDFS
        FSUtils.copyResourceToHDFS(
                "/apps/data/data.txt", "data.txt", storageUrl + "/falcon/test/input/2012/04/21/00");
    }

    private void copyLibsToHDFS(Cluster cluster, String storageUrl) throws IOException {
        // set up kahadb to be sent as part of workflows
        StartupProperties.get().setProperty("libext.paths", "./target/libext");
        String libext = ClusterHelper.getLocation(cluster, "working") + "/libext";
        FSUtils.copyOozieShareLibsToHDFS("./target/libext", storageUrl + libext);
    }

    @Test (enabled = false)
    public void testSubmitAndSchedulePigProcess() throws Exception {
        overlay.put("cluster", "primary-cluster");

        String filePath = context.overlayParametersOverTemplate(CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));
        // context.setCluster(filePath);

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                TestContext.executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                TestContext.executeWithURL("entity -submit -type feed -file " + filePath));

        final String pigProcessName = "pig-" + context.getProcessName();
        overlay.put("processName", pigProcessName);

        filePath = context.overlayParametersOverTemplate(TestContext.PIG_PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(0,
                TestContext.executeWithURL("entity -submitAndSchedule -type process -file " + filePath));

        WorkflowJob jobInfo = OozieTestUtils.getWorkflowJob(context.getCluster().getCluster(),
                OozieClient.FILTER_NAME + "=FALCON_PROCESS_DEFAULT_" + pigProcessName);
        Assert.assertEquals(WorkflowJob.Status.SUCCEEDED, jobInfo.getStatus());

        InstancesResult response = context.getService().path("api/instance/running/process/" + pigProcessName)
                .header("Remote-User", "guest")
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());

        // verify LogMover
        Path oozieLogPath = OozieTestUtils.getOozieLogPath(context.getCluster().getCluster(), jobInfo);
        Assert.assertTrue(context.getCluster().getFileSystem().exists(oozieLogPath));

        TestContext.executeWithURL("entity -delete -type process -name " + pigProcessName);
    }
}

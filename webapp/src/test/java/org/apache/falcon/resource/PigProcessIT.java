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

package org.apache.falcon.resource;

import org.apache.falcon.entity.ClusterHelper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Integration tests for Pig Processing Engine.
 */
@Test
public class PigProcessIT {

    private static final String SHARE_LIB_HDFS_PATH = "/user/oozie/share/lib";

    private final TestContext context = new TestContext();
    private Map<String, String> overlay;


    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();

        overlay = context.getUniqueOverlay();

        String filePath = context.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);

        String storageUrl = ClusterHelper.getStorageUrl(context.getCluster().getCluster());
        System.out.println("nn = " + storageUrl);
        TestContext.copyOozieShareLibsToHDFS("./target/sharelib", storageUrl + SHARE_LIB_HDFS_PATH);

        // copyPigScriptToHDFS
        TestContext.copyResourceToHDFS(
                "/apps/pig/id.pig", "id.pig", storageUrl + "/falcon/test/apps/pig");
        // copyTestDataToHDFS
        TestContext.copyResourceToHDFS(
                "/apps/pig/data.txt", "data.txt", storageUrl + "/falcon/test/input/2012/04/21/00");
    }

    @AfterClass
    public void tearDown() throws IOException {
        TestContext.deleteOozieShareLibsFromHDFS(SHARE_LIB_HDFS_PATH);
    }

    @Test
    public void testSubmitAndSchedulePigProcess() throws Exception {

        Thread.sleep(5000);

        String filePath = context.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));
        // context.setCluster(filePath);

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

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

/*
        WorkflowJob jobInfo = context.getWorkflowJob(
                OozieClient.FILTER_NAME + "=FALCON_PROCESS_DEFAULT_" + pigProcessName);
        Assert.assertEquals(WorkflowJob.Status.SUCCEEDED, jobInfo.getStatus());

        InstancesResult response = context.service.path("api/instance/running/process/" + pigProcessName)
                .header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON).get(InstancesResult.class);
        org.testng.Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        org.testng.Assert.assertNotNull(response.getInstances());
        org.testng.Assert.assertEquals(1, response.getInstances().length);

        // verify LogMover
        Path oozieLogPath = context.getOozieLogPath(jobInfo);
        System.out.println("oozieLogPath = " + oozieLogPath);
        Assert.assertTrue(context.getCluster().getFileSystem().exists(oozieLogPath));
*/

        TestContext.executeWithURL("entity -delete -type process -name " + pigProcessName);
    }
}

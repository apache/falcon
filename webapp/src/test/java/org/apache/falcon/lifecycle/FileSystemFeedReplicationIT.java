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

package org.apache.falcon.lifecycle;

import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.FSUtils;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

/**
 * Integration tests for Feed Replication with Table storage.
 *
 * This test is disabled as it heavily depends on oozie sharelibs for
 * hcatalog being made available on HDFS. captured in FALCON-139.
 */
@Test (enabled = false)
public class FileSystemFeedReplicationIT {

    private static final String PARTITION_VALUE = "2013-10-24-00"; // ${YEAR}-${MONTH}-${DAY}-${HOUR}
    private static final String SOURCE_LOCATION = "/falcon/test/primary-cluster/customer_raw/";
    private static final String TARGET_LOCATION = "/falcon/test/bcp-cluster/customer_bcp/";

    private final TestContext sourceContext = new TestContext();
    private final TestContext targetContext = new TestContext();

    private final TestContext targetAlphaContext = new TestContext();
    private final TestContext targetBetaContext = new TestContext();
    private final TestContext targetGammaContext = new TestContext();

    @BeforeClass
    public void setUp() throws Exception {
        TestContext.cleanupStore();

        Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String sourceFilePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        sourceContext.setCluster(sourceFilePath);

        final Cluster sourceCluster = sourceContext.getCluster().getCluster();
        String sourceStorageUrl = ClusterHelper.getStorageUrl(sourceCluster);

        // copyTestDataToHDFS
        final String sourcePath = sourceStorageUrl + SOURCE_LOCATION + PARTITION_VALUE;
        FSUtils.copyResourceToHDFS("/apps/data/data.txt", "data.txt", sourcePath);

        String targetFilePath = targetContext.overlayParametersOverTemplate("/table/bcp-cluster.xml", overlay);
        targetContext.setCluster(targetFilePath);

        final Cluster targetCluster = targetContext.getCluster().getCluster();
        copyLibsToHDFS(targetCluster);

        String file = targetAlphaContext.overlayParametersOverTemplate("/table/target-cluster-alpha.xml", overlay);
        targetAlphaContext.setCluster(file);
        copyLibsToHDFS(targetAlphaContext.getCluster().getCluster());

        file = targetBetaContext.overlayParametersOverTemplate("/table/target-cluster-beta.xml", overlay);
        targetBetaContext.setCluster(file);
        copyLibsToHDFS(targetBetaContext.getCluster().getCluster());

        file = targetGammaContext.overlayParametersOverTemplate("/table/target-cluster-gamma.xml", overlay);
        targetGammaContext.setCluster(file);
        copyLibsToHDFS(targetGammaContext.getCluster().getCluster());
    }

    private void copyLibsToHDFS(Cluster cluster) throws IOException {
        // set up kahadb to be sent as part of workflows
        StartupProperties.get().setProperty("libext.paths", "./target/libext");
        String libext = ClusterHelper.getLocation(cluster, "working") + "/libext";
        String targetStorageUrl = ClusterHelper.getStorageUrl(cluster);
        FSUtils.copyOozieShareLibsToHDFS("./target/libext", targetStorageUrl + libext);
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.executeWithURL("entity -delete -type feed -name customer-fs-replicating-feed");
        TestContext.executeWithURL("entity -delete -type cluster -name primary-cluster");
        TestContext.executeWithURL("entity -delete -type cluster -name bcp-cluster");

        cleanupStagingDirs(sourceContext.getCluster().getCluster());
        cleanupStagingDirs(targetContext.getCluster().getCluster());

        cleanupStagingDirs(targetAlphaContext.getCluster().getCluster());
        cleanupStagingDirs(targetBetaContext.getCluster().getCluster());
        cleanupStagingDirs(targetGammaContext.getCluster().getCluster());
    }

    private void cleanupStagingDirs(Cluster cluster) throws IOException {
        FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
        fs.delete(new Path("/falcon/test"), true);
    }

    @Test (enabled = false)
    public void testFSReplicationSingleSourceToTarget() throws Exception {
        final Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String filePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/bcp-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        // verify if the partition on the source exists - precondition
        FileSystem sourceFS = FileSystem.get(ClusterHelper.getConfiguration(sourceContext.getCluster().getCluster()));
        Assert.assertTrue(sourceFS.exists(new Path(SOURCE_LOCATION + PARTITION_VALUE)));

        filePath = sourceContext.overlayParametersOverTemplate("/table/customer-fs-replicating-feed.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        // wait until the workflow job completes
        final String feedName = "customer-fs-replicating-feed";
        WorkflowJob jobInfo = OozieTestUtils.getWorkflowJob(targetContext.getCluster().getCluster(),
                OozieClient.FILTER_NAME + "=FALCON_FEED_REPLICATION_" + feedName);
        Assert.assertEquals(jobInfo.getStatus(), WorkflowJob.Status.SUCCEEDED);

        Assert.assertTrue(sourceFS.exists(new Path(SOURCE_LOCATION + PARTITION_VALUE)));
        // verify if the partition on the target exists
        FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(targetContext.getCluster().getCluster()));
        Assert.assertTrue(fs.exists(new Path(TARGET_LOCATION + PARTITION_VALUE)));

        InstancesResult response = targetContext.getService().path("api/instance/running/feed/" + feedName)
                .header("Remote-User", "guest")
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);

        TestContext.executeWithURL("entity -delete -type feed -name " + feedName);
        TestContext.executeWithURL("entity -delete -type cluster -name primary-cluster");
        TestContext.executeWithURL("entity -delete -type cluster -name bcp-cluster");
    }

    @Test (enabled = false)
    public void testFSReplicationSingleSourceToMultipleTargets() throws Exception {
        final Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String filePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/target-cluster-alpha.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/target-cluster-beta.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/target-cluster-gamma.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        // verify if the partition on the source exists - precondition
        FileSystem sourceFS = FileSystem.get(ClusterHelper.getConfiguration(sourceContext.getCluster().getCluster()));
        Assert.assertTrue(sourceFS.exists(new Path(SOURCE_LOCATION + PARTITION_VALUE)));

        filePath = sourceContext.overlayParametersOverTemplate("/table/multiple-targets-replicating-feed.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        // wait until the workflow job completes
        final String feedName = "multiple-targets-replicating-feed";
        WorkflowJob jobInfo = OozieTestUtils.getWorkflowJob(targetContext.getCluster().getCluster(),
                OozieClient.FILTER_NAME + "=FALCON_FEED_REPLICATION_" + feedName);
        Assert.assertEquals(jobInfo.getStatus(), WorkflowJob.Status.SUCCEEDED);

        Assert.assertTrue(sourceFS.exists(new Path(SOURCE_LOCATION + PARTITION_VALUE)));

        // verify if the partition on the target exists
        FileSystem alpha = FileSystem.get(ClusterHelper.getConfiguration(targetAlphaContext.getCluster().getCluster()));
        Assert.assertTrue(
                alpha.exists(new Path("/falcon/test/target-cluster-alpha/customer_alpha/" + PARTITION_VALUE)));

        FileSystem beta = FileSystem.get(ClusterHelper.getConfiguration(targetBetaContext.getCluster().getCluster()));
        Assert.assertTrue(beta.exists(new Path("/falcon/test/target-cluster-beta/customer_beta/" + PARTITION_VALUE)));

        FileSystem gamma = FileSystem.get(ClusterHelper.getConfiguration(targetGammaContext.getCluster().getCluster()));
        Assert.assertTrue(
                gamma.exists(new Path("/falcon/test/target-cluster-gamma/customer_gamma/" + PARTITION_VALUE)));

        InstancesResult response = targetContext.getService().path("api/instance/running/feed/" + feedName)
                .header("Remote-User", "guest")
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);

        TestContext.executeWithURL("entity -delete -type feed -name " + feedName);
        TestContext.executeWithURL("entity -delete -type cluster -name primary-cluster");
        TestContext.executeWithURL("entity -delete -type cluster -name target-cluster-alpha");
        TestContext.executeWithURL("entity -delete -type cluster -name target-cluster-beta");
        TestContext.executeWithURL("entity -delete -type cluster -name target-cluster-gamma");
    }

    @Test (enabled = false)
    public void testFSReplicationComplex() throws Exception {
        // copyTestDataToHDFS
        Cluster sourceCluster = sourceContext.getCluster().getCluster();
        FileSystem sourceFS = FileSystem.get(ClusterHelper.getConfiguration(sourceCluster));
        String sourceStorageUrl = ClusterHelper.getStorageUrl(sourceCluster);
        final String partitionValue = "2012-10-01-12";
        String sourceLocation = sourceStorageUrl + SOURCE_LOCATION + partitionValue + "/" + sourceCluster.getColo();
        FSUtils.copyResourceToHDFS("/apps/data/data.txt", "data.txt", sourceLocation);
        Path sourcePath = new Path(sourceLocation);
        Assert.assertTrue(sourceFS.exists(sourcePath));

        final Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String filePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/target-cluster-alpha.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/target-cluster-beta.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        // verify if the partition on the source exists - precondition
        Assert.assertTrue(sourceFS.exists(sourcePath));

        filePath = sourceContext.overlayParametersOverTemplate("/table/complex-replicating-feed.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        // wait until the workflow job completes
        final String feedName = "complex-replicating-feed";
        WorkflowJob jobInfo = OozieTestUtils.getWorkflowJob(targetContext.getCluster().getCluster(),
                OozieClient.FILTER_NAME + "=FALCON_FEED_REPLICATION_" + feedName);
        Assert.assertEquals(jobInfo.getStatus(), WorkflowJob.Status.SUCCEEDED);

        Assert.assertTrue(sourceFS.exists(new Path(SOURCE_LOCATION + partitionValue)));

        // verify if the partition on the target exists
        FileSystem alpha = FileSystem.get(ClusterHelper.getConfiguration(targetAlphaContext.getCluster().getCluster()));
        Assert.assertTrue(alpha.exists(new Path("/localDC/rc/billing/ua1/" + partitionValue)));

        FileSystem beta = FileSystem.get(ClusterHelper.getConfiguration(targetBetaContext.getCluster().getCluster()));
        Assert.assertTrue(beta.exists(new Path("/localDC/rc/billing/ua2/" + partitionValue)));

        InstancesResult response = targetContext.getService().path("api/instance/running/feed/" + feedName)
                .header("Remote-User", "guest")
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);

        TestContext.executeWithURL("entity -delete -type feed -name " + feedName);
        TestContext.executeWithURL("entity -delete -type cluster -name primary-cluster");
        TestContext.executeWithURL("entity -delete -type cluster -name target-cluster-alpha");
        TestContext.executeWithURL("entity -delete -type cluster -name target-cluster-beta");
    }
}

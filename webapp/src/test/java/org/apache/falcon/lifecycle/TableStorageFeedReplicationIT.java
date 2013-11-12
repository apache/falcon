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
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.FSUtils;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for Feed Replication with Table storage.
 *
 * This test is disabled as it heavily depends on oozie sharelibs for
 * hcatalog being made available on HDFS. captured in FALCON-139.
 */
@Test (enabled = false)
public class TableStorageFeedReplicationIT {

    private static final String SOURCE_DATABASE_NAME = "src_demo_db";
    private static final String SOURCE_TABLE_NAME = "customer_raw";

    private static final String TARGET_DATABASE_NAME = "tgt_demo_db";
    private static final String TARGET_TABLE_NAME = "customer_bcp";

    private static final String PARTITION_VALUE = "2013102400"; // ${YEAR}${MONTH}${DAY}${HOUR}

    private final TestContext sourceContext = new TestContext();
    private String sourceMetastoreUrl;

    private final TestContext targetContext = new TestContext();
    private String targetMetastoreUrl;


    @BeforeClass
    public void setUp() throws Exception {
        TestContext.cleanupStore();

        Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String sourceFilePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        sourceContext.setCluster(sourceFilePath);

        final Cluster sourceCluster = sourceContext.getCluster().getCluster();
        String sourceStorageUrl = ClusterHelper.getStorageUrl(sourceCluster);

        // copyTestDataToHDFS
        final String sourcePath = sourceStorageUrl + "/falcon/test/input/" + PARTITION_VALUE;
        FSUtils.copyResourceToHDFS("/apps/data/data.txt", "data.txt", sourcePath);

        sourceMetastoreUrl = ClusterHelper.getInterface(sourceCluster, Interfacetype.REGISTRY).getEndpoint();
        setupHiveMetastore(sourceMetastoreUrl, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME);
        HiveTestUtils.loadData(sourceMetastoreUrl, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME, sourcePath,
                PARTITION_VALUE);

        String targetFilePath = targetContext.overlayParametersOverTemplate("/table/bcp-cluster.xml", overlay);
        targetContext.setCluster(targetFilePath);

        final Cluster targetCluster = targetContext.getCluster().getCluster();
        targetMetastoreUrl = ClusterHelper.getInterface(targetCluster, Interfacetype.REGISTRY).getEndpoint();
        setupHiveMetastore(targetMetastoreUrl, TARGET_DATABASE_NAME, TARGET_TABLE_NAME);

        copyLibsToHDFS(targetCluster);
    }

    private void setupHiveMetastore(String metastoreUrl, String databaseName,
                                    String tableName) throws Exception {
        cleanupHiveMetastore(metastoreUrl, databaseName, tableName);

        HiveTestUtils.createDatabase(metastoreUrl, databaseName);
        final List<String> partitionKeys = Arrays.asList("ds");
        HiveTestUtils.createTable(metastoreUrl, databaseName, tableName, partitionKeys);
        // todo this is a kludge to work around hive's limitations
        HiveTestUtils.alterTable(metastoreUrl, databaseName, tableName);
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
        cleanupHiveMetastore(sourceMetastoreUrl, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME);
        cleanupHiveMetastore(targetMetastoreUrl, TARGET_DATABASE_NAME, TARGET_TABLE_NAME);

        cleanupStagingDirs(sourceContext.getCluster().getCluster(), SOURCE_DATABASE_NAME);
        cleanupStagingDirs(targetContext.getCluster().getCluster(), TARGET_DATABASE_NAME);
    }

    private void cleanupHiveMetastore(String metastoreUrl, String databaseName, String tableName) throws Exception {
        HiveTestUtils.dropTable(metastoreUrl, databaseName, tableName);
        HiveTestUtils.dropDatabase(metastoreUrl, databaseName);
    }

    private void cleanupStagingDirs(Cluster cluster, String databaseName) throws IOException {
        FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
        String stagingDir = "/apps/falcon/staging/"
                + "FALCON_FEED_REPLICATION_customer-table-replicating-feed_primary-cluster/"
                + databaseName;
        fs.delete(new Path(stagingDir), true);
        fs.delete(new Path("/falcon/test/input"), true);
    }

    @Test (enabled = false)
    public void testTableReplication() throws Exception {
        final String feedName = "customer-table-replicating-feed";
        final Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String filePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/bcp-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        HCatPartition sourcePartition = HiveTestUtils.getPartition(
                sourceMetastoreUrl, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME, "ds", PARTITION_VALUE);
        Assert.assertNotNull(sourcePartition);

        filePath = sourceContext.overlayParametersOverTemplate("/table/customer-table-replicating-feed.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        // wait until the workflow job completes
        WorkflowJob jobInfo = OozieTestUtils.getWorkflowJob(targetContext.getCluster().getCluster(),
                OozieClient.FILTER_NAME + "=FALCON_FEED_REPLICATION_" + feedName);
        Assert.assertEquals(jobInfo.getStatus(), WorkflowJob.Status.SUCCEEDED);

        // verify if the partition on the target exists
        HCatPartition targetPartition = HiveTestUtils.getPartition(
                targetMetastoreUrl, TARGET_DATABASE_NAME, TARGET_TABLE_NAME, "ds", PARTITION_VALUE);
        Assert.assertNotNull(targetPartition);

        InstancesResult response = targetContext.getService().path("api/instance/running/feed/" + feedName)
                .header("Remote-User", "guest")
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);

        TestContext.executeWithURL("entity -delete -type feed -name customer-table-replicating-feed");
        TestContext.executeWithURL("entity -delete -type cluster -name primary-cluster");
        TestContext.executeWithURL("entity -delete -type cluster -name bcp-cluster");
    }

    @Test (enabled = false)
    public void testTableReplicationWithExistingTargetPartition() throws Exception {
        final String feedName = "customer-table-replicating-feed";
        final Map<String, String> overlay = sourceContext.getUniqueOverlay();
        String filePath = sourceContext.overlayParametersOverTemplate("/table/primary-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        filePath = targetContext.overlayParametersOverTemplate("/table/bcp-cluster.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submit -type cluster -file " + filePath));

        HCatPartition sourcePartition = HiveTestUtils.getPartition(
                sourceMetastoreUrl, SOURCE_DATABASE_NAME, SOURCE_TABLE_NAME, "ds", PARTITION_VALUE);
        Assert.assertNotNull(sourcePartition);

        addPartitionToTarget();
        // verify if the partition on the target exists before replication starts
        // to see import drops partition before importing partition
        HCatPartition targetPartition = HiveTestUtils.getPartition(
                targetMetastoreUrl, TARGET_DATABASE_NAME, TARGET_TABLE_NAME, "ds", PARTITION_VALUE);
        Assert.assertNotNull(targetPartition);

        filePath = sourceContext.overlayParametersOverTemplate("/table/customer-table-replicating-feed.xml", overlay);
        Assert.assertEquals(0, TestContext.executeWithURL("entity -submitAndSchedule -type feed -file " + filePath));

        // wait until the workflow job completes
        WorkflowJob jobInfo = OozieTestUtils.getWorkflowJob(targetContext.getCluster().getCluster(),
                OozieClient.FILTER_NAME + "=FALCON_FEED_REPLICATION_" + feedName);
        Assert.assertEquals(jobInfo.getStatus(), WorkflowJob.Status.SUCCEEDED);

        // verify if the partition on the target exists
        targetPartition = HiveTestUtils.getPartition(
                targetMetastoreUrl, TARGET_DATABASE_NAME, TARGET_TABLE_NAME, "ds", PARTITION_VALUE);
        Assert.assertNotNull(targetPartition);

        InstancesResult response = targetContext.getService().path("api/instance/running/feed/" + feedName)
                .header("Remote-User", "guest")
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);

        TestContext.executeWithURL("entity -delete -type feed -name customer-table-replicating-feed");
        TestContext.executeWithURL("entity -delete -type cluster -name primary-cluster");
        TestContext.executeWithURL("entity -delete -type cluster -name bcp-cluster");
    }

    private void addPartitionToTarget() throws Exception {
        final Cluster targetCluster = targetContext.getCluster().getCluster();
        String targetStorageUrl = ClusterHelper.getStorageUrl(targetCluster);

        // copyTestDataToHDFS
        final String targetPath = targetStorageUrl + "/falcon/test/input/" + PARTITION_VALUE;
        FSUtils.copyResourceToHDFS("/apps/data/data.txt", "data.txt", targetPath);

        HiveTestUtils.loadData(targetMetastoreUrl, TARGET_DATABASE_NAME, TARGET_TABLE_NAME,
                targetPath, PARTITION_VALUE);
    }
}

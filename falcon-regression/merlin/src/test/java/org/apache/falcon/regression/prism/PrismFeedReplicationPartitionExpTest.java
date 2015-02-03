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

package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Replication feed with partitions as expression language variables tests.
 */
@Test(groups = "distributed")
public class PrismFeedReplicationPartitionExpTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private FileSystem cluster1FS = serverFS.get(0);
    private FileSystem cluster2FS = serverFS.get(1);
    private FileSystem cluster3FS = serverFS.get(2);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private String testDate = "/2012/10/01/12/";
    private String baseTestDir = cleanAndGetTestDir();
    private String testBaseDir1 = baseTestDir + "/localDC/rc/billing";
    private String testBaseDir2 = baseTestDir + "/clusterPath/localDC/rc/billing";
    private String testBaseDir3 = baseTestDir + "/dataBillingRC/fetlrc/billing";
    private String testBaseDir4 = baseTestDir + "/sourcetarget";
    private String testBaseDirServer1Source = baseTestDir + "/source1";
    private String testDirWithDate = testBaseDir1 + testDate;
    private String testDirWithDateSourceTarget = testBaseDir4 + testDate;
    private String testDirWithDateSource1 = testBaseDirServer1Source + testDate;
    private String testFile1 = OSUtil.NORMAL_INPUT + "dataFile.xml";
    private String testFile2 = OSUtil.RESOURCES + OSUtil.getPath("pig", "id.pig");
    private String testFile3 = OSUtil.RESOURCES + OSUtil.getPath("ELbundle", "cluster-0.1.xml");
    private String testFile4 = OSUtil.NORMAL_INPUT + "dataFile.properties";
    private static final Logger LOGGER =
        Logger.getLogger(PrismFeedReplicationPartitionExpTest.class);


// pt : partition in target
// ps: partition in source


    private void uploadDataToServer3(String location, String fileName) throws IOException {
        HadoopUtil.recreateDir(cluster3FS, location);
        HadoopUtil.copyDataToFolder(cluster3FS, location, fileName);
    }

    private void uploadDataToServer1(String location, String fileName) throws IOException {
        HadoopUtil.recreateDir(cluster1FS, location);
        HadoopUtil.copyDataToFolder(cluster1FS, location, fileName);
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        LOGGER.info("creating test data");

        uploadDataToServer3(testDirWithDate + "00/ua2/", testFile1);
        uploadDataToServer3(testDirWithDate + "05/ua2/", testFile2);
        uploadDataToServer3(testDirWithDate + "10/ua2/", testFile3);
        uploadDataToServer3(testDirWithDate + "15/ua2/", testFile4);
        uploadDataToServer3(testDirWithDate + "20/ua2/", testFile4);

        uploadDataToServer3(testDirWithDate + "00/ua1/", testFile1);
        uploadDataToServer3(testDirWithDate + "05/ua1/", testFile2);
        uploadDataToServer3(testDirWithDate + "10/ua1/", testFile3);
        uploadDataToServer3(testDirWithDate + "15/ua1/", testFile4);
        uploadDataToServer3(testDirWithDate + "20/ua1/", testFile4);

        uploadDataToServer3(testDirWithDate + "00/ua3/", testFile1);
        uploadDataToServer3(testDirWithDate + "05/ua3/", testFile2);
        uploadDataToServer3(testDirWithDate + "10/ua3/", testFile3);
        uploadDataToServer3(testDirWithDate + "15/ua3/", testFile4);
        uploadDataToServer3(testDirWithDate + "20/ua3/", testFile4);

        uploadDataToServer3(testBaseDir3 + testDate + "00/ua2/", testFile1);
        uploadDataToServer3(testBaseDir3 + testDate + "05/ua2/", testFile2);
        uploadDataToServer3(testBaseDir3 + testDate + "10/ua2/", testFile3);
        uploadDataToServer3(testBaseDir3 + testDate + "15/ua2/", testFile4);
        uploadDataToServer3(testBaseDir3 + testDate + "20/ua2/", testFile4);


        uploadDataToServer3(testBaseDir3 + testDate + "00/ua1/", testFile1);
        uploadDataToServer3(testBaseDir3 + testDate + "05/ua1/", testFile2);
        uploadDataToServer3(testBaseDir3 + testDate + "10/ua1/", testFile3);
        uploadDataToServer3(testBaseDir3 + testDate + "15/ua1/", testFile4);
        uploadDataToServer3(testBaseDir3 + testDate + "20/ua1/", testFile4);


        uploadDataToServer3(testBaseDir3 + testDate + "00/ua3/", testFile1);
        uploadDataToServer3(testBaseDir3 + testDate + "05/ua3/", testFile2);
        uploadDataToServer3(testBaseDir3 + testDate + "10/ua3/", testFile3);
        uploadDataToServer3(testBaseDir3 + testDate + "15/ua3/", testFile4);
        uploadDataToServer3(testBaseDir3 + testDate + "20/ua3/", testFile4);


        //data for test normalTest_1s2t_pst where both source target partition are required

        uploadDataToServer3(testDirWithDateSourceTarget + "00/ua3/ua2/", testFile1);
        uploadDataToServer3(testDirWithDateSourceTarget + "05/ua3/ua2/", testFile2);
        uploadDataToServer3(testDirWithDateSourceTarget + "10/ua3/ua2/", testFile3);
        uploadDataToServer3(testDirWithDateSourceTarget + "15/ua3/ua2/", testFile4);
        uploadDataToServer3(testDirWithDateSourceTarget + "20/ua3/ua2/", testFile4);

        uploadDataToServer3(testDirWithDateSourceTarget + "00/ua3/ua1/", testFile1);
        uploadDataToServer3(testDirWithDateSourceTarget + "05/ua3/ua1/", testFile2);
        uploadDataToServer3(testDirWithDateSourceTarget + "10/ua3/ua1/", testFile3);
        uploadDataToServer3(testDirWithDateSourceTarget + "15/ua3/ua1/", testFile4);
        uploadDataToServer3(testDirWithDateSourceTarget + "20/ua3/ua1/", testFile4);

        // data when server 1 acts as source
        uploadDataToServer1(testDirWithDateSource1 + "00/ua2/", testFile1);
        uploadDataToServer1(testDirWithDateSource1 + "05/ua2/", testFile2);


        uploadDataToServer1(testDirWithDateSource1 + "00/ua1/", testFile1);
        uploadDataToServer1(testDirWithDateSource1 + "05/ua1/", testFile2);


        uploadDataToServer1(testDirWithDateSource1 + "00/ua3/", testFile1);
        uploadDataToServer1(testDirWithDateSource1 + "05/ua3/", testFile2);

        LOGGER.info("completed creating test data");

    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readFeedReplicationBundle();

        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        for (String dir : new String[]{testBaseDir1, testBaseDir2, testBaseDir3, testBaseDir4}) {
            HadoopUtil.deleteDirIfExists(dir, cluster1FS);
            HadoopUtil.deleteDirIfExists(dir, cluster2FS);
        }
        removeTestClassEntities();
    }


    @Test(enabled = true, groups = "embedded")
    public void blankPartition() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally
        //partition is left blank

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2012-10-01T12:10Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("")
                .withDataLocation(testBaseDir1 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2012-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("")
                .withDataLocation(testBaseDir2 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("")
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertFailed(r, "submit of feed should have failed as the partition in source "
            + "is blank");
    }


    @Test(enabled = true)
    public void normalTest1Source1Target1NeutralPartitionedSource() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 1 source clusters cluster3
        //cluster2 is the target
        //data should be replicated to cluster2 from cluster3

        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);
        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";


        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2099-10-01T12:10Z")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2099-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(testBaseDir2 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir1 + MINUTE_DATE_PATTERN)
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(feed.toString());
        AssertUtil.assertSucceeded(r);
        TimeUtil.sleepSeconds(15);

        HadoopUtil.recreateDir(cluster3FS, testDirWithDate + "00/ua3/");
        HadoopUtil.recreateDir(cluster3FS, testDirWithDate + "05/ua3/");

        HadoopUtil.copyDataToFolder(cluster3FS, testDirWithDate + "00/ua3/",
            testFile1);
        HadoopUtil.copyDataToFolder(cluster3FS, testDirWithDate + "05/ua3/",
            testFile2);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, feed.getName(), 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(), feed.getName(),
                "REPLICATION"), 1);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(), feed.getName(),
                "RETENTION"), 1);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), feed.getName(),
                "RETENTION"), 1);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster3.getFeedHelper(), feed.getName(),
                "RETENTION"), 1);


        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir2));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua2");


        List<Path> ua3ReplicatedData00 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "00/ua3/"));
        List<Path> ua3ReplicatedData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "05/ua3/"));

        List<Path> ua2ReplicatedData00 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir2 + testDate + "00"));
        List<Path> ua2ReplicatedData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir2 + testDate + "05"));

        AssertUtil.checkForListSizes(ua3ReplicatedData00, ua2ReplicatedData00);
        AssertUtil.checkForListSizes(ua3ReplicatedData05, ua2ReplicatedData05);
    }


    @Test(enabled = true)
    public void normalTest1Source1Target1NeutralPartitionedTarget() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally
        // path for data in target cluster should also be customized
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";


        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2099-10-01T12:10Z")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2099-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir2 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withDataLocation(testBaseDir1 + MINUTE_DATE_PATTERN)
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitAndSchedule(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, feed.getName(), 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), feed.getName(),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), feed.getName(),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), feed.getName(),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), feed.getName(),
                "RETENTION"), 1);


        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData =
            HadoopUtil.getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir2));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua3ReplicatedData00 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "00/ua2/"));
        List<Path> ua3ReplicatedData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "05/ua2/"));

        List<Path> ua2ReplicatedData00 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir2 + testDate + "00"));
        List<Path> ua2ReplicatedData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir2 + testDate + "05"));

        AssertUtil.checkForListSizes(ua3ReplicatedData00, ua2ReplicatedData00);
        AssertUtil.checkForListSizes(ua3ReplicatedData05, ua2ReplicatedData05);
    }


    @Test(enabled = true)
    public void normalTest1Source2TargetPartitionedTarget() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //cluster3 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on cluster1 and cluster2 as targets
        //ua3 is the source and ua1 and ua2 are target

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);
        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.setFilePath(testBaseDir3 + MINUTE_DATE_PATTERN);

        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2012-10-01T12:10Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("${cluster.colo}")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2012-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("${cluster.colo}")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());


        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed.toString()));
        TimeUtil.sleepSeconds(15);

        InstanceUtil.waitTillInstanceReachState(cluster1OC, feed.getName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, feed.getName(), 3,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(testBaseDir3 + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2", "ua3");

        List<Path> ua2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS,
                new Path(testBaseDir3 + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1", "ua3");


        List<Path> ua1ReplicatedData00 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(testBaseDir3 + testDate + "00/"));
        List<Path> ua1ReplicatedData10 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(testBaseDir3 + testDate + "10/"));

        List<Path> ua2ReplicatedData10 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir3 + testDate + "10"));
        List<Path> ua2ReplicatedData15 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir3 + testDate + "15"));

        List<Path> ua3OriginalData10ua1 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir3 + testDate + "10/ua1"));
        List<Path> ua3OriginalData10ua2 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir3 + testDate + "10/ua2"));
        List<Path> ua3OriginalData15ua2 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir3 + testDate + "15/ua2"));

        AssertUtil.checkForListSizes(ua1ReplicatedData00, new ArrayList<Path>());
        AssertUtil.checkForListSizes(ua1ReplicatedData10, ua3OriginalData10ua1);
        AssertUtil.checkForListSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForListSizes(ua2ReplicatedData15, ua3OriginalData15ua2);
    }

    @Test(enabled = true, groups = "embedded")
    public void normalTest2Source1TargetPartitionedTarget() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 2 source clusters cluster3 and cluster1
        //cluster2 is the target
        // Since there is no partition expression in source clusters, the feed submission should
        // fail (FALCON-305).

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2012-10-01T12:10Z")
                .withClusterType(ClusterType.SOURCE)
                .withDataLocation(testBaseDir1 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2012-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir2 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());

        //clean target if old data exists
        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        AssertUtil.assertFailed(r, "Submission of feed should have failed.");
        Assert.assertTrue(r.getMessage().contains(
                "Partition expression has to be specified for cluster "
                    + Util.readEntityName(bundles[0].getClusters().get(0))
                    + " as there are more than one source clusters"),
            "Failed response has unexpected error message.");
    }


    @Test(enabled = true)
    public void normalTest1Source2TargetPartitionedSource() throws Exception {

        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //cluster3 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on cluster1 and cluster2 as targets
        //ua3 is the source and ua1 and ua2 are target
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";


        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.setFilePath(testBaseDir1 + MINUTE_DATE_PATTERN);
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2012-10-01T12:11Z")
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(testBaseDir1 + "/ua1" + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2012-10-01T12:26Z")
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(testBaseDir1 + "/ua2" + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(10000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("${cluster.colo}")
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed.toString()));
        TimeUtil.sleepSeconds(15);

        InstanceUtil.waitTillInstanceReachState(cluster1OC, feed.getName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);
        InstanceUtil.waitTillInstanceReachState(cluster2OC, feed.getName(), 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(testBaseDir1 + "/ua1" + testDate));
        //check for no ua2 or ua3 in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir1 + "/ua2" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1");


        List<Path> ua1ReplicatedData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS,
                new Path(testBaseDir1 + "/ua1" + testDate + "05/"));
        List<Path> ua1ReplicatedData10 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS,
                new Path(testBaseDir1 + "/ua1" + testDate + "10/"));

        List<Path> ua2ReplicatedData10 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir1 + "/ua2" + testDate + "10"));
        List<Path> ua2ReplicatedData15 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir1 + "/ua2" + testDate + "15"));

        List<Path> ua3OriginalData10ua1 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "10/ua1"));
        List<Path> ua3OriginalData05ua1 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "05/ua1"));
        List<Path> ua3OriginalData10ua2 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "10/ua2"));
        List<Path> ua3OriginalData15ua2 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "15/ua2"));

        AssertUtil.checkForListSizes(ua1ReplicatedData10, ua3OriginalData10ua1);
        AssertUtil.checkForListSizes(ua1ReplicatedData05, ua3OriginalData05ua1);
        AssertUtil.checkForListSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForListSizes(ua2ReplicatedData15, ua3OriginalData15ua2);

    }


    @Test(enabled = true)
    public void normalTest2Source1TargetPartitionedSource() throws Exception {
        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        // there are 2 source clusters cluster3 and cluster1
        //cluster2 is the target
        //data should be replicated to cluster2 from ua2 sub dir of cluster3 and cluster1
        // source cluster path in cluster1 should be mentioned in cluster definition
        // path for data in target cluster should also be customized

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2099-10-01T12:10Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDirServer1Source + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2099-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(testBaseDir2 + "/replicated" + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir1 + MINUTE_DATE_PATTERN)
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(feed.toString());
        AssertUtil.assertSucceeded(r);
        TimeUtil.sleepSeconds(15);

        InstanceUtil.waitTillInstanceReachState(cluster2OC, feed.getName(), 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua2ReplicatedData = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2FS,
            new Path(testBaseDir2 + "/replicated" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData00ua1 = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2FS,
            new Path(testBaseDir2 + "/replicated" + testDate + "00/ua1"));
        List<Path> ua2ReplicatedData05ua3 = HadoopUtil.getAllFilesRecursivelyHDFS(cluster2FS,
            new Path(testBaseDir2 + "/replicated" + testDate + "05/ua3/"));


        List<Path> ua1OriginalData00 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(
                testBaseDirServer1Source + testDate + "00/ua1"));
        List<Path> ua3OriginalData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(testDirWithDate + "05/ua1"));

        AssertUtil.checkForListSizes(ua2ReplicatedData00ua1, ua1OriginalData00);
        AssertUtil.checkForListSizes(ua2ReplicatedData05ua3, ua3OriginalData05);
    }


    @Test(enabled = true)
    public void normalTest1Source2TargetPartitionedSourceTarget() throws Exception {


        //this test is for ideal condition when data is present in all the required places and
        // replication takes
        // place normally

        //cluster3 is global cluster where test data is present in location
        // /data/fetlrc/billing/2012/10/01/12/
        // (00 to 30)
        //data should be replicated to folder on cluster1 and cluster2 as targets
        //ua3 is the source and ua1 and ua2 are target
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.setFilePath(testBaseDir1 + MINUTE_DATE_PATTERN);
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2099-10-01T12:10Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir1 + "/ua1" + MINUTE_DATE_PATTERN + "/")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2099-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir1 + "/ua2" + MINUTE_DATE_PATTERN + "/")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("${cluster.colo}")
                .withDataLocation(testBaseDir4 + MINUTE_DATE_PATTERN + "/")
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed.toString()));
        TimeUtil.sleepSeconds(15);
        InstanceUtil.waitTillInstanceReachState(cluster1OC, feed.getName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);
        InstanceUtil.waitTillInstanceReachState(cluster2OC, feed.getName(), 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED, 20);

        //check if data has been replicated correctly

        //on ua1 only ua1 should be replicated, ua2 only ua2
        //number of files should be same as source


        List<Path> ua1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(testBaseDir1 + "/ua1" + testDate));
        //check for no ua2  in ua1
        AssertUtil.failIfStringFoundInPath(ua1ReplicatedData, "ua2");

        List<Path> ua2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir1 + "/ua2" + testDate));
        AssertUtil.failIfStringFoundInPath(ua2ReplicatedData, "ua1");


        List<Path> ua1ReplicatedData05 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS,
                new Path(testBaseDir1 + "/ua1" + testDate + "05/"));
        List<Path> ua1ReplicatedData10 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS,
                new Path(testBaseDir1 + "/ua1" + testDate + "10/"));

        List<Path> ua2ReplicatedData10 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir1 + "/ua2" + testDate + "10"));
        List<Path> ua2ReplicatedData15 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(testBaseDir1 + "/ua2" + testDate + "15"));


        List<Path> ua3OriginalData05ua1 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(
                testDirWithDateSourceTarget + "05/ua3/ua1"));
        List<Path> ua3OriginalData10ua1 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(
                testDirWithDateSourceTarget + "10/ua3/ua1"));
        List<Path> ua3OriginalData10ua2 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(
                testDirWithDateSourceTarget + "10/ua3/ua2"));
        List<Path> ua3OriginalData15ua2 = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, new Path(
                testDirWithDateSourceTarget + "15/ua3/ua2"));

        AssertUtil.checkForListSizes(ua1ReplicatedData05, ua3OriginalData05ua1);
        AssertUtil.checkForListSizes(ua1ReplicatedData10, ua3OriginalData10ua1);
        AssertUtil.checkForListSizes(ua2ReplicatedData10, ua3OriginalData10ua2);
        AssertUtil.checkForListSizes(ua2ReplicatedData15, ua3OriginalData15ua2);
    }


    @Test(enabled = true, groups = "embedded")
    public void moreThanOneClusterWithSameNameDiffValidity() throws Exception {
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String startTimeUA1 = "2012-10-01T12:05Z";
        String startTimeUA2 = "2012-10-01T12:10Z";

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA1, "2012-10-01T12:10Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("")
                .withDataLocation(testBaseDir1 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTimeUA2, "2012-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withPartition("")
                .withDataLocation(testBaseDir2 + MINUTE_DATE_PATTERN)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity("2012-10-01T12:00Z", "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withPartition("")
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        ServiceResponse r = prism.getFeedHelper().submitEntity(feed.toString());
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertFailed(r, "is defined more than once for feed");
        Assert.assertTrue(r.getMessage().contains("is defined more than once for feed"));
    }
}

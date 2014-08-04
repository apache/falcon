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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

@Test(groups = "distributed")
public class PrismFeedLateReplicationTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster2FS = serverFS.get(1);
    FileSystem cluster3FS = serverFS.get(2);
    private String baseTestDir = baseHDFSDir + "/PrismFeedLateReplicationTest";
    private String inputPath =
        baseTestDir + "/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedLateReplicationTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(groups = {"multiCluster"})
    public void multipleSourceOneTarget_pastData() throws Exception {

        bundles[0].setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        String postFix = "/US/" + cluster2.getClusterHelper().getColoName();
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.lateDataReplenish(cluster2FS, 90, 1, prefix, postFix);


        postFix = "/UK/" + cluster3.getClusterHelper().getColoName();
        prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        String startTime = TimeUtil.getTimeWrtSystemTime(-30);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "US/${cluster.colo}");

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
            null);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
            "UK/${cluster.colo}");


        logger.info("feed: " + Util.prettyPrintXml(feed));

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        TimeUtil.sleepSeconds(10);

        String bundleId =
            InstanceUtil.getLatestBundleID(cluster1, Util.readEntityName(feed), EntityType.FEED);

        //wait till 1st instance of replication coord is SUCCEEDED
        List<String> replicationCoordIDTarget = InstanceUtil
            .getReplicationCoordID(bundleId, cluster1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(cluster1, replicationCoordIDTarget.get(0),
                0)
                == WorkflowJob.Status.SUCCEEDED
                && InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0)
                == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            TimeUtil.sleepSeconds(20);
        }

        TimeUtil.sleepSeconds(15);

        List<String> inputFolderListForColo1 =
            InstanceUtil.getInputFoldersForInstanceForReplication(cluster1,
                replicationCoordIDTarget.get(0), 1);
        List<String> inputFolderListForColo2 =
            InstanceUtil.getInputFoldersForInstanceForReplication(cluster1,
                replicationCoordIDTarget.get(1), 1);

        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(cluster3FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo2);
    }

    @Test(groups = {"multiCluster"})
    public void multipleSourceOneTarget_futureData() throws Exception {

        bundles[0].setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);


        String startTime = TimeUtil.getTimeWrtSystemTime(3);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "US/${cluster.colo}");

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
            null);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
            "UK/${cluster.colo}");


        logger.info("feed: " + Util.prettyPrintXml(feed));

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        TimeUtil.sleepSeconds(10);

        String postFix = "/US/" + cluster2.getClusterHelper().getColoName();
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.lateDataReplenish(cluster2FS, 90, 1, prefix, postFix);

        postFix = "/UK/" + cluster3.getClusterHelper().getColoName();
        prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        TimeUtil.sleepSeconds(60);

        //wait till 1st instance of replication coord is SUCCEEDED
        String bundleId = InstanceUtil
            .getLatestBundleID(cluster1, Util.readEntityName(feed), EntityType.FEED);

        List<String> replicationCoordIDTarget = InstanceUtil.getReplicationCoordID(bundleId,
            cluster1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(cluster1, replicationCoordIDTarget.get(0),
                0)
                == WorkflowJob.Status.SUCCEEDED
                && InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0)
                == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            logger.info("still in for loop");
            TimeUtil.sleepSeconds(20);
        }

        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(0), 0),
            WorkflowJob.Status.SUCCEEDED);
        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0),
            WorkflowJob.Status.SUCCEEDED);

        TimeUtil.sleepSeconds(15);

        List<String> inputFolderListForColo1 = InstanceUtil
            .getInputFoldersForInstanceForReplication(cluster1, replicationCoordIDTarget.get(0),
                1);
        List<String> inputFolderListForColo2 = InstanceUtil
            .getInputFoldersForInstanceForReplication(cluster1, replicationCoordIDTarget.get(1),
                1);

        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(cluster3FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo2);

        //sleep till late starts
        TimeUtil.sleepTill(TimeUtil.addMinsToTime(startTime, 4));

        //check for run id to  be 1
        Assert.assertEquals(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(0), 0),
            1, "id has to be equal 1");
        Assert.assertEquals(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(1), 0),
            1, "id has to be equal 1");

        //wait for lates run to complete
        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(cluster1, replicationCoordIDTarget.get(0),
                0)
                == WorkflowJob.Status.SUCCEEDED
                && InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0)
                == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            logger.info("still in for loop");
            TimeUtil.sleepSeconds(20);
        }
        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(0), 0),
            WorkflowJob.Status.SUCCEEDED);
        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0),
            WorkflowJob.Status.SUCCEEDED);

        TimeUtil.sleepSeconds(30);

        //put data for the second time
        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.OOZIE_EXAMPLE_INPUT_DATA
            + "2ndLateData", inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(cluster3FS, OSUtil.OOZIE_EXAMPLE_INPUT_DATA
            + "2ndLateData", inputFolderListForColo2);

        //sleep till late 2 starts
        TimeUtil.sleepTill(TimeUtil.addMinsToTime(startTime, 9));

        //check for run id to be 2
        Assert.assertEquals(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(0), 0),
            2, "id has to be equal 2");
        Assert.assertEquals(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(1), 0),
            2, "id has to be equal 2");
    }

    /**
     * this test case does the following
     * two source ua2 and ua3
     * ua3 has following part data
     * ua1/ua2
     * ua1/ua2
     * ua1/ua2
     * <p/>
     * ua2 has following part data
     * ua1/ua3
     * ua1/ua3
     * ua1/ua3
     * <p/>
     * ua1 is the target, which in the end should have all ua1 data
     * <p/>
     * after first instance succeed data in put into relevant source and late should rerun
     * <p/>
     * after first late succeed data is put into other source and late should not
     */

    @Test(groups = {"multiCluster"})
    public void mixedTest01() throws Exception {

        bundles[0].setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);


        String startTime = TimeUtil.getTimeWrtSystemTime(3);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "ua1/${cluster.colo}");

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
            null);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
            "ua1/${cluster.colo}");

        //create data in colos

        String postFix = "/ua1/ua2";
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.lateDataReplenishWithoutSuccess(cluster2FS, 90, 1, prefix, postFix);

        postFix = "/ua2/ua2";
        HadoopUtil.lateDataReplenishWithoutSuccess(cluster2FS, 90, 1, prefix, postFix);

        postFix = "/ua3/ua2";
        HadoopUtil.lateDataReplenishWithoutSuccess(cluster2FS, 90, 1, prefix, postFix);

        //put _SUCCESS in parent folder UA2
        HadoopUtil.putFileInFolderHDFS(cluster2FS, 90, 1, prefix, "_SUCCESS");

        postFix = "/ua1/ua3";
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        postFix = "/ua2/ua3";
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        postFix = "/ua3/ua3";
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        //put _SUCCESS in parent folder of UA3
        HadoopUtil.putFileInFolderHDFS(cluster3FS, 90, 1, prefix, "_SUCCESS");

        //submit and schedule feed
        logger.info("feed: " + Util.prettyPrintXml(feed));

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        TimeUtil.sleepSeconds(10);

        //wait till 1st instance of replication coord is SUCCEEDED
        String bundleId =
            InstanceUtil.getLatestBundleID(cluster1, Util.readEntityName(feed), EntityType.FEED);

        List<String> replicationCoordIDTarget =
            InstanceUtil.getReplicationCoordID(bundleId, cluster1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(cluster1, replicationCoordIDTarget.get(0),
                0)
                == WorkflowJob.Status.SUCCEEDED
                && InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0)
                == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            logger.info("still in for loop");
            TimeUtil.sleepSeconds(20);
        }

        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(0), 0), WorkflowJob.Status.SUCCEEDED,
            "Replication job should have succeeded.");
        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0), WorkflowJob.Status.SUCCEEDED,
            "Replication job should have succeeded.");

        TimeUtil.sleepSeconds(15);

        //check for exact folders to be created in ua1 :  ua1/ua2 and ua1/ua3 no other should
        // be present. both of them should have _success


        List<String> inputFolderListForColo1 = InstanceUtil
            .getInputFoldersForInstanceForReplication(cluster1, replicationCoordIDTarget.get(0),
                1);
        List<String> inputFolderListForColo2 = InstanceUtil
            .getInputFoldersForInstanceForReplication(cluster1, replicationCoordIDTarget.get(1),
                1);

        String outPutLocation = InstanceUtil
            .getOutputFolderForInstanceForReplication(cluster1, replicationCoordIDTarget.get(0),
                0);
        String outPutBaseLocation = InstanceUtil
            .getOutputFolderBaseForInstanceForReplication(cluster1,
                replicationCoordIDTarget.get(0), 0);

        List<String> subfolders = HadoopUtil.getHDFSSubFoldersName(cluster1FS, outPutBaseLocation);

        Assert.assertEquals(subfolders.size(), 1);
        Assert.assertEquals(subfolders.get(0), "ua1");

        Assert.assertFalse(HadoopUtil.isFilePresentHDFS(cluster1FS, outPutBaseLocation,
            "_SUCCESS"));

        Assert.assertTrue(HadoopUtil.isFilePresentHDFS(cluster1FS, outPutLocation, "_SUCCESS"));

        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(cluster3FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo2);

        //sleep till late starts
        TimeUtil.sleepTill(TimeUtil.addMinsToTime(startTime, 4));

        //check for run id to  be 1
        Assert.assertTrue(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(0), 0) ==
                1
                && InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(1),
                0) == 1,
            "id have to be equal 1");


        //wait for lates run to complete
        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(cluster1, replicationCoordIDTarget.get(0),
                0)
                == WorkflowJob.Status.SUCCEEDED
                && InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0)
                == WorkflowJob.Status.SUCCEEDED) {
                break;
            }
            logger.info("still in for loop");
            TimeUtil.sleepSeconds(20);
        }


        TimeUtil.sleepSeconds(30);

        //put data for the second time
        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.OOZIE_EXAMPLE_INPUT_DATA
            + "2ndLateData", inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(cluster3FS, OSUtil.OOZIE_EXAMPLE_INPUT_DATA
            + "2ndLateData", inputFolderListForColo2);

        //sleep till late 2 starts
        TimeUtil.sleepTill(TimeUtil.addMinsToTime(startTime, 9));

        //check for run id to be 2
        Assert.assertTrue(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(0), 0) ==
                2
                && InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(1),
                0) == 2,
            "id have to be equal 2");
    }

    /**
     * only difference between mixed 01 and 02 is of availability flag. feed has _success as
     * availability flag ...so replication should not start till _success is put in ua2
     * <p/>
     * this test case does the following
     * two source ua2 and ua3
     * ua3 has follwing part data
     * ua1/ua2
     * ua1/ua2
     * ua1/ua2
     * <p/>
     * ua2 has following part data
     * ua1/ua3
     * ua1/ua3
     * ua1/ua3
     * <p/>
     * ua1 is the target, which in the end should have all ua1 data
     * after first instance succeed data in put into relevant source and late should rerun
     * after first late succeed data is put into other source and late should not rerun
     */
    @Test(groups = {"multiCluster"})
    public void mixedTest02() throws Exception {
        bundles[0].setInputFeedDataPath(inputPath);

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        //set availability flag as _success
        bundles[0].setInputFeedAvailabilityFlag("_SUCCESS");

        //get feed
        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        String startTime = TimeUtil.getTimeWrtSystemTime(3);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "ua1/${cluster.colo}");

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
            null);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
            "ua1/${cluster.colo}");

        //create data in colos

        String postFix = "/ua1/ua2";
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.lateDataReplenishWithoutSuccess(cluster2FS, 90, 1, prefix, postFix);

        postFix = "/ua2/ua2";
        HadoopUtil.lateDataReplenishWithoutSuccess(cluster2FS, 90, 1, prefix, postFix);

        postFix = "/ua3/ua2";
        HadoopUtil.lateDataReplenishWithoutSuccess(cluster2FS, 90, 1, prefix, postFix);

        //put _SUCCESS in parent folder UA2
        HadoopUtil.putFileInFolderHDFS(cluster2FS, 90, 1, prefix, "_SUCCESS");

        postFix = "/ua1/ua3";
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        postFix = "/ua2/ua3";
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        postFix = "/ua3/ua3";
        HadoopUtil.lateDataReplenish(cluster3FS, 90, 1, prefix, postFix);

        //put _SUCCESS in parent folder of UA3
        HadoopUtil.putFileInFolderHDFS(cluster3FS, 90, 1, prefix, "_SUCCESS");

        TimeUtil.sleepSeconds(15);

        //submit and schedule feed
        logger.info("feed: " + Util.prettyPrintXml(feed));

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
        TimeUtil.sleepSeconds(10);

        //wait till 1st instance of replication coord is SUCCEEDED
        String bundleId =
            InstanceUtil.getLatestBundleID(cluster1, Util.readEntityName(feed), EntityType.FEED);

        List<String> replicationCoordIDTarget =
            InstanceUtil.getReplicationCoordID(bundleId, cluster1.getFeedHelper());

        for (int i = 0; i < 30; i++) {
            if (InstanceUtil.getInstanceStatusFromCoord(cluster1, replicationCoordIDTarget.get(0),
                0)
                == WorkflowJob.Status.SUCCEEDED
                && InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0)
                == WorkflowJob.Status.SUCCEEDED) {
                break;
            }

            logger.info("still in for loop");
            TimeUtil.sleepSeconds(20);
        }

        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(0), 0), WorkflowJob.Status.SUCCEEDED,
            "Replication job did not succeed");
        Assert.assertEquals(InstanceUtil.getInstanceStatusFromCoord(cluster1,
                replicationCoordIDTarget.get(1), 0), WorkflowJob.Status.SUCCEEDED,
            "Replication job did not succeed");

        TimeUtil.sleepSeconds(15);

        /* check for exact folders to be created in ua1 :  ua1/ua2 and ua1/ua3 no other should
           be present. both of
           them should have _success */
        List<String> inputFolderListForColo1 =
            InstanceUtil.getInputFoldersForInstanceForReplication(cluster1,
                replicationCoordIDTarget.get(0), 1);
        List<String> inputFolderListForColo2 =
            InstanceUtil.getInputFoldersForInstanceForReplication(cluster1,
                replicationCoordIDTarget.get(1), 1);

        String outPutLocation = InstanceUtil
            .getOutputFolderForInstanceForReplication(cluster1, replicationCoordIDTarget.get(0),
                0);
        String outPutBaseLocation = InstanceUtil
            .getOutputFolderBaseForInstanceForReplication(cluster1,
                replicationCoordIDTarget.get(0), 0);

        List<String> subfolders = HadoopUtil.getHDFSSubFoldersName(cluster1FS, outPutBaseLocation);

        Assert.assertEquals(subfolders.size(), 1);
        Assert.assertEquals(subfolders.get(0), "ua1");

        Assert.assertFalse(HadoopUtil.isFilePresentHDFS(cluster1FS, outPutBaseLocation,
            "_SUCCESS"));

        Assert.assertTrue(HadoopUtil.isFilePresentHDFS(cluster1FS, outPutLocation, "_SUCCESS"));

        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo1);
        HadoopUtil.flattenAndPutDataInFolder(cluster3FS, OSUtil.NORMAL_INPUT,
            inputFolderListForColo2);

        //sleep till late starts
        TimeUtil.sleepTill(TimeUtil.addMinsToTime(startTime, 4));

        //check for run id to  be 1
        Assert.assertTrue(
            InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(0), 0) ==
                1
                && InstanceUtil.getInstanceRunIdFromCoord(cluster1, replicationCoordIDTarget.get(1),
                0) == 1,
            "id have to be equal 1");
    }
}

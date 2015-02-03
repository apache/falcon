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
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Update replication feed tests.
 */
@Test(groups = "embedded")
public class PrismFeedReplicationUpdateTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private FileSystem cluster1FS = serverFS.get(0);
    private FileSystem cluster2FS = serverFS.get(1);
    private FileSystem cluster3FS = serverFS.get(2);
    private String cluster1Colo = cluster1.getClusterHelper().getColoName();
    private String cluster2Colo = cluster2.getClusterHelper().getColoName();
    private String cluster3Colo = cluster3.getClusterHelper().getColoName();
    private final String baseTestDir = cleanAndGetTestDir();
    private final String inputPath = baseTestDir + "/input-data" + MINUTE_DATE_PATTERN;
    private String alternativeInputPath = baseTestDir + "/newFeedPath/input-data"
        + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(PrismFeedReplicationUpdateTest.class);

    @BeforeClass(alwaysRun = true)
    public void prepareCluster() throws IOException {
        // upload workflow to hdfs
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();

        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Set feed cluster1 as target, clusters 2 and 3 as source. Run feed. Update feed and check
     * if action succeed. Check that appropriate number of replication and retention coordinators
     * exist on matching clusters.
     *
     * @throws Exception
     */
    @Test(enabled = true, timeOut = 1200000)
    public void multipleSourceOneTarget() throws Exception {

        bundles[0].setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.clearFeedClusters();

        // use the colo string here so that the test works in embedded and distributed mode.
        String postFix = "/US/" + cluster2Colo;
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.lateDataReplenish(cluster2FS, 5, 80, prefix, postFix);

        // use the colo string here so that the test works in embedded and distributed mode.
        postFix = "/UK/" + cluster3Colo;
        prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 5, 80, prefix, postFix);

        String startTime = TimeUtil.getTimeWrtSystemTime(-30);

        feed
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 105))
                .withClusterType(ClusterType.TARGET)
                .build());

        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 130))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build());

        LOGGER.info("feed: " + Util.prettyPrintXml(feed.toString()));

        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed.toString()));

        //change feed location path
        feed.setFilePath(alternativeInputPath);

        LOGGER.info("updated feed: " + Util.prettyPrintXml(feed.toString()));

        //update feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed.toString(), feed.toString()));

        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(),
            Util.readEntityName(feed.toString()),
            "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(),
            Util.readEntityName(feed.toString()),
            "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster3.getFeedHelper(),
            Util.readEntityName(feed.toString()),
            "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster3.getFeedHelper(),
            Util.readEntityName(feed.toString()),
            "RETENTION"), 2);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feed.toString()),
                "REPLICATION"), 4);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feed.toString()),
                "RETENTION"), 2);
    }

    /**
     * Set feed1 to have cluster1 as source, cluster3 as target. Set feed2 clusters vise versa.
     * Add both clusters to process and feed2 as input feed. Run process. Update feed1.
     * TODO test case is incomplete
     *
     * @throws Exception
     */
    @Test(enabled = true, timeOut = 1800000)
    public void updateFeedDependentProcessTest() throws Exception {
        //set cluster colos
        bundles[0].setCLusterColo(cluster1Colo);
        bundles[1].setCLusterColo(cluster2Colo);
        bundles[2].setCLusterColo(cluster3Colo);

        //submit 3 clusters
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        //get 2 unique feeds
        FeedMerlin feed01 = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        FeedMerlin feed02 = new FeedMerlin(bundles[1].getInputFeedFromBundle());
        FeedMerlin outputFeed = new FeedMerlin(bundles[0].getOutputFeedFromBundle());

        //set clusters to null;
        feed01.clearFeedClusters();
        feed02.clearFeedClusters();
        outputFeed.clearFeedClusters();

        //set new feed input data
        feed01.setFeedPathValue(baseTestDir + "/feed01" + MINUTE_DATE_PATTERN);
        feed02.setFeedPathValue(baseTestDir + "/feed02" + MINUTE_DATE_PATTERN);

        //generate data in both the colos ua1 and ua3
        String prefix = feed01.getFeedPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        HadoopUtil.lateDataReplenish(cluster1FS, 25, 1, prefix, null);

        prefix = feed02.getFeedPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 25, 1, prefix, null);

        String startTime = TimeUtil.getTimeWrtSystemTime(-50);

        //set clusters for feed01
        feed01.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());

        feed01.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build());

        //set clusters for feed02
        feed02.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build());

        feed02.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());

        //set clusters for output feed
        outputFeed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());

        outputFeed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build());

        //submit and schedule feeds
        prism.getFeedHelper().submitAndSchedule(feed01.toString());
        prism.getFeedHelper().submitAndSchedule(feed02.toString());
        prism.getFeedHelper().submitAndSchedule(outputFeed.toString());

        //create a process with 2 clusters

        //get a process
        ProcessMerlin process = new ProcessMerlin(bundles[0].getProcessData());

        //add clusters to process
        String processStartTime = TimeUtil.getTimeWrtSystemTime(-6);
        String processEndTime = TimeUtil.getTimeWrtSystemTime(70);

        process.clearProcessCluster();

        process.addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withValidity(processStartTime, processEndTime)
                .build());

        process.addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[2].getClusters().get(0)))
                .withValidity(processStartTime, processEndTime)
                .build()
        );
        process.addInputFeed(feed02.getName(), feed02.getName());

        //submit and schedule process
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        LOGGER.info("Wait till process goes into running ");

        int timeout = OSUtil.IS_WINDOWS ? 50 : 25;
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), process.getName(), 1,
            Status.RUNNING, EntityType.PROCESS, timeout);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(2), process.getName(), 1,
            Status.RUNNING, EntityType.PROCESS, timeout);

        feed01.setFilePath(alternativeInputPath);
        LOGGER.info("updated feed: " + Util.prettyPrintXml(feed01.toString()));
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed01.toString(), feed01.toString()));
    }
}

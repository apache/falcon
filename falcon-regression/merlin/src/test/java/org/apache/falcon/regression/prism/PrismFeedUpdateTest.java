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
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Update feed via prism tests.
 */
@Test(groups = "embedded")
public class PrismFeedUpdateTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private FileSystem server1FS = serverFS.get(0);
    private OozieClient cluster1OC = serverOC.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String workflowForNoIpOp = baseTestDir + "/noinop";
    private final String cluster1colo = cluster1.getClusterHelper().getColoName();
    private final String cluster2colo = cluster2.getClusterHelper().getColoName();
    private static final Logger LOGGER = Logger.getLogger(PrismFeedUpdateTest.class);
    private String feedInputTimedOutPath = baseTestDir + "/timedout" + MINUTE_DATE_PATTERN;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        uploadDirToClusters(workflowForNoIpOp, OSUtil.RESOURCES + "workflows/aggregatorNoOutput/");
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
            bundles[i].setInputFeedDataPath(feedInputTimedOutPath);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Set 2 processes with common output feed. Second one is zero-input process. Update feed
     * queue. TODO : complete test case
     */
    @Test(enabled = true, timeOut = 1200000)
    public void updateFeedQueueDependentMultipleProcessOneProcessZeroInput() throws Exception {
        //cluster1colo and cluster2colo are source. feed01 on cluster1colo target cluster2colo,
        // feed02 on cluster2colo target cluster1colo
        bundles[0].setProcessWorkflow(workflowForNoIpOp);
        String cluster1Def = bundles[0].getClusters().get(0);
        String cluster2Def = bundles[1].getClusters().get(0);

        //set cluster colos
        bundles[0].setCLusterColo(cluster1colo);
        LOGGER.info("cluster bundles[0]: " + Util.prettyPrintXml(cluster1Def));
        bundles[1].setCLusterColo(cluster2colo);
        LOGGER.info("cluster bundles[1]: " + Util.prettyPrintXml(cluster2Def));

        //submit 2 clusters
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster1Def));
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster2Def));

        //get 2 unique feeds
        String feed01 = bundles[0].getInputFeedFromBundle();
        String outputFeed = bundles[0].getOutputFeedFromBundle();

        /* set source and target for the 2 feeds */
        //set clusters to null;
        feed01 = FeedMerlin.fromString(feed01).clearFeedClusters().toString();
        outputFeed = FeedMerlin.fromString(outputFeed).clearFeedClusters().toString();

        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01, baseTestDir + "/feed01" + MINUTE_DATE_PATTERN);

        //generate data in both the colos cluster1colo and cluster2colo
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        String startTime = TimeUtil.getTimeWrtSystemTime(-40);
        System.out.println("Start time = " + startTime);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
        HadoopUtil.lateDataReplenish(server1FS, 80, 20, prefix, null);

        //set clusters for feed01
        feed01 = FeedMerlin.fromString(feed01).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(cluster1Def))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build()).toString();
        feed01 = FeedMerlin.fromString(feed01).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(cluster2Def))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build()).toString();

        //set clusters for output feed
        outputFeed = FeedMerlin.fromString(outputFeed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(cluster1Def))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build()).toString();
        outputFeed = FeedMerlin.fromString(outputFeed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(cluster2Def))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build()).toString();

        //submit and schedule feeds
        LOGGER.info("feed01: " + Util.prettyPrintXml(feed01));
        LOGGER.info("outputFeed: " + Util.prettyPrintXml(outputFeed));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed01));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(outputFeed));

        /* create 2 process with 2 clusters */
        //get first process
        String process01 = bundles[0].getProcessData();

        //add clusters to process
        String processStartTime = TimeUtil.getTimeWrtSystemTime(-11);
        String processEndTime = TimeUtil.getTimeWrtSystemTime(70);
        process01 = ProcessMerlin.fromString(process01).clearProcessCluster().toString();
        process01 = ProcessMerlin.fromString(process01).addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(Util.readEntityName(cluster1Def))
                .withValidity(processStartTime, processEndTime)
                .build()
        ).toString();
        process01 = ProcessMerlin.fromString(process01).addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(Util.readEntityName(cluster2Def))
                .withValidity(processStartTime, processEndTime)
                .build()
        ).toString();

        //get 2nd process
        String process02 = process01;
        process02 = InstanceUtil
            .setProcessName(process02, this.getClass().getSimpleName()
                + "_zeroInputProcess" + new Random().nextInt());
        List<String> feed = new ArrayList<String>();
        feed.add(outputFeed);
        final ProcessMerlin processMerlin = new ProcessMerlin(process02);
        processMerlin.setProcessFeeds(feed, 0, 0, 1);
        process02 = processMerlin.toString();

        //submit and schedule both process
        LOGGER.info("process: " + Util.prettyPrintXml(process01));
        LOGGER.info("process: " + Util.prettyPrintXml(process02));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process01));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process02));
        LOGGER.info("Wait till process goes into running ");
        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.readEntityName(process01), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 1);
        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.readEntityName(process02), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 1);

        //change feed location path
        outputFeed = Util.setFeedProperty(outputFeed, "queueName", "myQueue");
        LOGGER.info("updated feed: " + Util.prettyPrintXml(outputFeed));

        //update feed first time
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(outputFeed, outputFeed));
    }

    /**
     * schedules a feed and dependent process. Process start and end are in past
     * Test for bug https://issues.apache.org/jira/browse/FALCON-500
     */
    @Test
    public void dependentProcessSucceeded()
        throws Exception {
        bundles[0].setProcessValidity("2014-06-01T04:00Z", "2014-06-01T04:02Z");
        bundles[0].submitAndScheduleAllFeeds();
        bundles[0].submitAndScheduleProcess();

        InstanceUtil.waitTillInstancesAreCreated(cluster1, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS,
            bundles[0].getProcessName(),
            0, 0);
        InstanceUtil.waitForBundleToReachState(cluster1, bundles[0].getProcessName(),
            Job.Status.SUCCEEDED, 20);

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.addProperty("someProp", "someVal");
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed.toString(), feed.toString()));
        //check for new feed bundle creation
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1OC, EntityType.FEED,
            feed.getName()), 2);
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1OC, EntityType.PROCESS,
            bundles[0].getProcessName()), 1);
    }

    /**
     * schedules a feed and dependent process. Update availability flag and check for process update
     * Test for bug https://issues.apache.org/jira/browse/FALCON-278
     */
    @Test
    public void updateAvailabilityFlag()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(30);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].submitAndScheduleAllFeeds();
        bundles[0].submitAndScheduleProcess();

        InstanceUtil.waitTillInstancesAreCreated(cluster1, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS,
            bundles[0].getProcessName(),
            0, 0);

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.setAvailabilityFlag("mytestflag");
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed.toString(), feed.toString()));
        //check for new feed bundle creation
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1OC, EntityType.FEED,
            feed.getName()), 2);
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1OC, EntityType.PROCESS,
            bundles[0].getProcessName()), 2);
    }
}

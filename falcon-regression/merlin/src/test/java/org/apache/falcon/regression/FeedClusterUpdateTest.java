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

package org.apache.falcon.regression;

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
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
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Feed cluster update tests.
 */
@Test(groups = "distributed")
public class FeedClusterUpdateTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private FileSystem cluster2FS = serverFS.get(1);
    private FileSystem cluster3FS = serverFS.get(2);
    private String feed;
    private String startTime;
    private String feedOriginalSubmit;
    private String feedUpdated;
    private static final Logger LOGGER = Logger.getLogger(FeedClusterUpdateTest.class);


    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
        try {
            String postFix = "/US/" + servers.get(1).getClusterHelper().getColoName();
            HadoopUtil.deleteDirIfExists(baseTestDir, cluster2FS);
            HadoopUtil.lateDataReplenish(cluster2FS, 80, 1, baseTestDir, postFix);
            postFix = "/UK/" + servers.get(2).getClusterHelper().getColoName();
            HadoopUtil.deleteDirIfExists(baseTestDir, cluster3FS);
            HadoopUtil.lateDataReplenish(cluster3FS, 80, 1, baseTestDir, postFix);
        } finally {
            removeTestClassEntities();
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
        BundleUtil.submitAllClusters(prism, bundles[0], bundles[1], bundles[2]);
        feed = bundles[0].getDataSets().get(0);
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();
        startTime = TimeUtil.getTimeWrtSystemTime(-50);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void addSourceCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();
        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit), "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 0);

        //prepare updated Feed
        feedUpdated = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        prism.getFeedHelper().submitAndSchedule(feedUpdated);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void addTargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();
        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            0);

        //prepare updated Feed
        feedUpdated = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Updated Feed: " + Util.prettyPrintXml(feedUpdated));

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        prism.getFeedHelper().submitAndSchedule(feedUpdated);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add2SourceCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            0);

        //prepare updated Feed
        feedUpdated = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Updated Feed: " + Util.prettyPrintXml(feedUpdated));

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        prism.getFeedHelper().submitAndSchedule(feedUpdated);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add2TargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source

        response = cluster2.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            0);

        //prepare updated Feed
        feedUpdated = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();

        LOGGER.info("Updated Feed: " + Util.prettyPrintXml(feedUpdated));

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        prism.getFeedHelper().submitAndSchedule(feedUpdated);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add1Source1TargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 0);

        //prepare updated Feed
        feedUpdated = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Updated Feed: " + Util.prettyPrintXml(feedUpdated));

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        prism.getFeedHelper().submitAndSchedule(feedUpdated);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void deleteSourceCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build())
            .toString();
        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source

        response = prism.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit), "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "RETENTION"), 1);

        //prepare updated Feed
        feedUpdated = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime,
                    TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .build())
            .toString();
        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        response = cluster3.getFeedHelper().getEntityDefinition(feedUpdated);
        AssertUtil.assertFailed(response);

        prism.getFeedHelper().submitAndSchedule(feedUpdated);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 0);
    }

    @Test(enabled = true, groups = {"multiCluster"})
    public void deleteTargetCluster() throws Exception {

        /*
        this test creates a multiCluster feed. Cluster1 is the target cluster
         and cluster3 and Cluster2 are the source cluster.

        feed is submitted through prism so submitted to both target and
        source. Feed is scheduled through prism, so only on Cluster3 and
        Cluster2 retention coord should exists. Cluster1 one which
         is target both retention and replication coord should exists. there
         will be 2 replication coord, one each for each source cluster.

        then we update feed by deleting cluster1 and cluster2 from the feed
        xml and send update request.

        Once update is over. definition should go missing from cluster1 and
        cluster2 and prism and cluster3 should have new def

        there should be a new retention coord on cluster3 and old number of
        coord on cluster1 and cluster2
         */

        //add two source and one target

        feedOriginalSubmit = FeedMerlin.fromString(feed).clearFeedClusters().toString();

        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build())
            .toString();
        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 20),
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feedOriginalSubmit = FeedMerlin.fromString(feedOriginalSubmit).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedOriginalSubmit));

        ServiceResponse response = prism.getFeedHelper().submitEntity(feedOriginalSubmit);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(response);

        //schedule on source
        response = prism.getFeedHelper().schedule(feedOriginalSubmit);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                Util.readEntityName(feedOriginalSubmit),
                "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                    Util.readEntityName(feedOriginalSubmit), "RETENTION"),
            1);

        //prepare updated Feed

        feedUpdated = FeedMerlin.fromString(feed).clearFeedClusters().toString();

        feedUpdated = FeedMerlin.fromString(feedUpdated).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(TimeUtil.addMinsToTime(startTime, 40),
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("Feed: " + Util.prettyPrintXml(feedUpdated));

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        TimeUtil.sleepSeconds(20);
        AssertUtil.assertSucceeded(response);


        //verify xmls definitions
        response = cluster1.getFeedHelper().getEntityDefinition(feedUpdated);
        AssertUtil.assertFailed(response);
        response = cluster2.getFeedHelper().getEntityDefinition(feedUpdated);
        AssertUtil.assertFailed(response);
        response = cluster3.getFeedHelper().getEntityDefinition(feedUpdated);
        Assert.assertTrue(XmlUtil.isIdentical(feedUpdated, response.getMessage()));
        response = prism.getFeedHelper().getEntityDefinition(feedUpdated);
        Assert.assertTrue(XmlUtil.isIdentical(feedUpdated, response.getMessage()));

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readEntityName(feedUpdated),
                "RETENTION"), 1);
    }

    /*
    @Test(enabled = false)
    public void delete2SourceCluster() {

    }

    @Test(enabled = false)
    public void delete2TargetCluster() {

    }

    @Test(enabled = false)
    public void delete1Source1TargetCluster() {

    }
    */
}

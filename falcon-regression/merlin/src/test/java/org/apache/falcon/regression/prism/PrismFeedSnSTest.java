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
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

public class PrismFeedSnSTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    private boolean restartRequired;
    String aggregateWorkflowDir = baseHDFSDir + "/PrismFeedSnSTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedSnSTest.class);
    String feed1, feed2;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
        feed1 = bundles[0].getDataSets().get(0);
        feed2 = bundles[1].getDataSets().get(0);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(cluster1.getFeedHelper());
        }
        removeBundles();
    }

    /**
     *  Submit and schedule feed1 on cluster1 and check that only this feed is running there.
     *  Perform the same for feed2 on cluster2.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testFeedSnSOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule feed1 on cluster1 and feed2 on cluster2. Check that they are running
     *  on matching clusters only. Submit and schedule them once more. Check that new bundles
     *  were not created and feed still running on matching clusters.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testSnSAlreadyScheduledFeedOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        //ensure only one bundle is there
        Assert.assertEquals(OozieUtil.getBundles(cluster1OC,
            Util.readEntityName(feed1), EntityType.FEED).size(), 1);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));
        //ensure only one bundle is there
        Assert.assertEquals(OozieUtil.getBundles(cluster2OC,
            Util.readEntityName(feed2), EntityType.FEED).size(), 1);
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    /**
     * Submit and schedule feed1 on cluster1, feed2 on cluster2. Suspend feed1 and check their
     * statuses. Submit and schedule feed1 again. Check that statuses hasn't been changed and new
     * bundle hasn't been created. Resume feed1. Repeat the same for feed2.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSnSSuspendedFeedOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        Assert.assertEquals(OozieUtil.getBundles(cluster1OC,
            Util.readEntityName(feed1), EntityType.FEED).size(), 1);

        AssertUtil.assertSucceeded(cluster1.getFeedHelper().resume(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(feed2));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.SUSPENDED);
        Assert.assertEquals(OozieUtil.getBundles(cluster2OC,
            Util.readEntityName(feed2), EntityType.FEED).size(), 1);
        AssertUtil.assertSucceeded(cluster2.getFeedHelper().resume(feed2));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule both feeds. Delete them and submit and schedule again. Check that
     *  action succeeded.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testSnSDeletedFeedOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed2));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);

        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));
    }

    /**
     *  Attempt to submit and schedule non-registered feed should fail.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testScheduleNonExistentFeedOnBothColos() throws Exception {
        AssertUtil.assertFailed(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.assertFailed(prism.getFeedHelper().submitAndSchedule(feed2));
    }

    /**
     *  Shut down server on cluster1. Submit and schedule feed on cluster2. Check that only
     *  mentioned feed is running there.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testFeedSnSOn1ColoWhileOtherColoIsDown() throws Exception {
        restartRequired = true;
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(
            bundles[1].getClusters().get(0)));

        Util.shutDownService(cluster1.getFeedHelper());
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     *  Attempt to submit and schedule feed on cluster which is down should fail and this feed
     *  shouldn't run on another cluster.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testFeedSnSOn1ColoWhileThatColoIsDown() throws Exception {
        restartRequired = true;
        bundles[0].submitFeed();
        Util.shutDownService(cluster1.getFeedHelper());
        AssertUtil.assertFailed(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule and then suspend feed1 on cluster1. Submit and schedule feed2 on
     *  cluster2 and check that this actions don't affect each other.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeed() throws Exception {
        bundles[0].submitAndScheduleFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule and then delete feed1 on cluster1. Submit and schedule feed2 on
     *  cluster2 and check that this actions don't affect each other.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeed() throws Exception {
        bundles[0].submitAndScheduleFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    /**
     * Submit and schedule feed1 on cluster1 and check that it failed. Repeat for feed2.
     *  TODO: should be reviewed
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testFeedSnSOnBothColosUsingColoHelper() throws Exception {
        //schedule both bundles
        bundles[0].submitFeed();
        ServiceResponse result = (cluster1.getFeedHelper().submitEntity(feed1));
        Assert.assertEquals(result.getCode(), 404);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        bundles[1].submitFeed();
        result = cluster2.getFeedHelper().submitAndSchedule(feed2);
        Assert.assertEquals(result.getCode(), 404);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule both feeds. Suspend feed1 and submit and schedule it once more. Check
     *  that status of feed1 is still suspended. Resume it. Suspend feed2 but submit and schedule
     *  feed1 again. Check that it didn't affect feed2 and it is still suspended.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSnSSuspendedFeedOnBothColosUsingColoHelper() throws Exception {
        //schedule both bundles
        bundles[0].submitFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        bundles[1].submitFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));

        AssertUtil.assertSucceeded(cluster1.getFeedHelper().suspend(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.assertSucceeded(cluster1.getFeedHelper().resume(feed1));

        AssertUtil.assertSucceeded(cluster2.getFeedHelper().suspend(feed2));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule both feeds and then delete them. Submit and schedule feeds again.
     *  Check that action succeeded and feeds are running.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testScheduleDeletedFeedOnBothColosUsingColoHelper() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed2));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed1));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));

        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper().getStatus(
            feed1)).getMessage(), cluster1.getClusterHelper().getColoName() + "/RUNNING");
        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper().getStatus(
            feed2)).getMessage(), cluster2.getClusterHelper().getColoName() + "/RUNNING");
    }

    /**
     *  Attempt to submit and schedule non-registered feeds should fail.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSNSNonExistentFeedOnBothColosUsingColoHelper() throws Exception {
        Assert.assertEquals(cluster1.getFeedHelper().submitAndSchedule(feed1).getCode(), 404);
        Assert.assertEquals(cluster2.getFeedHelper().submitAndSchedule(feed2).getCode(), 404);
    }

    /**
     *  Shut down server on cluster1. Submit and schedule feed on cluster2. Check that only that
     *  feed is running on cluster2.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testFeedSnSOn1ColoWhileOtherColoIsDownUsingColoHelper() throws Exception {
        restartRequired = true;
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(
            bundles[1].getClusters().get(0)));

        Util.shutDownService(cluster1.getFeedHelper());
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed2));

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     *  Set both clusters as feed clusters. Shut down one of them. Submit and schedule feed.
     *  Check that action is partially successful.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testFeedSnSOn1ColoWhileThatColoIsDownUsingColoHelper() throws Exception {
        restartRequired = true;
        String clust1 = bundles[0].getClusters().get(0);
        String clust2 = bundles[1].getClusters().get(0);

        bundles[0].setCLusterColo(cluster1.getClusterHelper().getColoName());
        logger.info("cluster bundles[0]: " + Util.prettyPrintXml(clust1));
        ServiceResponse r = prism.getClusterHelper().submitEntity(clust1);
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundles[1].setCLusterColo(cluster2.getClusterHelper().getColoName());
        logger.info("cluster bundles[1]: " + Util.prettyPrintXml(clust2));
        r = prism.getClusterHelper().submitEntity(clust2);
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeUA1 = "2012-10-01T12:00Z";
        String startTimeUA2 = "2012-10-01T12:00Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRetention("days(10000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA1, "2099-10-01T12:10Z"),
                XmlUtil.createRetention("days(10000)", ActionType.DELETE),
                Util.readEntityName(clust1), ClusterType.SOURCE, "${cluster.colo}",
                baseHDFSDir + "/localDC/rc/billing" + MINUTE_DATE_PATTERN);
        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeUA2, "2099-10-01T12:25Z"),
                XmlUtil.createRetention("days(10000)", ActionType.DELETE),
                Util.readEntityName(clust2), ClusterType.TARGET, null, baseHDFSDir +
                    "/clusterPath/localDC/rc/billing" + MINUTE_DATE_PATTERN);
        logger.info("feed: " + Util.prettyPrintXml(feed));

        Util.shutDownService(cluster1.getFeedHelper());

        r = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertPartial(r);
        r = prism.getFeedHelper().schedule(feed);
        AssertUtil.assertPartial(r);
        Util.startService(cluster1.getFeedHelper());
        prism.getClusterHelper().delete(clust1);
        prism.getClusterHelper().delete(clust2);
    }

    /**
     *   Submit and schedule feed1 and suspend it. Submit and schedule feed2 on another cluster
     *   and check that only feed2 is running on cluster2.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasSuspendedFeedUsingColoHelper()
        throws Exception {
        bundles[0].submitAndScheduleFeed();
        AssertUtil.assertSucceeded(cluster1.getFeedHelper().suspend(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);

        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    /**
     *  Submit and schedule and delete feed1. Submit and schedule feed2 and check that this
     *  action didn't affect feed1 and it is still killed.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testFeedSnSOn1ColoWhileAnotherColoHasKilledFeedUsingColoHelper() throws Exception {
        bundles[0].submitAndScheduleFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed1));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws IOException {
        cleanTestDirs();
    }
}

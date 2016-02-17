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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Feed submit and schedule tests.
 */
@Test(groups = "embedded")
public class FeedSubmitAndScheduleTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        feed = bundles[0].getDataSets().get(0);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        //remove entities which belong to both default and different user
        removeTestClassEntities(null, MerlinConstants.DIFFERENT_USER_NAME);
    }

    @Test(groups = {"singleCluster"})
    public void snsNewFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
    }

    /**
     * Submits and schedules feed with a cluster it depends on.
     *
     * @throws JAXBException
     * @throws IOException
     * @throws URISyntaxException
     * @throws AuthenticationException
     */
    private void submitFirstClusterScheduleFirstFeed()
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(bundles[0].getClusters().get(0)));
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Submits and schedules a feed and then tries to do the same on it. Checks that status
     * hasn't changed and response is successful.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void snsExistingFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.RUNNING);

        //get created bundle id
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, Util.readEntityName(feed), EntityType.FEED);

        //try to submit and schedule the same process again
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.RUNNING);

        //check that new bundle wasn't created
        OozieUtil.verifyNewBundleCreation(clusterOC, bundleId, null, feed, false, false);
    }

    /**
     * Try to submit and schedule feed without submitting cluster it depends on.
     * Request should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void snsFeedWithoutCluster() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertFailed(response);
    }

    /**
     * Submits and schedules feed. Removes it. Submitted and schedules removed feed.
     * Checks response and status of feed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void snsDeletedFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Suspends feed, submit and schedules it. Checks that response is successful,
     * feed status hasn't changed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void snsSuspendedFeed() throws Exception {
        submitFirstClusterScheduleFirstFeed();
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
    }

    /**
     * Test for https://issues.apache.org/jira/browse/FALCON-1647.
     * Create cluster entity as user1. Submit and schedule feed entity feed1 in this cluster as user1.
     * Now try to submit and schedule a feed entity feed2 in this cluster as user2.
     */
    @Test
    public void snsDiffFeedDiffUserSameCluster()
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException, JAXBException {
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        FeedMerlin feedMerlin = FeedMerlin.fromString(feed);
        feedMerlin.setName(feedMerlin.getName() + "-2");
        feedMerlin.setACL(MerlinConstants.DIFFERENT_USER_NAME, MerlinConstants.DIFFERENT_USER_GROUP, "*");
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(
            feedMerlin.toString(), MerlinConstants.DIFFERENT_USER_NAME, null);
        AssertUtil.assertSucceeded(response);
    }
}

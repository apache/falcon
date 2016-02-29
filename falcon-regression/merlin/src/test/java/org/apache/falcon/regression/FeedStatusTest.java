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
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Feed status tests. Checks getStatus functionality.
 */
@Test(groups = { "distributed", "embedded", "sanity" })
public class FeedStatusTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String feed;
    private static final Logger LOGGER = Logger.getLogger(FeedStatusTest.class);


    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0].generateUniqueBundle(this);
        bundles[0] = new Bundle(bundles[0], cluster);

        //submit the cluster
        ServiceResponse response =
            prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);
        feed = bundles[0].getInputFeedFromBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedules feed. Queries a feed status and checks the response
     * correctness and a feed status correspondence.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getStatusForScheduledFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        LOGGER.info("Feed: " + Util.prettyPrintXml(feed));
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(feed);

        AssertUtil.assertSucceeded(response);

        String colo = prism.getFeedHelper().getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
    }

    /**
     * Schedules and suspends feed. Queries a feed status and checks the response
     * correctness and a feed status correspondence.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getStatusForSuspendedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);

        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().suspend(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(feed);

        AssertUtil.assertSucceeded(response);
        String colo = prism.getFeedHelper().getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/SUSPENDED"));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
    }

    /**
     * Submits feed. Queries a feed status and checks the response
     * correctness and a feed status correspondence.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getStatusForSubmittedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);

        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(feed);

        AssertUtil.assertSucceeded(response);
        String colo = prism.getFeedHelper().getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/SUBMITTED"));
        AssertUtil.checkNotStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
    }

    /**
     * Removes feed. Queries a feed status. Checks that the response correctness.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getStatusForDeletedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().delete(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().getStatus(feed);
        AssertUtil.assertFailed(response);

        Assert.assertTrue(
            response.getMessage().contains(Util.readEntityName(feed) + " (FEED) not found"));
        AssertUtil.checkNotStatus(clusterOC, EntityType.FEED, feed, Job.Status.KILLED);
    }

    /**
     * Queries a status of feed which wasn't submitted and checks the response.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getStatusForNonExistentFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().getStatus(feed);
        AssertUtil.assertFailed(response);
        Assert.assertTrue(
            response.getMessage().contains(Util.readEntityName(feed) + " (FEED) not found"));

    }
}

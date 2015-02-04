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
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Feed suspend tests.
 */
@Test(groups = "embedded")
public class FeedSuspendTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String feed;

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
     * Schedule feed, suspend it. Check that web response reflects success and feed status is
     * "suspended".
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void suspendScheduledFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().suspend(feed);
        AssertUtil.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
    }

    /**
     * Try to suspend running feed twice. Response should reflect success,
     * feed status should be suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void suspendAlreadySuspendedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().suspend(feed);
        AssertUtil.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
        response = prism.getFeedHelper().suspend(feed);

        AssertUtil.assertSucceeded(response);
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
    }

    /**
     * Remove feed. Attempt to suspend it should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void suspendDeletedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().delete(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().suspend(feed);
        AssertUtil.assertFailed(response);
    }

    /**
     * Attempt to suspend non existent feed should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void suspendNonExistentFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().suspend(feed);
        AssertUtil.assertFailed(response);
    }

    /**
     * Attempt to suspend non scheduled feed should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void suspendSubmittedFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().suspend(feed);
        AssertUtil.assertFailed(response);
    }
}

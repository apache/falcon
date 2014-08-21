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
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed resume tests.
 */
@Test(groups = "embedded")
public class FeedResumeTest extends BaseTestClass {

    private final IEntityManagerHelper feedHelper = prism.getFeedHelper();
    private String feed;
    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String aggregateWorkflowDir = baseHDFSDir + "/FeedResumeTest/aggregator";
    private static final Logger LOGGER = Logger.getLogger(FeedResumeTest.class);

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        LOGGER.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0].generateUniqueBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitClusters(prism);
        feed = bundles[0].getInputFeedFromBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Launches feed, suspends it and then resumes and checks if it got running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void resumeSuspendedFeed() throws Exception {
        AssertUtil
            .assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        AssertUtil.assertSucceeded(feedHelper.suspend(URLS.SUSPEND_URL, feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
        AssertUtil.assertSucceeded(feedHelper.resume(URLS.RESUME_URL, feed));
        ServiceResponse response = feedHelper.getStatus(URLS.STATUS_URL, feed);
        String colo = feedHelper.getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
    }


    /**
     * Tries to resume feed that wasn't submitted and scheduled. Attempt should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void resumeNonExistentFeed() throws Exception {
        AssertUtil.assertFailed(feedHelper.resume(URLS.RESUME_URL, feed));
    }

    /**
     * Tries to resume deleted feed. Attempt should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void resumeDeletedFeed() throws Exception {
        AssertUtil
            .assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        AssertUtil.assertSucceeded(feedHelper.delete(URLS.DELETE_URL, feed));
        AssertUtil.assertFailed(feedHelper.resume(URLS.RESUME_URL, feed));
    }

    /**
     * Tries to resume scheduled feed which wasn't suspended. Feed status shouldn't change.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void resumeScheduledFeed() throws Exception {
        AssertUtil
            .assertSucceeded(feedHelper.submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
        AssertUtil.assertSucceeded(feedHelper.resume(URLS.RESUME_URL, feed));
        ServiceResponse response = feedHelper.getStatus(URLS.STATUS_URL, feed);
        String colo = feedHelper.getColo();
        Assert.assertTrue(response.getMessage().contains(colo + "/RUNNING"));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
    }
}

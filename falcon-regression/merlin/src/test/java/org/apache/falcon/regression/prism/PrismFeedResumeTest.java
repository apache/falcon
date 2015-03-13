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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Resume feed via prism tests.
 */
@Test(groups = "distributed")
public class PrismFeedResumeTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private boolean restartRequired;
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readLateDataBundle();

        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(cluster1.getFeedHelper());
        }
        removeTestClassEntities();
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

        //suspend using prismHelper
        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(bundles[0].getDataSets().get(0)));
        //verify
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.SUSPENDED);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        //suspend using prismHelper
        AssertUtil.assertSucceeded(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        //verify
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        //try using the colohelper
        AssertUtil.assertSucceeded(cluster1.getFeedHelper()
            .suspend(bundles[0].getDataSets().get(0)));
        //verify
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.SUSPENDED);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        //suspend using prismHelper
        AssertUtil.assertSucceeded(
            cluster1.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        //verify
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);

        //suspend on the other one
        AssertUtil.assertSucceeded(cluster1.getFeedHelper()
            .suspend(bundles[0].getDataSets().get(0)));
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.SUSPENDED);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(
            cluster1.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

        //delete using coloHelpers
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(bundles[0].getDataSets().get(0)));

        //suspend using prismHelper
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(bundles[1].getDataSets().get(0)));
        //suspend on the other one
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
        AssertUtil.assertFailed(cluster1.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.assertFailed(cluster2.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(bundles[0].getDataSets().get(0)));
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.SUSPENDED);
        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            AssertUtil.assertSucceeded(prism.getFeedHelper()
                .resume(bundles[0].getDataSets().get(0)));
            //verify
            checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
            checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        }

        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(bundles[1].getDataSets().get(0)));
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            AssertUtil.assertSucceeded(cluster1.getFeedHelper()
                .resume(bundles[0].getDataSets().get(0)));
            //verify
            checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
            checkAndCompareStatus(cluster2, bundles[1], Job.Status.SUSPENDED);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            AssertUtil.assertSucceeded(prism.getFeedHelper()
                .resume(bundles[1].getDataSets().get(0)));
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            AssertUtil.assertSucceeded(cluster2.getFeedHelper()
                .resume(bundles[1].getDataSets().get(0)));
            checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
            checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        }
    }

    @Test
    public void testResumeNonExistentFeedOnBothColos() throws Exception {
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));

        AssertUtil.assertFailed(cluster1.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(cluster2.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
    }

    @Test
    public void testResumeSubmittedFeedOnBothColos() throws Exception {
        bundles[0].submitFeed();
        bundles[1].submitFeed();

        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));

        AssertUtil.assertFailed(cluster1.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(cluster2.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeScheduledFeedOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        //schedule using colohelpers
        bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);
        AssertUtil.assertSucceeded(cluster1.getFeedHelper()
            .suspend(bundles[0].getDataSets().get(0)));
        AssertUtil.assertSucceeded(cluster2.getFeedHelper()
            .suspend(bundles[1].getDataSets().get(0)));

        Util.shutDownService(cluster1.getFeedHelper());

        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        //verify
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.SUSPENDED);
        //resume on the other one
        AssertUtil.assertSucceeded(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);

        Util.startService(cluster1.getFeedHelper());
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.assertSucceeded(
            cluster2.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        checkAndCompareStatus(cluster1, bundles[0], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        //schedule using colohelpers
        bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

        //delete using prismHelper
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(bundles[0].getDataSets().get(0)));

        Util.shutDownService(cluster1.getFeedHelper());

        //suspend using prismHelper
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);

        //suspend using prismHelper
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        checkAndCompareStatus(cluster2, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(bundles[1].getDataSets().get(0)));
        //suspend on the other one
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);

        AssertUtil.assertFailed(cluster2.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeNonExistentFeedOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        Util.shutDownService(cluster1.getFeedHelper());

        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(cluster2.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSubmittedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        bundles[0].submitFeed();
        bundles[1].submitFeed();

        Util.shutDownService(cluster1.getFeedHelper());

        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
        AssertUtil.assertFailed(cluster2.getFeedHelper().resume(bundles[1].getDataSets().get(0)));
    }


    private void checkAndCompareStatus(ColoHelper coloHelper, Bundle bundle,
                                       Job.Status expectedStatus) throws Exception {
        AssertUtil
            .checkStatus(coloHelper.getFeedHelper().getOozieClient(), EntityType.FEED, bundle,
                expectedStatus);
        String entity = bundle.getDataSets().get(0);
        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper().getStatus(entity)).getMessage(),
            coloHelper.getFeedHelper().getColoName() + "/" + expectedStatus);
        Assert.assertEquals(Util.parseResponse(prism.getFeedHelper().getStatus(entity)).getMessage(),
            coloHelper.getFeedHelper().getColoName() + "/"
                + Util.parseResponse(coloHelper.getFeedHelper().getStatus(entity)).getMessage());
    }
}

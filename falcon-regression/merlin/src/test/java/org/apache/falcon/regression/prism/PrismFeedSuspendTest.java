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
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedSuspendTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismFeedSuspendTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedSuspendTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Run two feed on different clusters. Delete 1st feed and try to suspend it. Should fail.
     * Check that 2nd feed is running on 2nd cluster. Delete it and try to suspend it too.
     * Attempt should fail and both feeds should be killed.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testSuspendDeletedFeedOnBothColos() throws Exception {
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //delete using prism
        AssertUtil.assertSucceeded(prism.getFeedHelper()
            .delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));
        //suspend using prism
        AssertUtil.assertFailed(prism.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getFeedHelper()
            .delete(Util.URLS.DELETE_URL, bundles[1].getDataSets().get(0)));
        //suspend on the other one
        AssertUtil.assertFailed(prism.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
    }

    /**
     * Run two feeds on different clusters. Suspend feed and try to suspend it once more. Check
     * that action succeeds and feed is suspended. Make the same for 2nd feed.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);
        for (int i = 0; i < 2; i++) {
            //suspend using prism
            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        }
        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0))
            );
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.SUSPENDED);
        }
    }

    /**
     * Attempt to suspend nonexistent feed should fail through both prism and matching server.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void testSuspendNonExistentFeedOnBothColos() throws Exception {
        AssertUtil.assertFailed(prism.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));

        AssertUtil.assertFailed(cluster1.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(cluster2.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
    }

    /**
     * Attempt to suspend non-running feed should fail through both prism and matching server.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void testSuspendSubmittedFeedOnBothColos() throws Exception {
        bundles[0].submitFeed();
        bundles[1].submitFeed();

        AssertUtil.assertFailed(prism.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(prism.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));

        AssertUtil.assertFailed(cluster1.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.assertFailed(cluster2.getFeedHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
    }

    /**
     * Run two feeds on different clusters. Stop server on 1st cluster. Attempt to suspend feed on
     * stopped server through prism should fail. Check that 2nd feed is running. Suspend it
     * and check that it is suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendScheduledFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
            bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

            Util.shutDownService(cluster1.getFeedHelper());

            //suspend using prism
            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

            //suspend on the other one
            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getFeedHelper());
        }
    }

    /**
     * Run two feeds on different clusters. Delete 1st feed. Stop server on 1st cluster. Attempt
     * to suspend deleted feed on stopped server should fail. Delete 2nd feed. Attempt
     * to suspend deleted 2nd feed should also fail. Check that both feeds are killed.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendDeletedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
            bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

            //delete using coloHelpers
            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0))
            );
            Util.shutDownService(cluster1.getFeedHelper());

            //suspend using prism
            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
            //verify
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .delete(Util.URLS.DELETE_URL, bundles[1].getDataSets().get(0))
            );
            //suspend on the other one
            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0))
            );
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.KILLED);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getFeedHelper());
        }
    }

    /**
     * Run two feeds on different clusters. Suspend 1st feed and check that it suspended,
     * and then stop server on its cluster. Attempt to suspend the same feed again should fail.
     * Suspend 2nd feed and check that both feeds are suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendSuspendedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].submitAndScheduleFeedUsingColoHelper(cluster1);
            bundles[1].submitAndScheduleFeedUsingColoHelper(cluster2);

            //suspend using prism
            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);

            Util.shutDownService(cluster1.getFeedHelper());

            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0))
            );
            //suspend on the other one
            AssertUtil.assertSucceeded(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
            AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

    /**
     * Stop the 1st server. Attempt to suspend nonexistent feeds on both clusters should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendNonExistentFeedOnBothColosWhen1ColoIsDown()
        throws Exception {
        try {
            Util.shutDownService(cluster1.getFeedHelper());
            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0))
            );
            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
            AssertUtil.assertFailed(
                cluster2.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

    /**
     * Submit two feeds. Stop the server on the 1st cluster. Attempts to suspend non-scheduled
     * feeds on both clusters should fail through prism as well as through colohelper.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            bundles[0].submitFeed();
            bundles[1].submitFeed();

            Util.shutDownService(cluster1.getFeedHelper());

            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0))
            );
            AssertUtil.assertFailed(
                prism.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));

            AssertUtil.assertFailed(
                cluster2.getFeedHelper()
                    .suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

}

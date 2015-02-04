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
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Resume process via prism tests.
 */
@Test(groups = "distributed")
public class PrismProcessResumeTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readLateDataBundle();
        for (int i = 0; i < 2; i++) {
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
     * Schedule process. Suspend/resume it one by one. Check that process really suspended/resumed.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testResumeSuspendedFeedOnBothColos() throws Exception {
        //schedule using process on 2 clusters
        bundles[0].addClusterToBundle(bundles[1].getClusters().get(0), ClusterType.SOURCE, null, null);
        bundles[0].submitAndScheduleProcess();

        //suspend using prism
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));

        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);

        //resume using prism
        AssertUtil.assertSucceeded(prism.getProcessHelper().resume(bundles[0].getProcessData()));

        //verify
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //suspend using the colohelper
        AssertUtil.assertSucceeded(
            cluster2.getProcessHelper().suspend(bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //resume using colohelper
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().resume(bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //suspend on the other one
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().suspend(bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);

        //resume using colohelper
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().resume(bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Schedule processes, remove them. Try to resume them using colo-helpers and through prism.
     * Attempt to -resume process which was removed should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].addClusterToBundle(bundles[1].getClusters().get(0), ClusterType.SOURCE, null, null);
        bundles[0].submitAndScheduleProcess();

        //delete using prism
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));

        //try to resume it through prism
        AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[0].getProcessData()));
    }

    /**
     * Schedule processes. suspend them and then resume. Then try to resume them once
     * more.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].addClusterToBundle(bundles[1].getClusters().get(0), ClusterType.SOURCE, null, null);
        bundles[0].submitAndScheduleProcess();

        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);

        //resume suspended process using prism
        AssertUtil.assertSucceeded(prism.getProcessHelper().resume(bundles[0].getProcessData()));

        //verify
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //resume resumed process
        AssertUtil.assertSucceeded(prism.getProcessHelper().resume(bundles[0].getProcessData()));

        //verify
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Attempt to resume non-existent process should fail through both prism and colohelpers.
     *
     * @throws Exception
     */
    @Test
    public void testResumeNonExistentProcessOnBothColos() throws Exception {
        AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[0].getProcessData()));
        AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[1].getProcessData()));

        AssertUtil.assertFailed(cluster2.getProcessHelper().resume(bundles[0].getProcessData()));
        AssertUtil.assertFailed(cluster1.getProcessHelper().resume(bundles[0].getProcessData()));
    }

    /**
     * Attempt to resume process which wasn't submitted should fail.
     *
     * @throws Exception
     */
    @Test
    public void testResumeSubmittedProcessOnBothColos() throws Exception {
        bundles[0].submitProcess(true);
        bundles[1].submitProcess(true);

        AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[0].getProcessData()));
        AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[1].getProcessData()));

        AssertUtil.assertFailed(cluster2.getProcessHelper().resume(bundles[0].getProcessData()));
        AssertUtil.assertFailed(cluster1.getProcessHelper().resume(bundles[1].getProcessData()));


    }

    /**
     * Schedule processes on both servers and then suspend them. Shutdown server. Check that it's
     * impossible to resume process on this server and possible on another server.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testResumeScheduledProcessOnBothColosWhen1ColoIsDown()
        throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].addClusterToBundle(bundles[1].getClusters().get(0), ClusterType.SOURCE, null, null);
            bundles[0].submitAndScheduleProcess();

            AssertUtil.assertSucceeded(
                cluster2.getProcessHelper().suspend(bundles[0].getProcessData()));
            AssertUtil.assertSucceeded(
                cluster1.getProcessHelper().suspend(bundles[0].getProcessData()));

            Util.shutDownService(cluster2.getProcessHelper());

            AssertUtil.assertPartial(prism.getProcessHelper().resume(bundles[0].getProcessData()));

            //verify
            AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }

    }

    /**
     * Shutdown one of the server. Attempt to resume non-existent process on both servers should
     * fail.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testResumeNonExistentProcessOnBothColosWhen1ColoIsDown()
        throws Exception {
        try {
            Util.shutDownService(cluster2.getProcessHelper());

            AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[1].getProcessData()));
            AssertUtil.assertFailed(prism.getProcessHelper().resume(bundles[0].getProcessData()));
            AssertUtil.assertFailed(
                cluster1.getProcessHelper().resume(bundles[1].getProcessData()));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    /**
     * Submit processes on both servers. Shutdown one server. Attempt to resume non-scheduled
     * process ob both servers should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testResumeSubmittedProcessOnBothColosWhen1ColoIsDown()
        throws Exception {
        try {

            bundles[0].addClusterToBundle(bundles[1].getClusters().get(0), ClusterType.SOURCE, null, null);
            bundles[0].submitAndScheduleProcess();

            Util.shutDownService(cluster2.getProcessHelper());

            AssertUtil.assertPartial(prism.getProcessHelper().resume(bundles[0].getProcessData()));
            AssertUtil.assertSucceeded(cluster1.getProcessHelper().resume(bundles[0].getProcessData()));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }

    }
}

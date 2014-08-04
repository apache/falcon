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
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessSuspendTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    private boolean restartRequired;
    String aggregateWorkflowDir = baseHDFSDir + "/PrismProcessSuspendTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismProcessSuspendTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = BundleUtil.readLateDataBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }

    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(cluster1.getProcessHelper());
        }
        removeBundles();
    }


    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendSuspendedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster2);


        //suspend using prismHelper
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        Util.shutDownService(cluster1.getProcessHelper());

        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            AssertUtil.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
            AssertUtil
                .checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
            AssertUtil
                .checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        }
    }


    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendScheduledProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster2);

        //suspend using prismHelper
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        //suspend on the other one
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        Assert.assertTrue(Util.parseResponse(prism.getProcessHelper()
            .getStatus(URLS.STATUS_URL, bundles[0].getProcessData())).getMessage()
            .contains("SUSPENDED"));
        Assert.assertTrue(Util.parseResponse(prism.getProcessHelper()
            .getStatus(URLS.STATUS_URL, bundles[1].getProcessData())).getMessage()
            .contains("RUNNING"));
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster2);

        //delete using coloHelpers
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));


        //suspend using prismHelper
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundles[1].getProcessData()));
        //suspend on the other one
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendSuspendedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster2);


        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            AssertUtil.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
            //verify
            AssertUtil
                .checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            AssertUtil.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
            AssertUtil
                .checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
            AssertUtil
                .checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        }
    }

    @Test(groups = "embedded")
    public void testSuspendNonExistentProcessOnBothColos() throws Exception {
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));

        AssertUtil.assertFailed(cluster1.getProcessHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.assertFailed(cluster2.getProcessHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
    }

    @Test(groups = "embedded")
    public void testSuspendSubmittedProcessOnBothColos() throws Exception {
        bundles[0].submitProcess(true);
        bundles[1].submitProcess(true);

        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));

        AssertUtil.assertFailed(cluster1.getProcessHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.assertFailed(cluster2.getProcessHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendScheduledProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster2);

        Util.shutDownService(cluster1.getProcessHelper());

        //suspend using prismHelper
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        //suspend on the other one
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSuspendDeletedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;         //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster1);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster2);

        //delete using coloHelpers
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));

        Util.shutDownService(cluster1.getProcessHelper());

        //suspend using prismHelper
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundles[1].getProcessData()));
        //suspend on the other one
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.KILLED);
    }


    @Test(groups = "distributed")
    public void testSuspendNonExistentProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getProcessHelper());

        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));

        AssertUtil.assertFailed(cluster2.getProcessHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
    }

    @Test(groups = "distributed")
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;
        bundles[0].submitProcess(true);
        bundles[1].submitProcess(true);

        Util.shutDownService(cluster1.getProcessHelper());

        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.assertFailed(
            prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.assertFailed(cluster2.getProcessHelper()
            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
    }
}

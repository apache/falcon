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
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
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

public class PrismProcessSnSTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismProcessSnSTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismProcessSnSTest.class);
    String process1;
    String process2;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readLateDataBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
        process1 = bundles[0].getProcessData();
        process2 = bundles[1].getProcessData();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Submit and schedule process1 on cluster1. Check that process2 is not running on cluster1.
     * Submit and schedule process2 on cluster2. Check that process2 is running and process1 is
     * not running on cluster2.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessSnSOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        //check if there is no criss cross
        ServiceResponse response = prism.getProcessHelper().getStatus(URLS.STATUS_URL, process2);
        logger.info(response.getMessage());
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Submit process1 on cluster1 and schedule it. Check that process1 runs on cluster1 but not
     * on cluster2. Submit process2 but schedule process1 once more. Check that process1 is running
     * on cluster1 but not on cluster2.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessSnSForSubmittedProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitProcess(true);
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        bundles[1].submitProcess(true);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Submit process1 on cluster1 and schedule it. Check that only process1 runs on cluster1.
     * Submit process2 and check that it isn't running on cluster1. Submit and schedule process1
     * once more and check that it is still running on cluster1 but process2 isn't running on
     * cluster2.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessSnSForSubmittedProcessOnBothColosUsingColoHelper()
        throws Exception {
        bundles[0].submitProcess(true);
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitProcess(true);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitProcess(true);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Submit and schedule process1 on cluster1 and check that only it is running there. Submit
     * and schedule process2 on cluster2 and check the same for it. Schedule process1 on cluster2.
     * Check that it is running on cluster2 and cluster1 but process2 isn't running on cluster1.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testProcessSnSAlreadyScheduledOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        bundles[1].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //reschedule trial
        AssertUtil.assertSucceeded(cluster2.getProcessHelper()
            .schedule(URLS.SCHEDULE_URL, process1));
        Assert.assertEquals(OozieUtil.getBundles(cluster2.getFeedHelper().getOozieClient(),
            Util.readEntityName(process1), EntityType.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    /**
     * Submit and schedule both process1 and process2. Suspend process1. Check their statuses.
     * Submit and schedule process1 once more.
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSnSSuspendedProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        bundles[1].submitAndScheduleProcess();
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().suspend(URLS.SUSPEND_URL, process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1));
        Assert.assertEquals(OozieUtil.getBundles(cluster2OC, Util.readEntityName(process1),
            EntityType.PROCESS).size(), 1);
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().resume(URLS.SUSPEND_URL, process1));

        AssertUtil.assertSucceeded(cluster1.getProcessHelper().suspend(URLS.SUSPEND_URL, process2));
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process2));

        Assert.assertEquals(OozieUtil.getBundles(cluster1OC, Util.readEntityName(process2),
            EntityType.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
    }

    /**
     * Submit and schedule both processes on both cluster1 and cluster2. Check that they are
     * running. Delete both of them. Submit and schedule them once more. Check that they are
     * running again.
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testSnSDeletedProcessOnBothColos() throws Exception {
        //schedule both bundles
        final String cluster1Running = cluster1.getClusterHelper().getColoName() + "/RUNNING";
        final String cluster2Running = cluster2.getClusterHelper().getColoName() + "/RUNNING";

        bundles[0].submitAndScheduleProcess();
        Assert.assertEquals(Util.parseResponse(prism.getProcessHelper()
            .getStatus(URLS.STATUS_URL, process1)).getMessage(), cluster1Running);
        bundles[1].submitAndScheduleProcess();
        Assert.assertEquals(Util.parseResponse(prism.getProcessHelper()
            .getStatus(URLS.STATUS_URL, process2)).getMessage(), cluster2Running);

        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL, process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL, process2));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1));
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process2));

        Assert.assertEquals(Util.parseResponse(prism.getProcessHelper()
            .getStatus(URLS.STATUS_URL, process1)).getMessage(),cluster1Running);
        Assert.assertEquals(Util.parseResponse(prism.getProcessHelper()
            .getStatus(URLS.STATUS_URL, process2)).getMessage(), cluster2Running);
    }

    /**
     * Attempt to submit and schedule processes when all required entities weren't registered
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testScheduleNonExistentProcessOnBothColos() throws Exception {
        Assert.assertEquals(Util.parseResponse(cluster2.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process1)).getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(cluster1.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process2)).getStatusCode(), 404);
    }
}

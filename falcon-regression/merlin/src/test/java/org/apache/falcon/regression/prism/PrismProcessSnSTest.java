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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessSnSTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismProcessSnSTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismProcessSnSTest.class);

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
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessSnSOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        ServiceResponse response =
            prism.getProcessHelper()
                .getStatus(URLS.STATUS_URL, bundles[1].getProcessData());
        logger.info(response.getMessage());
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessSnSForSubmittedProcessOnBothColos() throws Exception {
        //schedule both bundles

        bundles[0].submitProcess(true);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        bundles[1].submitProcess(true);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessSnSForSubmittedProcessOnBothColosUsingColoHelper()
        throws Exception {
        //schedule both bundles

        bundles[0].submitProcess(true);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitProcess(true);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitProcess(true);

        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testProcessSnSAlreadyScheduledOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //reschedule trial

        AssertUtil.assertSucceeded(cluster2.getProcessHelper()
            .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
        Assert.assertEquals(OozieUtil.getBundles(cluster2.getFeedHelper().getOozieClient(),
            Util.readEntityName(bundles[0].getProcessData()), EntityType.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testSnSSuspendedProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        bundles[1].submitAndScheduleProcess();

        AssertUtil.assertSucceeded(cluster2.getProcessHelper()
            .suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        Assert.assertEquals(OozieUtil.getBundles(cluster2.getFeedHelper().getOozieClient(),
            Util.readEntityName(bundles[0].getProcessData()), EntityType.PROCESS).size(), 1);
        AssertUtil.assertSucceeded(cluster2.getProcessHelper()
            .resume(URLS.SUSPEND_URL, bundles[0].getProcessData()));

        AssertUtil.assertSucceeded(cluster1.getProcessHelper()
            .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()));

        Assert.assertEquals(OozieUtil.getBundles(cluster1.getFeedHelper().getOozieClient(),
            Util.readEntityName(bundles[1].getProcessData()), EntityType.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
    }

    @Test(groups = {"prism", "0.2", "embedded"})
    public void testSnSDeletedProcessOnBothColos() throws Exception {
        //schedule both bundles
        final String cluster1Running = cluster1.getClusterHelper().getColoName() + "/RUNNING";
        final String cluster2Running = cluster2.getClusterHelper().getColoName() + "/RUNNING";
        bundles[0].submitAndScheduleProcess();

        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                    .getStatus(URLS.STATUS_URL, bundles[0].getProcessData())).getMessage(),
            cluster1Running
        );

        bundles[1].submitAndScheduleProcess();
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                    .getStatus(URLS.STATUS_URL, bundles[1].getProcessData())).getMessage(),
            cluster2Running
        );

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()));

        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                    .getStatus(URLS.STATUS_URL, bundles[0].getProcessData())
            ).getMessage(),
            cluster1Running
        );
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                    .getStatus(URLS.STATUS_URL, bundles[1].getProcessData())
            ).getMessage(),
            cluster2Running
        );

    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testScheduleNonExistentProcessOnBothColos() throws Exception {
        Assert.assertEquals(Util.parseResponse(cluster2.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()))
            .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(cluster1.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()))
            .getStatusCode(), 404);

    }

}

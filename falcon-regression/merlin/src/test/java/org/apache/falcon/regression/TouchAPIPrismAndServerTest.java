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
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;

/**
 * Touch feature test both via server and prism.
 */
@Test(groups = "embedded")
public class TouchAPIPrismAndServerTest extends BaseTestClass {
    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(TouchAPIPrismAndServerTest.class);
    private String startTime;
    private String endTime;
    private String clusterName;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        startTime = TimeUtil.getTimeWrtSystemTime(0);
        endTime = TimeUtil.addMinsToTime(startTime, 20);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedule a process with end time greater than current time
     * Perform touch via both server and prism
     * Should succeed with creation of a new bundle from the current time.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void touchProcessSchedule() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        String coordId = OozieUtil.getLatestCoordinatorID(clusterOC,
            bundles[0].getProcessName(), EntityType.PROCESS);
        String oldbundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(),
            EntityType.PROCESS);

        // via prism
        ServiceResponse response = prism.getProcessHelper().touchEntity(bundles[0].getProcessData());
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        Assert.assertNotEquals(oldbundleId, bundleId, "Bundle ids are same. No new bundle generated.");
        validate(response, "Old coordinator id: " + coordId + ". New bundle id: " + bundleId);

        // via server
        oldbundleId = bundleId;
        coordId = OozieUtil.getLatestCoordinatorID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        response = cluster.getProcessHelper().touchEntity(bundles[0].getProcessData());
        bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        Assert.assertNotEquals(oldbundleId, bundleId, "Bundle ids are same. No new bundle generated.");
        validate(response, "Old coordinator id: " + coordId + ". New bundle id: " + bundleId);
    }

    /**
     * Schedule a feed with end time greater than current time
     * Perform touch via both server and prism
     * Should succeed with creation of a new bundle from the current time.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void touchFeedSchedule() throws Exception {
        bundles[0].submitAndScheduleFeed();
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, bundles[0], Job.Status.RUNNING);
        String coordId = OozieUtil.getLatestCoordinatorID(clusterOC, clusterName, EntityType.FEED);
        String oldbundleId = OozieUtil.getLatestBundleID(clusterOC, clusterName, EntityType.FEED);

        // via prism
        TimeUtil.sleepSeconds(60);
        ServiceResponse response = prism.getFeedHelper().touchEntity(bundles[0].getDataSets().get(0));
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, clusterName, EntityType.FEED);
        Assert.assertNotEquals(oldbundleId, bundleId, "Bundle ids are same. No new bundle generated.");
        validate(response, "Old coordinator id: " + coordId + ". New bundle id: " + bundleId);

        // via server
        oldbundleId = bundleId;
        coordId = OozieUtil.getLatestCoordinatorID(clusterOC, clusterName, EntityType.FEED);
        TimeUtil.sleepSeconds(60);
        response = cluster.getFeedHelper().touchEntity(bundles[0].getDataSets().get(0));
        bundleId = OozieUtil.getLatestBundleID(clusterOC, clusterName, EntityType.FEED);
        Assert.assertNotEquals(oldbundleId, bundleId, "Bundle ids are same. No new bundle generated.");
        validate(response, "Old coordinator id: " + coordId + ". New bundle id: " + bundleId);

    }

    /**
     * Schedule a process with end time less than current time.
     * Perform touch via both server and prism
     * No new bundle should be created.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void touchProcessScheduleWithEndTimeLessThanCurrentTime() throws Exception {
        startTime = TimeUtil.getTimeWrtSystemTime(-120);
        endTime = TimeUtil.addMinsToTime(startTime, 20);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        String coordId = OozieUtil.getLatestCoordinatorID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        String oldbundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);

        // via prism
        ServiceResponse response = prism.getProcessHelper().touchEntity(bundles[0].getProcessData());
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        Assert.assertEquals(oldbundleId, bundleId, "New bundle generated");
        validate(response, "Old coordinator id: " + coordId);

        //via server
        oldbundleId = bundleId;
        response = cluster.getProcessHelper().touchEntity(bundles[0].getProcessData());
        bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        Assert.assertEquals(oldbundleId, bundleId, "New bundle generated");
        validate(response, "Old coordinator id: " + coordId);
    }

    private void validate(ServiceResponse response, String message) throws JAXBException {
        AssertUtil.assertSucceeded(response);
        Assert.assertTrue(response.getMessage().contains(message),
                "Correct response was not present in process / feed schedule");
    }
}

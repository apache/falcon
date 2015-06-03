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

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Test updating of feed with custom update time.
 */
public class UpdateAtSpecificTimeTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(UpdateAtSpecificTimeTest.class);

    private Bundle processBundle;
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private OozieClient cluster3OC = serverOC.get(2);
    private FileSystem cluster2FS = serverFS.get(1);
    private final String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws IOException {
        Bundle bundle = BundleUtil.readFeedReplicationBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);
        bundles[2] = new Bundle(bundle, cluster3);

        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
        bundles[2].generateUniqueBundle(this);

        processBundle = BundleUtil.readELBundle();
        processBundle = new Bundle(processBundle, cluster1);
        processBundle.generateUniqueBundle(this);
        processBundle.setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(groups = {"singleCluster", "0.3.1", "embedded"}, timeOut = 1200000, enabled = true)
    public void updateTimeInPastProcess()
        throws JAXBException, IOException, URISyntaxException,
        OozieClientException, AuthenticationException, InterruptedException {

        processBundle.setProcessValidity(TimeUtil.getTimeWrtSystemTime(0),
            TimeUtil.getTimeWrtSystemTime(20));
        processBundle.submitFeedsScheduleProcess(prism);

        //get old process details
        String oldProcess = processBundle.getProcessData();
        String oldBundleId = OozieUtil.getLatestBundleID(cluster1OC,
            Util.readEntityName(processBundle.getProcessData()), EntityType.PROCESS);

        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, oldProcess, 0);
        List<String> initialNominalTimes = OozieUtil.getActionsNominalTime(cluster1OC,
            oldBundleId, EntityType.PROCESS);

        // update process by adding property
        processBundle.setProcessProperty("someProp", "someValue");
        ServiceResponse r = prism.getProcessHelper().update(oldProcess, processBundle.getProcessData());
        AssertUtil.assertSucceeded(r);

        //check new coord created with current time
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleId, initialNominalTimes,
            processBundle.getProcessData(), true, false);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, oldProcess, 1);
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleId, initialNominalTimes,
            processBundle.getProcessData(), true, true);
    }

    @Test(groups = {"MultiCluster", "0.3.1", "embedded"}, timeOut = 1200000, enabled = true)
    public void updateTimeInPastFeed()
        throws JAXBException, IOException, OozieClientException,
        URISyntaxException, AuthenticationException, InterruptedException {

        String startTimeClusterSource = TimeUtil.getTimeWrtSystemTime(-10);
        String startTimeClusterTarget = TimeUtil.getTimeWrtSystemTime(10);
        String feed = getMultiClusterFeed(startTimeClusterSource, startTimeClusterTarget);
        LOGGER.info("feed: " + Util.prettyPrintXml(feed));

        //submit and schedule feed
        ServiceResponse r = prism.getFeedHelper().submitAndSchedule(feed);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, feed, 0);

        //update frequency
        FeedMerlin updatedFeed = new FeedMerlin(feed);
        updatedFeed.setFrequency(new Frequency("7", Frequency.TimeUnit.minutes));
        r = prism.getFeedHelper().update(feed, updatedFeed.toString());
        AssertUtil.assertSucceeded(r);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, feed, 1);

        //check correct number of coord exists or not
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster1OC, updatedFeed.getName(), "REPLICATION"), 2);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster2OC, updatedFeed.getName(), "RETENTION"), 2);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster1OC, updatedFeed.getName(), "RETENTION"), 2);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster3OC, updatedFeed.getName(), "RETENTION"), 2);
    }

    @Test(groups = {"MultiCluster", "0.3.1", "distributed"}, timeOut = 1200000, enabled = true)
    public void inNextFewMinutesUpdateRollForwardProcess()
        throws JAXBException, IOException, URISyntaxException, JSchException,
        OozieClientException, SAXException, AuthenticationException, InterruptedException {
        /*
        submit process on 3 clusters. Schedule on 2 clusters. Bring down one of
        the scheduled cluster. Update with time 5 minutes from now. On running
        cluster new coord should be created with start time +5 and no instance
        should be missing. On 3rd cluster where process was only submit,
        definition should be updated. Bring the down cluster up. Update with same
        definition again, now the recently up cluster should also have new
        coords.
        */
        try {
            Util.startService(cluster2.getProcessHelper());
            String startTime = TimeUtil.getTimeWrtSystemTime(-15);
            processBundle.setProcessValidity(startTime,
                TimeUtil.getTimeWrtSystemTime(60));
            processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
                ClusterType.SOURCE, null, null);
            processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
                ClusterType.SOURCE, null, null);
            processBundle.submitBundle(prism);

            //schedule of 2 cluster
            cluster1.getProcessHelper().schedule(processBundle.getProcessData());
            cluster2.getProcessHelper().schedule(processBundle.getProcessData());
            InstanceUtil.waitTillInstancesAreCreated(cluster2OC, processBundle.getProcessData(), 0);

            //shut down cluster2
            Util.shutDownService(cluster2.getProcessHelper());

            // save old data before update
            String oldProcess = processBundle.getProcessData();
            String oldBundleIDCluster1 = OozieUtil
                .getLatestBundleID(cluster1OC,
                    Util.readEntityName(oldProcess), EntityType.PROCESS);
            String oldBundleIDCluster2 = OozieUtil
                .getLatestBundleID(cluster2OC,
                    Util.readEntityName(oldProcess), EntityType.PROCESS);
            List<String> oldNominalTimesCluster1 = OozieUtil.getActionsNominalTime(cluster1OC,
                oldBundleIDCluster1, EntityType.PROCESS);
            List<String> oldNominalTimesCluster2 = OozieUtil.getActionsNominalTime(cluster2OC,
                oldBundleIDCluster2, EntityType.PROCESS);

            //update process validity
            processBundle.setProcessProperty("someProp", "someValue");

            //send update request
            String updateTime = TimeUtil.getTimeWrtSystemTime(5);
            ServiceResponse r = prism.getProcessHelper().update(oldProcess, processBundle.getProcessData());
            AssertUtil.assertPartial(r);
            InstanceUtil.waitTillInstancesAreCreated(cluster1OC, processBundle.getProcessData(), 1);

            //verify new bundle on cluster1 and definition on cluster3
            OozieUtil
                .verifyNewBundleCreation(cluster1OC, oldBundleIDCluster1, oldNominalTimesCluster1,
                    oldProcess, true, false);
            OozieUtil.verifyNewBundleCreation(cluster2OC, oldBundleIDCluster2,
                oldNominalTimesCluster2,
                oldProcess, false, false);
            String definitionCluster3 = Util.getEntityDefinition(cluster3,
                processBundle.getProcessData(), true);
            Assert.assertTrue(XmlUtil.isIdentical(definitionCluster3,
                processBundle.getProcessData()), "Process definitions should be equal");

            //start the stopped cluster2
            Util.startService(cluster2.getProcessHelper());
            TimeUtil.sleepSeconds(40);
            String newBundleIdCluster1 = OozieUtil.getLatestBundleID(cluster1OC,
                Util.readEntityName(oldProcess), EntityType.PROCESS);

            //send second update request
            r = prism.getProcessHelper().update(oldProcess, processBundle.getProcessData());
            AssertUtil.assertSucceeded(r);
            String defCluster2 = Util.getEntityDefinition(cluster2,
                processBundle.getProcessData(), true);
            LOGGER.info("defCluster2 : " + Util.prettyPrintXml(defCluster2));

            // verify new bundle in cluster2 and no new bundle in cluster1  and
            OozieUtil.verifyNewBundleCreation(cluster1OC, newBundleIdCluster1,
                oldNominalTimesCluster1, oldProcess, false, false);
            OozieUtil.verifyNewBundleCreation(cluster2OC, oldBundleIDCluster2,
                oldNominalTimesCluster2, oldProcess, true, false);

            //wait till update time is reached
            TimeUtil.sleepTill(updateTime);
            OozieUtil.verifyNewBundleCreation(cluster2OC, oldBundleIDCluster2,
                oldNominalTimesCluster2, oldProcess, true, true);
            OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleIDCluster1,
                oldNominalTimesCluster1, oldProcess, true, true);
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    @Test(groups = {"MultiCluster", "0.3.1", "distributed"}, timeOut = 1200000, enabled = true)
    public void inNextFewMinutesUpdateRollForwardFeed()
        throws JAXBException, IOException, URISyntaxException, JSchException,
        OozieClientException, SAXException, AuthenticationException, InterruptedException {
        try {
            String startTimeClusterSource = TimeUtil.getTimeWrtSystemTime(-18);
            String feed = getMultiClusterFeed(startTimeClusterSource, startTimeClusterSource);
            LOGGER.info("feed: " + Util.prettyPrintXml(feed));

            //submit feed on all 3 clusters
            ServiceResponse r = prism.getFeedHelper().submitEntity(feed);
            AssertUtil.assertSucceeded(r);

            //schedule feed of cluster1 and cluster2
            r = cluster1.getFeedHelper().schedule(feed);
            AssertUtil.assertSucceeded(r);
            r = cluster2.getFeedHelper().schedule(feed);
            AssertUtil.assertSucceeded(r);
            InstanceUtil.waitTillInstancesAreCreated(cluster1OC, feed, 0);

            //shutdown cluster2
            Util.shutDownService(cluster2.getProcessHelper());

            //add some property to feed so that new bundle is created
            FeedMerlin updatedFeed = new FeedMerlin(feed).setFeedProperty("someProp", "someVal");

            //save old data
            String oldBundleCluster1 = OozieUtil.getLatestBundleID(cluster1OC,
                Util.readEntityName(feed), EntityType.FEED);
            List<String> oldNominalTimesCluster1 = OozieUtil.getActionsNominalTime(cluster1OC,
                oldBundleCluster1, EntityType.FEED);

            //send update command with +5 mins in future
            r = prism.getFeedHelper().update(feed, updatedFeed.toString());
            AssertUtil.assertPartial(r);

            //verify new bundle creation on cluster1 and new definition on cluster3
            OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleCluster1,
                oldNominalTimesCluster1, feed, true, false);
            String definition = Util.getEntityDefinition(cluster3, feed, true);
            Diff diff = XMLUnit.compareXML(definition, processBundle.getProcessData());
            LOGGER.info(diff);

            //start stopped cluster2
            Util.startService(cluster2.getProcessHelper());
            String newBundleCluster1 = OozieUtil.getLatestBundleID(cluster1OC,
                Util.readEntityName(feed), EntityType.FEED);

            //send update again
            r = prism.getFeedHelper().update(feed, updatedFeed.toString());
            AssertUtil.assertSucceeded(r);

            //verify new bundle creation on cluster2 and no new bundle on cluster1
            Assert.assertEquals(OozieUtil
                .checkIfFeedCoordExist(cluster2OC, Util.readEntityName(feed), "RETENTION"), 2);
            OozieUtil.verifyNewBundleCreation(cluster1OC, newBundleCluster1,
                oldNominalTimesCluster1, feed, false, false);

            //wait till update time is reached
            TimeUtil.sleepTill(TimeUtil.getTimeWrtSystemTime(5));

            //verify new bundle creation with instance matching
            OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleCluster1,
                oldNominalTimesCluster1, feed, true, true);
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    @Test(groups = {"multiCluster", "0.3.1", "embedded"}, timeOut = 1200000, enabled = true)
    public void updateTimeAfterEndTimeProcess()
        throws JAXBException, InterruptedException, IOException, URISyntaxException,
        OozieClientException, AuthenticationException {
        /* submit and schedule process with end time after 60 mins. Set update time
           as with +60 from start mins */
        LOGGER.info("Running test updateTimeAfterEndTimeProcess");
        String startTime = TimeUtil.getTimeWrtSystemTime(-15);
        String endTime = TimeUtil.getTimeWrtSystemTime(60);
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(10);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0),
            Util.readEntityName(processBundle.getProcessData()), 0,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        //save old data
        String oldProcess = processBundle.getProcessData();
        String oldBundleID = OozieUtil.getLatestBundleID(cluster1OC,
            Util.readEntityName(oldProcess), EntityType.PROCESS);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster1OC, oldBundleID, EntityType.PROCESS);

        //update
        processBundle.setProcessProperty("someProp", "someVal");
        String updateTime = TimeUtil.addMinsToTime(endTime, 60);
        LOGGER.info("Original Feed : " + Util.prettyPrintXml(oldProcess));
        LOGGER.info("Updated Feed :" + Util.prettyPrintXml(processBundle.getProcessData()));
        LOGGER.info("Update Time : " + updateTime);
        ServiceResponse r = prism.getProcessHelper().update(oldProcess, processBundle.getProcessData());
        AssertUtil.assertSucceeded(r);

        //verify new bundle creation with instances matching
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleID, oldNominalTimes,
            oldProcess, true, false);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, processBundle.getProcessData(), 1);
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleID, oldNominalTimes,
            oldProcess, true, true);
    }

    @Test(groups = {"multiCluster", "0.3.1", "embedded"}, timeOut = 1200000, enabled = true)
    public void updateTimeAfterEndTimeFeed()
        throws JAXBException, IOException, OozieClientException,
        URISyntaxException, AuthenticationException, InterruptedException {

        /* submit and schedule feed with end time 60 mins in future and update with +60 in future*/
        String startTime = TimeUtil.getTimeWrtSystemTime(-15);
        String endTime = TimeUtil.getTimeWrtSystemTime(60);

        String feed = processBundle.getDataSets().get(0);
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();
        feed = FeedMerlin.fromString(feed).addFeedCluster(new FeedMerlin.FeedClusterBuilder(
            Util.readEntityName(processBundle.getClusters().get(0)))
            .withRetention("days(100000)", ActionType.DELETE)
            .withValidity(startTime, endTime)
            .withClusterType(ClusterType.SOURCE)
            .withDataLocation(baseTestDir + "/replication" + MINUTE_DATE_PATTERN)
            .build()).toString();

        ServiceResponse r = prism.getClusterHelper().submitEntity(
            processBundle.getClusters().get(0));
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(r);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, feed, 0);

        //save old data
        String oldBundleID = OozieUtil.getLatestBundleID(cluster1OC,
            Util.readEntityName(feed), EntityType.FEED);
        String updateTime = TimeUtil.addMinsToTime(endTime, 60);
        FeedMerlin updatedFeed = new FeedMerlin(feed).setFeedProperty("someProp", "someVal");
        LOGGER.info("Original Feed : " + Util.prettyPrintXml(feed));
        LOGGER.info("Updated Feed :" + Util.prettyPrintXml(updatedFeed.toString()));
        LOGGER.info("Update Time : " + updateTime);
        r = prism.getFeedHelper().update(feed, updatedFeed.toString());
        AssertUtil.assertSucceeded(r);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, feed, 1);

        //verify new bundle creation
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleID, null, feed, true, false);
    }

    @Test(groups = {"multiCluster", "0.3.1", "embedded"}, timeOut = 1200000, enabled = true)
    public void updateTimeBeforeStartTimeProcess() throws JAXBException, IOException,
        URISyntaxException, OozieClientException, AuthenticationException,
        InterruptedException {

        /* submit and schedule process with start time +10 mins from now. Update with start time
        -4 and update time +2 mins */
        String startTime = TimeUtil.getTimeWrtSystemTime(10);
        String endTime = TimeUtil.getTimeWrtSystemTime(20);
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.submitFeedsScheduleProcess(prism);

        //save old data
        String oldProcess = processBundle.getProcessData();
        String oldBundleID = OozieUtil.getLatestBundleID(cluster1OC,
            Util.readEntityName(oldProcess), EntityType.PROCESS);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster1OC, oldBundleID,
            EntityType.PROCESS);
        processBundle.setProcessValidity(TimeUtil.addMinsToTime(startTime, -4), endTime);
        ServiceResponse r = prism.getProcessHelper().update(oldProcess, processBundle.getProcessData());
        AssertUtil.assertSucceeded(r);
        TimeUtil.sleepSeconds(10);

        //verify new bundle creation
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleID, oldNominalTimes,
            oldProcess, true, false);
    }

    @Test(groups = {"MultiCluster", "0.3.1"}, timeOut = 1200000, enabled = true)
    public void updateDiffClusterDiffValidityProcess()
        throws JAXBException, IOException, URISyntaxException, OozieClientException,
        AuthenticationException, InterruptedException {

        //set start end process time for 3 clusters
        String startTimeCluster1 = TimeUtil.getTimeWrtSystemTime(-40);
        String endTimeCluster1 = TimeUtil.getTimeWrtSystemTime(3);
        String startTimeCluster2 = TimeUtil.getTimeWrtSystemTime(120);
        String endTimeCluster2 = TimeUtil.getTimeWrtSystemTime(240);
        String startTimeCluster3 = TimeUtil.getTimeWrtSystemTime(-30);
        String endTimeCluster3 = TimeUtil.getTimeWrtSystemTime(180);

        //create multi cluster bundle
        processBundle.setProcessValidity(startTimeCluster1, endTimeCluster1);
        processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
            ClusterType.SOURCE, startTimeCluster2, endTimeCluster2);
        processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
            ClusterType.SOURCE, startTimeCluster3, endTimeCluster3);

        //submit and schedule
        processBundle.submitFeedsScheduleProcess(prism);

        //wait for coord to be in running state
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, processBundle.getProcessData(), 0);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, processBundle.getProcessData(), 0);

        //save old info
        String oldBundleIdCluster1 = OozieUtil.getLatestBundleID(cluster1OC,
            Util.readEntityName(processBundle.getProcessData()), EntityType.PROCESS);
        List<String> nominalTimesCluster1 = OozieUtil.getActionsNominalTime(cluster1OC,
            oldBundleIdCluster1, EntityType.PROCESS);
        String oldBundleIdCluster2 = OozieUtil.getLatestBundleID(cluster2OC,
            Util.readEntityName(processBundle.getProcessData()), EntityType.PROCESS);
        String oldBundleIdCluster3 = OozieUtil.getLatestBundleID(cluster3OC,
            Util.readEntityName(processBundle.getProcessData()), EntityType.PROCESS);
        List<String> nominalTimesCluster3 = OozieUtil.getActionsNominalTime(cluster3OC,
            oldBundleIdCluster3, EntityType.PROCESS);

        //update process
        String updateTime = TimeUtil.addMinsToTime(endTimeCluster1, 3);
        processBundle.setProcessProperty("someProp", "someVal");
        ServiceResponse r = prism.getProcessHelper().update(processBundle.getProcessData(),
            processBundle.getProcessData());
        AssertUtil.assertSucceeded(r);

        //check for new bundle to be created
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleIdCluster1,
            nominalTimesCluster1, processBundle.getProcessData(), true, false);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleIdCluster3,
            nominalTimesCluster3, processBundle.getProcessData(), true, false);
        OozieUtil.verifyNewBundleCreation(cluster2OC, oldBundleIdCluster2,
            nominalTimesCluster3, processBundle.getProcessData(), true, false);

        //wait till new coord are running on cluster1
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, processBundle.getProcessData(), 1);
        OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleIdCluster1,
            nominalTimesCluster1, processBundle.getProcessData(), true, true);

        //verify
        String coordStartTimeCluster3 = OozieUtil.getCoordStartTime(cluster3OC,
            processBundle.getProcessData(), 1);
        String coordStartTimeCluster2 = OozieUtil.getCoordStartTime(cluster2OC,
            processBundle.getProcessData(), 1);

        DateTime updateTimeOozie = TimeUtil.oozieDateToDate(updateTime);
        Assert.assertTrue(TimeUtil.oozieDateToDate(coordStartTimeCluster3).isAfter(updateTimeOozie)
            || TimeUtil.oozieDateToDate(coordStartTimeCluster3).isEqual(updateTimeOozie),
            "new coord start time is not correct");
        Assert.assertFalse(
            TimeUtil.oozieDateToDate(coordStartTimeCluster2).isEqual(updateTimeOozie),
            "new coord start time is not correct");
        TimeUtil.sleepTill(updateTime);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, processBundle.getProcessData(), 1);

        //verify that no instance are missing
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleIdCluster3,
            nominalTimesCluster3, processBundle.getProcessData(), true, true);
    }

    private String submitAndScheduleFeed(Bundle b)
        throws JAXBException, IOException, URISyntaxException, AuthenticationException,
        InterruptedException {
        String feed = b.getDataSets().get(0);
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();
        feed = FeedMerlin.fromString(feed)
            .addFeedCluster(
                new FeedMerlin.FeedClusterBuilder(Util.readEntityName(b.getClusters().get(0)))
                    .withRetention("days(1000000)", ActionType.DELETE)
                    .withValidity("2012-10-01T12:10Z", "2099-10-01T12:10Z")
                    .withClusterType(ClusterType.SOURCE)
                    .withPartition("")
                    .withDataLocation("/someTestPath" + MINUTE_DATE_PATTERN)
                    .build())
            .toString();
        ServiceResponse r = prism.getClusterHelper().submitEntity(
            b.getClusters().get(0));
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper().submitAndSchedule(feed);
        AssertUtil.assertSucceeded(r);
        return feed;
    }

    private String getMultiClusterFeed(String startTimeClusterSource, String startTimeClusterTarget)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        String testDataDir = baseTestDir + "/replication";

        //create desired feed
        String feed = bundles[0].getDataSets().get(0);

        //cluster1 is target, cluster2 is source and cluster3 is neutral
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTimeClusterSource, "2099-10-01T12:10Z")
                .build())
            .toString();
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTimeClusterTarget, "2099-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(testDataDir + MINUTE_DATE_PATTERN)
                .build())
            .toString();
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTimeClusterSource, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withDataLocation(testDataDir + MINUTE_DATE_PATTERN)
                .build())
            .toString();

        //submit clusters
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        //create test data on cluster2
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTimeClusterSource,
            TimeUtil.getTimeWrtSystemTime(60), 1);
        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.SINGLE_FILE,
            testDataDir + "/", dataDates);
        return feed;
    }
}

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
package org.apache.falcon.regression.lineage;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Testing the list instances api for feed. Testing is based on initial scenario and sets of
 * expected instance statuses which are being compared with actual result of -list request
 * with different parameters in different order, variation, etc.
 */
@Test(groups = { "distributed", "embedded", "sanity", "multiCluster"})
public class ListFeedInstancesTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ListFeedInstancesTest.class);
    private OozieClient cluster2OC = serverOC.get(1);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String sourcePath = baseTestHDFSDir + "/source";
    private String feedDataLocation = sourcePath + MINUTE_DATE_PATTERN;
    private String targetPath = baseTestHDFSDir + "/target";
    private String targetDataLocation = targetPath + MINUTE_DATE_PATTERN;
    private final String startTime = "2015-01-02T00:00Z";
    private final String endTime = "2015-01-02T00:57Z";
    private String feedName;

    @BeforeClass(alwaysRun = true)
    public void setUp()
        throws IOException, OozieClientException, JAXBException, AuthenticationException,
        URISyntaxException, InterruptedException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
        }
    }

    /*
     * Prepares running feed with instances ordered (desc): 1 waiting, 1 running, 1 suspended,
     * 5 waiting, 2 killed, 2 waiting. Testing is based on expected sets of instance statuses.
     * Variety of instance statuses increases accuracy of testing.
     */
    @BeforeMethod(alwaysRun = true)
    private void prepareScenario() throws AuthenticationException, IOException, URISyntaxException,
        JAXBException, OozieClientException, InterruptedException {
        bundles[0].setInputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedDataLocation);
        String feed = bundles[0].getInputFeedFromBundle();
        feedName = Util.readEntityName(feed);
        String cluster1Def = bundles[0].getClusters().get(0);
        String cluster2Def = bundles[1].getClusters().get(0);
        //erase all clusters from feed definition
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();
        //set cluster1 as source
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(cluster1Def))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .withClusterType(ClusterType.SOURCE)
                .build()).toString();
        //set cluster2 as target
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(cluster2Def))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(targetDataLocation)
                .build()).toString();

        //submit clusters
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster1Def));
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster2Def));

        //submit and schedule feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        InstanceUtil.waitTillInstancesAreCreated(cluster2OC, feed, 0);
        InstanceUtil.waitTillInstanceReachState(cluster2OC, feedName, 12,
            CoordinatorAction.Status.WAITING, EntityType.FEED);

        //retrieve instances to rule them directly
        List<CoordinatorAction> actions = getReplicationInstances(cluster2OC, feedName);
        LOGGER.info(actions);
        Assert.assertNotNull(actions, "Required coordinator not found.");
        Assert.assertEquals(actions.size(), 12, "Unexpected number of actions.");

        //killing the 3d and the 4th instances
        String range;
        InstancesResult r;
        for (int i = 2; i <= 3; i++) {
            HadoopUtil.createFolders(serverFS.get(0), "", Arrays.asList(actions.get(i)
                .getMissingDependencies().split("#")));
            //only running instance can be killed, so we should make it running and then kill it
            InstanceUtil.waitTillInstanceReachState(cluster2OC, feedName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.FEED, 3);
            range = "?start=" + TimeUtil.addMinsToTime(startTime, i * 5 - 2)
                + "&end=" + TimeUtil.addMinsToTime(startTime, i * 5 + 2);
            r = prism.getFeedHelper().getProcessInstanceKill(feedName, range);
            InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);
        }
        //wait for 10th instance to run, suspend it then
        HadoopUtil.createFolders(serverFS.get(0), "", Arrays.asList(actions.get(9)
            .getMissingDependencies().split("#")));
        InstanceUtil.waitTillInstanceReachState(cluster2OC, feedName, 1,
            CoordinatorAction.Status.RUNNING, EntityType.FEED, 3);
        range = "?start=" + TimeUtil.addMinsToTime(endTime, -15)
            + "&end=" + TimeUtil.addMinsToTime(endTime, -10);
        r = prism.getFeedHelper().getProcessInstanceSuspend(feedName, range);
        InstanceUtil.validateResponse(r, 1, 0, 1, 0, 0);

        //wait for 11h to run
        HadoopUtil.createFolders(serverFS.get(0), "", Arrays.asList(actions.get(10)
            .getMissingDependencies().split("#")));
        InstanceUtil.waitTillInstanceReachState(cluster2OC, feedName, 1,
            CoordinatorAction.Status.RUNNING, EntityType.FEED, 3);

        //check that the scenario works as expected.
        r = prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&numResults=12");
        InstanceUtil.validateResponse(r, 12, 1, 1, 8, 2);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        HadoopUtil.deleteDirIfExists(sourcePath, serverFS.get(0));
    }

    /*
     * Retrieves replication coordinator actions (replication instances).
     * @param client target oozie client
     * @param fName feed name
     */
    private List<CoordinatorAction> getReplicationInstances(OozieClient client, String fName)
        throws OozieClientException {
        String filter = "name=FALCON_FEED_" + fName;
        List<BundleJob> bundleJobs  = OozieUtil.getBundles(client, filter, 0, 10);
        Assert.assertNotEquals(bundleJobs.size(), 0, "Could not retrieve bundles");
        List<String> bundleIds = OozieUtil.getBundleIds(bundleJobs);
        String bundleId = OozieUtil.getMaxId(bundleIds);
        LOGGER.info(String.format("Using bundle %s", bundleId));
        List<CoordinatorJob> coords = client.getBundleJobInfo(bundleId).getCoordinators();
        String coordId = null;
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_REPLICATION")) {
                coordId = coord.getId();
                break;
            }
        }
        LOGGER.info(String.format("Using coordinator id: %s", coordId));
        Assert.assertNotNull(coordId, "Replication coordinator not found.");
        CoordinatorJob coordinatorJob = client.getCoordJobInfo(coordId);
        return coordinatorJob.getActions();
    }

    /**
     * Test the list feed instances api using an orderBy parameter. Check the order.
     */
    @Test
    public void testFeedOrderBy()
        throws URISyntaxException, OozieClientException, JAXBException, AuthenticationException,
        IOException, InterruptedException {
        SoftAssert softAssert = new SoftAssert();
        //orderBy start time, check on order
        InstancesResult r = prism.getFeedHelper().listInstances(feedName,
            "orderBy=startTime&sortOrder=desc", null);
        InstancesResult.Instance[] instances = r.getInstances();
        Date previousDate = new Date();
        for (InstancesResult.Instance instance : instances) {
            Date current = instance.getStartTime();
            if (current != null) { //e.g if instance is WAITING it doesn't have start time
                softAssert.assertTrue(current.before(previousDate) || current.equals(previousDate),
                    "Wrong order. Current startTime :" + current + " Previous: " + previousDate);
                previousDate = (Date) current.clone();
            }
        }
        //orderBy status, check on order
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&numResults=12&orderBy=status&sortOrder=desc", null);
        InstanceUtil.validateResponse(r, 12, 1, 1, 8, 2);
        instances = r.getInstances();
        InstancesResult.WorkflowStatus previousStatus = InstancesResult.WorkflowStatus.WAITING;
        for (InstancesResult.Instance instance : instances) {
            InstancesResult.WorkflowStatus current = instance.getStatus();
            softAssert.assertTrue(current.toString().compareTo(previousStatus.toString()) <= 0,
                "Wrong order. Compared " + current + " and " + previousStatus + " statuses.");
            previousStatus = current;
        }
        //sort by endTime, check on order
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&numResults=12&orderBy=endTime&sortOrder=desc", null);
        instances = r.getInstances();
        previousDate = new Date();
        for (InstancesResult.Instance instance : instances) {
            Date current = instance.getEndTime();
            if (current != null) { //e.g if instance is WAITING it doesn't have end time
                softAssert.assertTrue(current.before(previousDate) || current.equals(previousDate),
                    "Wrong order. Current startTime :" + current + " Previous: " + previousDate);
                previousDate = (Date) current.clone();
            }
        }
        softAssert.assertAll();
    }

    /**
     * Test the list feed instance api using start/end parameters. Check instances number.
     */
    @Test
    public void testFeedStartEnd()
        throws URISyntaxException, OozieClientException, JAXBException, AuthenticationException,
        IOException, InterruptedException {
        //actual start/end values.
        InstancesResult r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&end=" + endTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);

        //without params, the default start/end should be applied. End is set to now,
        // start is set to end - (10 * entityFrequency)
        r = prism.getFeedHelper().listInstances(feedName, null, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);

        //increasing -start, -end stays the same.
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + TimeUtil.addMinsToTime(startTime, 6)
                + "&end=" + TimeUtil.addMinsToTime(endTime, -5), null);
        InstanceUtil.validateResponse(r, 9, 1, 1, 5, 2);
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + TimeUtil.addMinsToTime(startTime, 11)
                + "&end=" + TimeUtil.addMinsToTime(endTime, -5), null);
        InstanceUtil.validateResponse(r, 8, 1, 1, 5, 1);
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + TimeUtil.addMinsToTime(startTime, 16)
                + "&end=" + TimeUtil.addMinsToTime(endTime, -5), null);
        InstanceUtil.validateResponse(r, 7, 1, 1, 5, 0);

        //one instance between start/end, killed instance
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + TimeUtil.addMinsToTime(startTime, 12)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 16), null);
        InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);

        //one instance between start/end, waiting instance
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + TimeUtil.addMinsToTime(endTime, -5) + "&end=" + endTime, null);
        InstanceUtil.validateResponse(r, 1, 0, 0, 1, 0);

        //only start, actual feed startTime, should get 1-10 instances(end is automatically set to now).
        r = prism.getFeedHelper().listInstances(feedName, "start=" + startTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);

        //only start, greater then the actual startTime.
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + TimeUtil.addMinsToTime(startTime, 16), null);
        InstanceUtil.validateResponse(r, 8, 1, 1, 6, 0);

        //only end, 1 instance is expected
        r = prism.getFeedHelper().listInstances(feedName,
            "end=" + TimeUtil.addMinsToTime(startTime, 4), null);
        InstanceUtil.validateResponse(r, 1, 0, 0, 1, 0);

        //only end, actual value, 10 the most recent instances are expected
        r = prism.getFeedHelper().listInstances(feedName, "end=" + endTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);

        //only end, first 6 instances
        r = prism.getFeedHelper().listInstances(feedName,
            "end=" + TimeUtil.addMinsToTime(endTime, -31), null);
        InstanceUtil.validateResponse(r, 6, 0, 0, 4, 2);
        //only end, first 8 instances
        r = prism.getFeedHelper().listInstances(feedName,
            "end=" + TimeUtil.addMinsToTime(endTime, -21), null);
        InstanceUtil.validateResponse(r, 8, 0, 0, 6, 2);
    }

    /**
     * List feed instances with -offset and -numResults params expecting the list of feed
     * instances which start at the right offset and number of instances matches to expected.
     */
    @Test
    public void testFeedOffsetNumResults()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException {
        //check the default value of the numResults param. Expecting 10 instances.
        InstancesResult r = prism.getFeedHelper().listInstances(feedName, null, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);

        //changing a value to 6. 6 instances are expected
        r = prism.getFeedHelper().listInstances(feedName, "numResults=6", null);
        InstanceUtil.validateResponse(r, 6, 1, 1, 4, 0);

        //use a start option without a numResults parameter. 10 instances are expected
        r = prism.getFeedHelper().listInstances(feedName, "start=" + startTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);

        //use a start option with a numResults value which is smaller then the default.
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&numResults=8", null);
        InstanceUtil.validateResponse(r, 8, 1, 1, 6, 0);

        //use a start option with a numResults value greater then the default.
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 12, 1, 1, 8, 2);

        //get all instances
        InstancesResult.Instance[] allInstances = r.getInstances();

        //adding an offset param into request. Expected (total number - offset) instances.
        int offset = 3;
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&offset=" + offset + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 9, 0, 0, 7, 2);

        //check that expected instances were retrieved
        InstancesResult.Instance[] instances = r.getInstances();
        for (int i = 0; i < 9; i++) {
            LOGGER.info("Comparing instances: " + instances[i] + " and " + allInstances[i + offset]);
            Assert.assertTrue(instances[i].getInstance().equals(allInstances[i + offset].getInstance()));
        }
        //use different offset and numResults params in the request
        offset = 6;
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&offset=" + offset + "&numResults=6", null);
        InstanceUtil.validateResponse(r, 6, 0, 0, 4, 2);

        //check that expected instances are present in response
        instances = r.getInstances();
        for (int i = 0; i < 6; i++) {
            LOGGER.info("Comparing instances: " + instances[i] + " and " + allInstances[i + offset]);
            Assert.assertTrue(instances[i].getInstance().equals(allInstances[i + offset].getInstance()));
        }
    }

    /**
     * List feed instances with filterBy parameter.
     */
    @Test
    public void testFeedFilterBy()
        throws OozieClientException, AuthenticationException, IOException, URISyntaxException,
        InterruptedException {
        //test with the filterBy status.
        InstancesResult r = prism.getFeedHelper().listInstances(feedName,
            "filterBy=STATUS:RUNNING", null);
        InstanceUtil.validateResponse(r, 1, 1, 0, 0, 0);
        //end is set to now (actual end), start is set to (end - (10 * entityFrequency))
        //filtered range is from 3rd till 12th instance
        r = prism.getFeedHelper().listInstances(feedName, "filterBy=STATUS:WAITING", null);
        InstanceUtil.validateResponse(r, 6, 0, 0, 6, 0);

        //get all instances.
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 12, 1, 1, 8, 2);

        //use different statuses, filterBy among all instances.
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&filterBy=STATUS:KILLED", null);
        InstanceUtil.validateResponse(r, 2, 0, 0, 0, 2);
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&filterBy=STATUS:SUSPENDED", null);
        InstanceUtil.validateResponse(r, 1, 0, 1, 0, 0);
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&filterBy=STATUS:RUNNING", null);
        InstanceUtil.validateResponse(r, 1, 1, 0, 0, 0);
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&filterBy=STATUS:WAITING", null);
        InstanceUtil.validateResponse(r, 8, 0, 0, 8, 0);

        //use additional filters.
        String clusterName = bundles[1].getClusterNames().get(0);
        r = prism.getFeedHelper().listInstances(feedName,
            "start=" + startTime + "&filterBy=CLUSTER:" + clusterName, null);
        InstanceUtil.validateResponse(r, 10, 1, 1, 6, 2);
    }

    /**
     * List feed instances using custom filter. Expecting list of feed instances which
     * satisfy custom filters.
     */
    @Test
    public void testFeedCustomFilter()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException {
        String params = "start=" + startTime + "&filterBy=status:RUNNING";
        InstancesResult r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 1, 1, 0, 0, 0);

        //expecting 0 instances, because RUNNING instance is out of range start + 10 instances
        params = "start=" + startTime + "&end=" + endTime + "&filterBy=status:RUNNING&offset=2";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateSuccessWOInstances(r);

        //offset is absent that's why whole range is filtered
        params = "start=" + startTime + "&end=" + endTime + "&filterBy=status:WAITING";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 8, 0, 0, 8, 0);

        //filtered range is from 1st till 9th instances inclusively
        params = "start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 41)
            + "&filterBy=status:WAITING";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 7, 0, 0, 7, 0);

        //filtered range is within 3nd till 8th instance inclusively
        params = "start=" + startTime + "&offset=4&numResults=6&filterBy=status:WAITING";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 4, 0, 0, 4, 0);

        //filtered range is within 4th till 10th instances inclusively
        params = "start=" + TimeUtil.addMinsToTime(startTime, 16) + "&offset=2&numResults=12";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 6, 0, 1, 5, 0);

        //use mix of params
        String sourceCluster = bundles[0].getClusterNames().get(0);
        String clusterName = bundles[1].getClusterNames().get(0);
        params = "start=" + startTime + "&filterBy=STATUS:KILLED,CLUSTER:"+ clusterName
            + "&numResults=5&orderBy=startTime&sortOrder=desc";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 2, 0, 0, 0, 2);

        //should be ordered by a start time
        SoftAssert softAssert = new SoftAssert();
        InstancesResult.Instance[] instances = r.getInstances();
        Date previousDate = new Date();
        for (InstancesResult.Instance instance : instances) {
            Date current = instance.getStartTime();
            softAssert.assertNotNull(current, "Start time shouldn't be null for KILLED instance.");
            softAssert.assertTrue(current.before(previousDate) || current.equals(previousDate),
                "Wrong order. Current startTime :" + current + " Previous: " + previousDate);
            previousDate = (Date) current.clone();
        }
        softAssert.assertAll();

        // filtered range is within 2nd and 10th instance inclusively
        params = "start=" + TimeUtil.addMinsToTime(startTime, 2) + "&offset=2";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 9, 0, 1, 6, 2);

        //filtered range is within 7nd and 11th instances inclusively
        params = "start=" + TimeUtil.addMinsToTime(startTime, 2) + "&filterBy=SOURCECLUSTER:"
            + sourceCluster + "&offset=1&numResults=5" + "&colo=*";
        r = prism.getFeedHelper().listInstances(feedName, params, null);
        InstanceUtil.validateResponse(r, 5, 1, 1, 3, 0);
    }
}

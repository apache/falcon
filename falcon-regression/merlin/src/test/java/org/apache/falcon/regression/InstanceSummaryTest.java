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

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.OozieClientException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;

/** This test currently provide minimum verification. More detailed test should be added:
    1. process : test summary single cluster few instance some future some past
    2. process : test multiple cluster, full past on one cluster,  full future on one cluster,
    half future / past on third one
    3. feed : same as test 1 for feed
    4. feed : same as test 2 for feed
 */
@Test(groups = "embedded")
public class InstanceSummaryTest extends BaseTestClass {

    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestHDFSDir + "/testInputData" + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String startTime;
    private String endTime;
    private ColoHelper cluster3 = servers.get(2);
    private Bundle processBundle;
    private String processName;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        startTime = TimeUtil.get20roundedTime(TimeUtil.getTimeWrtSystemTime(-20));
        endTime = TimeUtil.getTimeWrtSystemTime(60);
        String startTimeData = TimeUtil.addMinsToTime(startTime, -100);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTimeData, endTime, 20);
        for (FileSystem fs : serverFS) {
            HadoopUtil.deleteDirIfExists(Util.getPathPrefix(feedInputPath), fs);
            HadoopUtil.flattenAndPutDataInFolder(fs, OSUtil.NORMAL_INPUT,
                Util.getPathPrefix(feedInputPath), dataDates);
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        processBundle = new Bundle(BundleUtil.readELBundle(), cluster3);
        processBundle.generateUniqueBundle(this);
        processBundle.setInputFeedDataPath(feedInputPath);
        processBundle.setOutputFeedLocationData(baseTestHDFSDir + "/output" + MINUTE_DATE_PATTERN);
        processBundle.setProcessWorkflow(aggregateWorkflowDir);

        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(BundleUtil.readELBundle(), servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
        processName = Util.readEntityName(processBundle.getProcessData());
    }

    /**
     *  Schedule single-cluster process. Get its instances summary.
     */
    @Test(enabled = true, timeOut = 1200000)
    public void testSummarySingleClusterProcess()
        throws URISyntaxException, JAXBException, IOException, ParseException,
        OozieClientException, AuthenticationException, InterruptedException {
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(serverOC.get(2), processBundle.getProcessData(), 0);

        // start only at start time
        InstancesSummaryResult r = prism.getProcessHelper()
            .getInstanceSummary(processName, "?start=" + startTime);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(2), processName, 2,
            Status.SUCCEEDED, EntityType.PROCESS);

        //AssertUtil.assertSucceeded(r);

        //start only before process start
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, -100));
        //AssertUtil.assertFailed(r,"response should have failed");

        //start only after process end
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, 120));


        //start only at mid specific instance
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, 10));

        //start only in between 2 instance
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, 7));

        //start and end at start and end
        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + startTime + "&end=" + endTime);

        //start in between and end at end
        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + TimeUtil.addMinsToTime(startTime, 14) + "&end=" + endTime);

        //start at start and end between
        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(endTime, -20));

        // start and end in between
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, 20)
                    + "&end=" + TimeUtil.addMinsToTime(endTime, -13));

        //start before start with end in between
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, -100)
                    + "&end=" + TimeUtil.addMinsToTime(endTime, -37));

        //start in between and end after end
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?start=" + TimeUtil.addMinsToTime(startTime, 60)
                    + "&end=" + TimeUtil.addMinsToTime(endTime, 100));

        // both start end out od range
        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + TimeUtil.addMinsToTime(startTime, -100)
                + "&end=" + TimeUtil.addMinsToTime(endTime, 100));

        // end only
        r = prism.getProcessHelper().getInstanceSummary(processName,
                "?end=" + TimeUtil.addMinsToTime(endTime, -30));
    }

    /**
     * Adjust multi-cluster process. Submit and schedule it. Get its instances summary.
     */
    @Test(enabled = true, timeOut = 1200000)
    public void testSummaryMultiClusterProcess() throws JAXBException,
            ParseException, IOException, URISyntaxException, AuthenticationException,
            InterruptedException {
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.submitFeedsScheduleProcess(prism);
        InstancesSummaryResult r = prism.getProcessHelper()
            .getInstanceSummary(processName, "?start=" + startTime);

        r = prism.getProcessHelper().getInstanceSummary(processName,
             "?start=" + startTime + "&end=" + endTime);

        r = prism.getProcessHelper().getInstanceSummary(processName,
             "?start=" + startTime + "&end=" + endTime);

        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + startTime + "&end=" + endTime);

        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + startTime + "&end=" + endTime);

        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + startTime + "&end=" + endTime);

        r = prism.getProcessHelper().getInstanceSummary(processName,
            "?start=" + startTime + "&end=" + endTime);
    }

    /**
     *  Adjust multi-cluster feed. Submit and schedule it. Get its instances summary.
     */
    @Test(enabled = true, timeOut = 1200000)
    public void testSummaryMultiClusterFeed() throws JAXBException, ParseException, IOException,
            URISyntaxException, OozieClientException, AuthenticationException,
            InterruptedException {

        //create desired feed
        String feed = bundles[0].getDataSets().get(0);

        //cluster_1 is target, cluster_2 is source and cluster_3 is neutral
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();

        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTime, "2099-10-01T12:10Z")
                .build()).toString();

        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTime, "2099-10-01T12:25Z")
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(feedInputPath)
                .build()).toString();

        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(100000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .withDataLocation(feedInputPath)
                .build()).toString();

        //submit clusters
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        //create test data on cluster_2
      /*InstanceUtil.createDataWithinDatesAndPrefix(cluster2,
        InstanceUtil.oozieDateToDate(startTime),
        InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(60)),
        feedInputPath, 1);*/

        //submit and schedule feed
        prism.getFeedHelper().submitAndSchedule(feed);

        InstancesSummaryResult r = prism.getFeedHelper()
            .getInstanceSummary(Util.readEntityName(feed), "?start=" + startTime);

        r = prism.getFeedHelper().getInstanceSummary(Util.readEntityName(feed),
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(endTime, -20));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
    }
}

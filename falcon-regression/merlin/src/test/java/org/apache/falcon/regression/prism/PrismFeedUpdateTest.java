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

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
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
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


@Test(groups = "embedded")
public class PrismFeedUpdateTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    FileSystem server1FS = serverFS.get(0);
    OozieClient OC1 = serverOC.get(0);
    String baseTestDir = baseHDFSDir + "/PrismFeedUpdateTest";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    public final String cluster1colo = cluster1.getClusterHelper().getColoName();
    public final String cluster2colo = cluster2.getClusterHelper().getColoName();
    private static final Logger logger = Logger.getLogger(PrismFeedUpdateTest.class);
    String feedInputTimedOutPath =
        baseTestDir + "/timedout/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

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
            bundles[i].setInputFeedDataPath(feedInputTimedOutPath);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * TODO : complete test case
     */
    @Test(enabled = true, timeOut = 1200000)
    public void updateFeedQueue_dependentMultipleProcess_oneProcessZeroInput() throws Exception {
        //cluster1colo and cluster2colo are source. feed01 on cluster1colo target cluster2colo,
        // feed02 on cluster2colo target cluster1colo

        //get 3 unique bundles
        //set cluster colos
        bundles[0].setCLusterColo(cluster1colo);
        logger.info("cluster bundles[0]: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));

        bundles[1].setCLusterColo(cluster2colo);
        logger.info("cluster bundles[1]: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));

        //submit 3 clusters

        //get 2 unique feeds
        String feed01 = bundles[0].getInputFeedFromBundle();
        String outputFeed = bundles[0].getOutputFeedFromBundle();

        //set source and target for the 2 feeds

        //set clusters to null;
        feed01 = InstanceUtil
            .setFeedCluster(feed01,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);
        outputFeed = InstanceUtil
            .setFeedCluster(outputFeed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);


        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01,
            baseTestDir + "/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


        //generate data in both the colos cluster1colo and cluster2colo
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
        HadoopUtil.lateDataReplenish(server1FS, 70, 1, prefix, null);

        String startTime = TimeUtil.getTimeWrtSystemTime(-50);

        //set clusters for feed01
        feed01 = InstanceUtil
            .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                null);
        feed01 = InstanceUtil
            .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, null);
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null);


        //submit and schedule feeds
        logger.info("feed01: " + Util.prettyPrintXml(feed01));
        logger.info("outputFeed: " + Util.prettyPrintXml(outputFeed));

        //create 2 process with 2 clusters

        //get first process
        String process01 = bundles[0].getProcessData();

        //add clusters to process

        String processStartTime = TimeUtil.getTimeWrtSystemTime(-11);
        String processEndTime = TimeUtil.getTimeWrtSystemTime(70);


        process01 = InstanceUtil
            .setProcessCluster(process01, null,
                XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
        process01 = InstanceUtil
            .setProcessCluster(process01, Util.readEntityName(bundles[0].getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));
        process01 = InstanceUtil
            .setProcessCluster(process01, Util.readEntityName(bundles[1].getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));

        //get 2nd process :
        String process02 = process01;
        process02 = InstanceUtil
            .setProcessName(process02, "zeroInputProcess" + new Random().nextInt());
        List<String> feed = new ArrayList<String>();
        feed.add(outputFeed);
        final ProcessMerlin processMerlin = new ProcessMerlin(process02);
        processMerlin.setProcessFeeds(feed, 0, 0, 1);
        process02 = processMerlin.toString();


        //submit and schedule both process
        logger.info("process: " + Util.prettyPrintXml(process01));
        logger.info("process: " + Util.prettyPrintXml(process02));


        logger.info("Wait till process goes into running ");

        //change feed location path
        outputFeed = Util.setFeedProperty(outputFeed, "queueName", "myQueue");

        logger.info("updated feed: " + Util.prettyPrintXml(outputFeed));

        //update feed first time
        prism.getFeedHelper().update(outputFeed, outputFeed);
    }


    /**
     * schedules a feed and dependent process. Process start and end are in past
     * Test for bug https://issues.apache.org/jira/browse/FALCON-500
     */
    @Test
    public void dependentProcessSucceeded()
        throws Exception {
        bundles[0].setProcessValidity("2014-06-01T04:00Z","2014-06-01T04:02Z");
        bundles[0].submitAndScheduleAllFeeds();
        bundles[0].submitAndScheduleProcess();

        InstanceUtil.waitTillInstancesAreCreated(cluster1, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS, bundles[0].getProcessName(),
            0, 0);
        InstanceUtil.waitForBundleToReachState(cluster1, bundles[0].getProcessName(),
            Job.Status.SUCCEEDED, 20);

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.addProperty("someProp","someVal");
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed.toString(), feed.toString()));
        //check for new feed bundle creation
        Assert.assertEquals(OozieUtil.getNumberOfBundle(prism, EntityType.FEED,
            feed.getName()),2);
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1, EntityType.PROCESS,
            bundles[0].getProcessName()),1);
    }

    /**
     * schedules a feed and dependent process. Update availability flag and check for process update
     * Test for bug https://issues.apache.org/jira/browse/FALCON-278
     */
    @Test
    public void updateAvailabilityFlag()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(30);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].submitAndScheduleAllFeeds();
        bundles[0].submitAndScheduleProcess();

        InstanceUtil.waitTillInstancesAreCreated(cluster1, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS, bundles[0].getProcessName(),
            0, 0);

        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.setAvailabilityFlag("mytestflag");
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed.toString(), feed.toString()));
        //check for new feed bundle creation
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1, EntityType.FEED,
            feed.getName()),2);
        Assert.assertEquals(OozieUtil.getNumberOfBundle(cluster1, EntityType.PROCESS,
            bundles[0].getProcessName()),2);
    }

}

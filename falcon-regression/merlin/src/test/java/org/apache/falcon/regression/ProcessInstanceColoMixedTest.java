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
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Process instance mixed colo tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceColoMixedTest extends BaseTestClass {

    private final String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceColoMixedTest";
    private final String feedPath = baseTestHDFSDir + "/feed0%d" + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private FileSystem cluster1FS = serverFS.get(0);
    private FileSystem cluster2FS = serverFS.get(1);
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceColoMixedTest.class);

    @BeforeClass(alwaysRun = true)
    public void prepareClusters() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(cluster1FS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        HadoopUtil.uploadDir(cluster2FS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {

        //get 3 unique bundles
        bundles[0] = BundleUtil.readELBundle();
        bundles[0].generateUniqueBundle();
        bundles[1] = BundleUtil.readELBundle();
        bundles[1].generateUniqueBundle();

        //generate bundles according to config files
        bundles[0] = new Bundle(bundles[0], cluster1);
        bundles[1] = new Bundle(bundles[1], cluster2);

        //set cluster colos
        bundles[0].setCLusterColo(cluster1.getClusterHelper().getColoName());
        LOGGER.info("cluster b1: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));
        bundles[1].setCLusterColo(cluster2.getClusterHelper().getColoName());
        LOGGER.info("cluster b2: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));

        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
        //submit 2 clusters
        Bundle.submitCluster(bundles[0], bundles[1]);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(timeOut = 12000000)
    public void mixed01C1sC2sC1eC2e() throws Exception {
        //ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
        //get 2 unique feeds
        String feed01 = bundles[0].getInputFeedFromBundle();
        String feed02 = bundles[1].getInputFeedFromBundle();
        String outputFeed = bundles[0].getOutputFeedFromBundle();
        //set source and target for the 2 feeds

        //set clusters to null;
        feed01 = FeedMerlin.fromString(feed01).clearFeedClusters().toString();
        feed02 = FeedMerlin.fromString(feed02).clearFeedClusters().toString();
        outputFeed = FeedMerlin.fromString(outputFeed).clearFeedClusters().toString();

        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01, String.format(feedPath, 1));
        feed02 = Util.setFeedPathValue(feed02, String.format(feedPath, 2));

        //generate data in both the colos ua1 and ua3
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.getTimeWrtSystemTime(-35), TimeUtil.getTimeWrtSystemTime(25), 1);

        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        HadoopUtil.flattenAndPutDataInFolder(cluster1FS, OSUtil.SINGLE_FILE, prefix, dataDates);

        prefix = InstanceUtil.getFeedPrefix(feed02);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.SINGLE_FILE, prefix, dataDates);

        String startTime = TimeUtil.getTimeWrtSystemTime(-70);

        //set clusters for feed01
        feed01 = FeedMerlin.fromString(feed01).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build()).toString();
        feed01 = FeedMerlin.fromString(feed01).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build()).toString();

        //set clusters for feed02
        feed02 = FeedMerlin.fromString(feed02).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build()).toString();
        feed02 = FeedMerlin.fromString(feed02).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build()).toString();

        //set clusters for output feed
        outputFeed = FeedMerlin.fromString(outputFeed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build()).toString();
        outputFeed = FeedMerlin.fromString(outputFeed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build()).toString();

        //submit and schedule feeds
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed01));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed02));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(outputFeed));

        String processStartTime = TimeUtil.getTimeWrtSystemTime(-16);
        // String processEndTime = InstanceUtil.getTimeWrtSystemTime(20);

        String process = bundles[0].getProcessData();
        process = ProcessMerlin.fromString(process).clearProcessCluster().toString();
        process = ProcessMerlin.fromString(process).addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withValidity(processStartTime, TimeUtil.addMinsToTime(processStartTime, 35))
                .build())
            .toString();
        process = ProcessMerlin.fromString(process).addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withValidity(TimeUtil.addMinsToTime(processStartTime, 16),
                    TimeUtil.addMinsToTime(processStartTime, 45))
                .build())
            .toString();
        process = InstanceUtil.addProcessInputFeed(process, Util.readEntityName(feed02),
            Util.readEntityName(feed02));

        //submit and schedule process
        prism.getProcessHelper().submitAndSchedule(process);

        LOGGER.info("Wait till process goes into running ");
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), Util.getProcessName(process), 1,
            Status.RUNNING, EntityType.PROCESS);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(1), Util.getProcessName(process), 1,
            Status.RUNNING, EntityType.PROCESS);

        final String processName = Util.readEntityName(bundles[0].getProcessData());
        InstancesResult responseInstance = prism.getProcessHelper().getProcessInstanceStatus(
            processName, "?start=" + processStartTime
            + "&end=" + TimeUtil.addMinsToTime(processStartTime, 45));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=" + TimeUtil.addMinsToTime(processStartTime, 37)
                + "&end=" + TimeUtil.addMinsToTime(processStartTime, 44));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=" + TimeUtil.addMinsToTime(processStartTime, 37)
                + "&end=" + TimeUtil.addMinsToTime(processStartTime, 44));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceResume(processName,
            "?start=" + processStartTime + "&end=" + TimeUtil.addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=" + TimeUtil.addMinsToTime(processStartTime, 16)
                + "&end=" + TimeUtil.addMinsToTime(processStartTime, 45));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = cluster1.getProcessHelper().getProcessInstanceKill(processName,
            "?start=" + processStartTime + "&end="+ TimeUtil.addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceRerun(processName,
            "?start=" + processStartTime + "&end=" + TimeUtil.addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);
    }
}


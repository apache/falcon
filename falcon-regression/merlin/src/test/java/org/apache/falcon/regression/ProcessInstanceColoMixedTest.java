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

    private final String baseTestHDFSDir = cleanAndGetTestDir();
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
        //generate bundles according to config files
        bundles[0] = new Bundle(BundleUtil.readELBundle(), cluster1);
        bundles[1] = new Bundle(BundleUtil.readELBundle(), cluster2);
        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);

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
        removeTestClassEntities();
    }

    @Test(timeOut = 12000000)
    public void mixed01C1sC2sC1eC2e() throws Exception {
        //ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
        //get 2 unique feeds
        FeedMerlin feed01 = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        FeedMerlin feed02 = new FeedMerlin(bundles[1].getInputFeedFromBundle());
        FeedMerlin outputFeed = new FeedMerlin(bundles[0].getOutputFeedFromBundle());
        //set source and target for the 2 feeds

        //set clusters to null;
        feed01.clearFeedClusters();
        feed02.clearFeedClusters();
        outputFeed.clearFeedClusters();

        //set new feed input data
        feed01.setFeedPathValue(String.format(feedPath, 1));
        feed02.setFeedPathValue(String.format(feedPath, 2));

        //generate data in both the colos ua1 and ua3
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.getTimeWrtSystemTime(-35), TimeUtil.getTimeWrtSystemTime(25), 1);

        String prefix = feed01.getFeedPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        HadoopUtil.flattenAndPutDataInFolder(cluster1FS, OSUtil.SINGLE_FILE, prefix, dataDates);

        prefix = feed02.getFeedPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.SINGLE_FILE, prefix, dataDates);

        String startTime = TimeUtil.getTimeWrtSystemTime(-70);

        //set clusters for feed01
        feed01.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());
        feed01.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build());

        //set clusters for feed02
        feed02.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build());
        feed02.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());

        //set clusters for output feed
        outputFeed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.SOURCE)
                .build());
        outputFeed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(10000)", ActionType.DELETE)
                .withValidity(startTime, "2099-01-01T00:00Z")
                .withClusterType(ClusterType.TARGET)
                .build());

        //submit and schedule feeds
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed01.toString()));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed02.toString()));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(outputFeed.toString()));

        String processStartTime = TimeUtil.getTimeWrtSystemTime(-16);
        // String processEndTime = InstanceUtil.getTimeWrtSystemTime(20);

        ProcessMerlin process = bundles[0].getProcessObject();
        process.clearProcessCluster();
        process.addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withValidity(processStartTime, TimeUtil.addMinsToTime(processStartTime, 35))
                .build());
        process.addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withValidity(TimeUtil.addMinsToTime(processStartTime, 16),
                    TimeUtil.addMinsToTime(processStartTime, 45))
                .build());
        process.addInputFeed(feed02.getName(), feed02.getName());

        //submit and schedule process
        prism.getProcessHelper().submitAndSchedule(process.toString());

        LOGGER.info("Wait till process goes into running ");
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), process.getName(), 1,
            Status.RUNNING, EntityType.PROCESS);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(1), process.getName(), 1,
            Status.RUNNING, EntityType.PROCESS);

        InstancesResult responseInstance = prism.getProcessHelper().getProcessInstanceStatus(process.getName(),
                "?start=" + processStartTime + "&end=" + TimeUtil.addMinsToTime(processStartTime, 45));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceSuspend(process.getName(),
            "?start=" + TimeUtil.addMinsToTime(processStartTime, 37)
                + "&end=" + TimeUtil.addMinsToTime(processStartTime, 44));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceStatus(process.getName(),
            "?start=" + TimeUtil.addMinsToTime(processStartTime, 37)
                + "&end=" + TimeUtil.addMinsToTime(processStartTime, 44));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceResume(process.getName(),
            "?start=" + processStartTime + "&end=" + TimeUtil.addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceStatus(process.getName(),
            "?start=" + TimeUtil.addMinsToTime(processStartTime, 16)
                + "&end=" + TimeUtil.addMinsToTime(processStartTime, 45));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = cluster1.getProcessHelper().getProcessInstanceKill(process.getName(),
            "?start=" + processStartTime + "&end="+ TimeUtil.addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper().getProcessInstanceRerun(process.getName(),
            "?start=" + processStartTime + "&end=" + TimeUtil.addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);
    }
}


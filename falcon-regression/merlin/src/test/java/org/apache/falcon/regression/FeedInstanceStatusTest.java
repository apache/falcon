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
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



/**
 * Feed instance status tests.
 */
@Test(groups = "embedded")
public class FeedInstanceStatusTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestDir + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private FileSystem cluster2FS = serverFS.get(1);
    private FileSystem cluster3FS = serverFS.get(2);
    private static final Logger LOGGER = Logger.getLogger(FeedInstanceStatusTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Goes through the whole feed replication workflow checking its instances status while.
     * submitting feed, scheduling it, performing different combinations of actions like
     * -submit, -resume, -kill, -rerun.
     */
    @Test(groups = {"multiCluster"})
    public void feedInstanceStatusRunning() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputPath);

        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(bundles[0].getClusters().get(0)));

        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(bundles[1].getClusters().get(0)));

        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(bundles[2].getClusters().get(0)));

        String feed = bundles[0].getDataSets().get(0);
        String feedName = Util.readEntityName(feed);
        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();
        String startTime = TimeUtil.getTimeWrtSystemTime(-50);
        final String startPlus20Min = TimeUtil.addMinsToTime(startTime, 20);
        final String startPlus40Min = TimeUtil.addMinsToTime(startTime, 40);
        final String startPlus100Min = TimeUtil.addMinsToTime(startTime, 100);

        feed = FeedMerlin.fromString(feed)
            .addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startTime, TimeUtil.addMinsToTime(startTime, 65))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("US/${cluster.colo}")
                .build())
            .toString();
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startPlus20Min,
                    TimeUtil.addMinsToTime(startTime, 85))
                .withClusterType(ClusterType.TARGET)
                .build())
            .toString();
        feed = FeedMerlin.fromString(feed).addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[2].getClusters().get(0)))
                .withRetention("hours(10)", ActionType.DELETE)
                .withValidity(startPlus40Min,
                    TimeUtil.addMinsToTime(startTime, 110))
                .withClusterType(ClusterType.SOURCE)
                .withPartition("UK/${cluster.colo}")
                .build())
            .toString();

        LOGGER.info("feed: " + Util.prettyPrintXml(feed));

        //status before submit
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus100Min
                + "&end=" + TimeUtil.addMinsToTime(startTime, 120));

        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed));
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + startPlus100Min);

        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed));

        // both replication instances
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + startPlus100Min);

        // single instance at -30
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus20Min);

        //single at -10
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus40Min);

        //single at 10
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus40Min);

        //single at 30
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus40Min);

        String postFix = "/US/" + cluster2.getClusterHelper().getColoName();
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        HadoopUtil.lateDataReplenish(cluster2FS, 80, 20, prefix, postFix);

        postFix = "/UK/" + cluster3.getClusterHelper().getColoName();
        prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        HadoopUtil.lateDataReplenish(cluster3FS, 80, 20, prefix, postFix);

        // both replication instances
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + startPlus100Min);

        // single instance at -30
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus20Min);

        //single at -10
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus40Min);

        //single at 10
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus40Min);

        //single at 30
        prism.getFeedHelper().getProcessInstanceStatus(feedName, "?start=" + startPlus40Min);

        LOGGER.info("Wait till feed goes into running ");

        //suspend instances -10
        prism.getFeedHelper().getProcessInstanceSuspend(feedName, "?start=" + startPlus40Min);
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startPlus20Min + "&end=" + startPlus40Min);

        //resuspend -10 and suspend -30 source specific
        prism.getFeedHelper().getProcessInstanceSuspend(feedName,
            "?start=" + startPlus20Min + "&end=" + startPlus40Min);
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startPlus20Min + "&end=" + startPlus40Min);

        //resume -10 and -30
        prism.getFeedHelper().getProcessInstanceResume(feedName,
            "?start=" + startPlus20Min + "&end=" + startPlus40Min);
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startPlus20Min + "&end=" + startPlus40Min);

        //get running instances
        prism.getFeedHelper().getRunningInstance(feedName);

        //rerun succeeded instance
        prism.getFeedHelper().getProcessInstanceRerun(feedName, "?start=" + startTime);
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + startPlus20Min);

        //kill instance
        prism.getFeedHelper().getProcessInstanceKill(feedName,
            "?start=" + TimeUtil.addMinsToTime(startTime, 44));
        prism.getFeedHelper().getProcessInstanceKill(feedName, "?start=" + startTime);

        //end time should be less than end of validity i.e startTime + 110
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 110));

        //rerun killed instance
        prism.getFeedHelper().getProcessInstanceRerun(feedName, "?start=" + startTime);
        prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 110));

        //kill feed
        prism.getFeedHelper().delete(feed);
        InstancesResult responseInstance = prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 110));

        LOGGER.info(responseInstance.getMessage());
    }
}

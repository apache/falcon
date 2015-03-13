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
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test delays in feed.
 */
@Test(groups = "distributed")
public class FeedDelayParallelTimeoutTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);

    private String baseTestDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestDir + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(FeedDelayParallelTimeoutTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);

        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(enabled = true, timeOut = 12000000)
    public void timeoutTest() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputPath);

        Bundle.submitCluster(bundles[0], bundles[1]);
        String feedOutput01 = bundles[0].getDataSets().get(0);
        org.apache.falcon.entity.v0.Frequency delay =
            new org.apache.falcon.entity.v0.Frequency(
                "hours(5)");

        feedOutput01 = FeedMerlin.fromString(feedOutput01).clearFeedClusters().toString();

        // uncomment below 2 line when falcon in sync with falcon

        // feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:10Z"),XmlUtil.createRetention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[1].getClusters().get(0)),ClusterType.SOURCE,"",delay,
        // feedInputPath);
        // feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:25Z"),XmlUtil.createRetention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[0].getClusters().get(0)),ClusterType.TARGET,"",delay,
        // feedOutputPath);

        //feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:10Z"),XmlUtil.createRetention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[1].getClusters().get(0)),ClusterType.SOURCE,"",
        // feedInputPath);
        //feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:25Z"),XmlUtil.createRetention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[0].getClusters().get(0)),ClusterType.TARGET,"",
        // feedOutputPath);

        feedOutput01 = Util.setFeedProperty(feedOutput01, "timeout", "minutes(35)");
        feedOutput01 = Util.setFeedProperty(feedOutput01, "parallel", "3");

        LOGGER.info("feedOutput01: " + Util.prettyPrintXml(feedOutput01));
        prism.getFeedHelper().submitAndSchedule(feedOutput01);
    }
}

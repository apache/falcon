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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

@Test(groups = "distributed")
public class FeedDelayParallelTimeoutTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);

    String baseTestDir = baseHDFSDir + "/FeedDelayParallelTimeoutTest";
    String feedInputPath = baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(FeedDelayParallelTimeoutTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);

        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(enabled = true, timeOut = 12000000)
    public void timeoutTest() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputPath);

        Bundle.submitCluster(bundles[0], bundles[1]);
        String feedOutput01 = bundles[0].getDataSets().get(0);
        org.apache.falcon.entity.v0.Frequency delay =
            new org.apache.falcon.entity.v0.Frequency(
                "hours(5)");

        feedOutput01 = InstanceUtil
            .setFeedCluster(feedOutput01,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);

        // uncomment below 2 line when falcon in sync with ivory

        //	feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:10Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[1].getClusters().get(0)),ClusterType.SOURCE,"",delay,
        // feedInputPath);
        //	feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:25Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[0].getClusters().get(0)),ClusterType.TARGET,"",delay,
        // feedOutputPath);

        //feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:10Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[1].getClusters().get(0)),ClusterType.SOURCE,"",
        // feedInputPath);
        //feedOutput01 = instanceUtil.setFeedCluster(feedOutput01,
        // XmlUtil.createValidity("2013-04-21T00:00Z",
        // "2099-10-01T12:25Z"),XmlUtil.createRtention("hours(15)",ActionType.DELETE),
        // Util.readClusterName(bundles[0].getClusters().get(0)),ClusterType.TARGET,"",
        // feedOutputPath);

        feedOutput01 = Util.setFeedProperty(feedOutput01, "timeout", "minutes(35)");
        feedOutput01 = Util.setFeedProperty(feedOutput01, "parallel", "3");

        logger.info("feedOutput01: " + Util.prettyPrintXml(feedOutput01));
        prism.getFeedHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedOutput01);
    }
}

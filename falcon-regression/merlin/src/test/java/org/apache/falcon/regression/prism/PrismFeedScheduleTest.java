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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

@Test(groups = "embedded")
public class PrismFeedScheduleTest extends BaseTestClass {

    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismFeedScheduleTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedScheduleTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws IOException {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readLateDataBundle();

        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Run feed. Suspend it. Run another feed on another cluster. Check that 1st feed is
     * suspended on 1st cluster and wnd feed is running on 2nd cluster and not criss cross.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2"})
    public void testFeedScheduleOn1ColoWhileAnotherColoHasSuspendedFeed()
        throws Exception {
        logger.info("cluster: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));
        logger.info("feed: " + Util.prettyPrintXml(bundles[0].getDataSets().get(0)));

        bundles[0].submitAndScheduleFeed();
        AssertUtil.assertSucceeded(prism.getFeedHelper()
            .suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        bundles[1].submitAndScheduleFeed();
        AssertUtil.checkStatus(cluster2OC, EntityType.FEED, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, EntityType.FEED, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }
}

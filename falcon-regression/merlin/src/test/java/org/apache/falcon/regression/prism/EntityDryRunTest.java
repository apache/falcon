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
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;

/**
 * test cases for https://issues.apache.org/jira/browse/FALCON-353.
 */
@Test(groups = "embedded")
public class EntityDryRunTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(EntityDryRunTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle b = BundleUtil.readELBundle();
        b = new Bundle(b, cluster);
        b.setInputFeedDataPath(feedInputPath);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * tries to submit process with invalid el exp.
     */
    @Test(groups = {"singleCluster"})
    public void testDryRunFailureScheduleProcess() throws Exception {
        bundles[0].setProcessProperty("EntityDryRunTestProp", "${coord:someEL(1)");
        bundles[0].submitProcess(true);
        ServiceResponse response = prism.getProcessHelper().schedule(bundles[0].getProcessData());
        validate(response,
                "Entity schedule failed for process: " + bundles[0].getProcessName());
    }

    /**
     * tries to update process with invalid EL exp.
     */
    @Test(groups = {"singleCluster"})
    public void testDryRunFailureUpdateProcess() throws Exception {
        bundles[0].setProcessValidity(TimeUtil.getTimeWrtSystemTime(-10),
            TimeUtil.getTimeWrtSystemTime(100));
        bundles[0].submitAndScheduleProcess();
        bundles[0].setProcessProperty("EntityDryRunTestProp", "${coord:someEL(1)");
        ServiceResponse response = prism.getProcessHelper().update(bundles[0].getProcessData(),
            bundles[0].getProcessData());
        validate(response,
            "The new entity (process) " + bundles[0].getProcessName() + " can't be scheduled");
        Assert.assertEquals(
            OozieUtil.getNumberOfBundle(clusterOC, EntityType.PROCESS, bundles[0].getProcessName()),
            1, "more than one bundle found after failed update request");
    }

    /**
     * tries to submit feed with invalied EL exp.
     */
    @Test(groups = {"singleCluster"})
    public void testDryRunFailureScheduleFeed() throws Exception {
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feed.setFeedProperty("EntityDryRunTestProp", "${coord:someEL(1)");
        bundles[0].submitClusters(prism);
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed.toString());
        validate(response,
                "Entity schedule failed for feed: " + bundles[0].getInputFeedNameFromBundle());
    }

    /**
     * tries to update feed with invalid el exp.
     */
    @Test(groups = {"singleCluster"})
    public void testDryRunFailureUpdateFeed() throws Exception {
        bundles[0].submitClusters(prism);
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        ServiceResponse response = prism.getFeedHelper().submitAndSchedule(feed.toString());
        AssertUtil.assertSucceeded(response);
        feed.setFeedProperty("EntityDryRunTestProp", "${coord:someEL(1)");
        response = prism.getFeedHelper().update(feed.toString(), feed.toString());
        validate(response, "The new entity (feed) " + bundles[0].getInputFeedNameFromBundle()
            + " can't be scheduled");
        Assert.assertEquals(
            OozieUtil.getNumberOfBundle(clusterOC, EntityType.FEED, feed.getName()), 1,
            "more than one bundle found after failed update request");
    }

    private void validate(ServiceResponse response, String message) throws JAXBException {
        AssertUtil.assertFailed(response);
        Assert.assertTrue(response.getMessage().contains(message),
            "Correct response was not present in process / feed schedule");
    }
}

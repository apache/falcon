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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test for https://issues.apache.org/jira/browse/FALCON-761.
 */
@Test(groups = "embedded", timeOut = 900000)
public class FeedInstanceListingTest extends BaseTestClass{
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestDir + "/output-data" + MINUTE_DATE_PATTERN;
    private String processName;

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);

    private static final Logger LOGGER = Logger.getLogger(FeedInstanceListingTest.class);

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setInputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        processName = bundles[0].getProcessName();
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException{
        cleanTestsDirs();
        removeTestClassEntities();
    }

    /**
     * Test when all data is available for all instances.
     */
    @Test
    public void testFeedListingWhenAllAvailable() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        List<List<String>> missingDependencies = OozieUtil.createMissingDependencies(cluster,
                EntityType.PROCESS, processName, 0);
        List<String> missingDependencyLastInstance = missingDependencies.get(missingDependencies.size()-1);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE, missingDependencyLastInstance);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        FeedInstanceResult r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 0, 0, 0, 5);
    }

   /**
    *Test when only empty directories exist for all instances.
    */
    @Test
    public void testFeedListingWhenAllEmpty() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        FeedInstanceResult r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 0, 5, 0, 0);
    }

   /**
    * Test when no data is present for any instance.
    */
    @Test
    public void testFeedListingWhenAllMissing() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        FeedInstanceResult r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 5, 0, 0, 0);
    }

   /**
    * Initially no availability flag is set for the feed. And data is created, so instance status is available.
    * Then, set the availability flag and update the feed. The instance status should change to partial.
    */
    @Test
    public void testFeedListingAfterFeedAvailabilityFlagUpdate() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        List<List<String>> missingDependencies = OozieUtil.createMissingDependencies(cluster,
                EntityType.PROCESS, processName, 0);
        List<String> missingDependencyLastInstance = missingDependencies.get(missingDependencies.size()-1);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE, missingDependencyLastInstance);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        FeedInstanceResult r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 0, 0, 0, 5);
        String inputFeed = bundles[0].getInputFeedFromBundle();
        bundles[0].setInputFeedAvailabilityFlag("_SUCCESS");
        ServiceResponse serviceResponse = prism.getFeedHelper().update(inputFeed, bundles[0].getInputFeedFromBundle(),
                TimeUtil.getTimeWrtSystemTime(0), null);
        AssertUtil.assertSucceeded(serviceResponse);
        //Since we have not created availability flag on HDFS, the feed instance status should be partial
        r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 0, 0, 5, 0);
    }

   /**
    * Data is created for the feed, so instance status is available.
    * Then, change the data path and update the feed. The instance status should change to partial.
    */
    @Test
    public void testFeedListingAfterFeedDataPathUpdate() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        List<List<String>> missingDependencies = OozieUtil.createMissingDependencies(cluster,
                EntityType.PROCESS, processName, 0);
        List<String> missingDependencyLastInstance = missingDependencies.get(missingDependencies.size()-1);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE, missingDependencyLastInstance);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        FeedInstanceResult r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 0, 0, 0, 5);
        String inputFeed = bundles[0].getInputFeedFromBundle();
        bundles[0].setInputFeedDataPath(baseTestDir + "/inputNew" + MINUTE_DATE_PATTERN);
        ServiceResponse serviceResponse = prism.getFeedHelper().update(inputFeed, bundles[0].getInputFeedFromBundle(),
                TimeUtil.getTimeWrtSystemTime(0), null);
        AssertUtil.assertSucceeded(serviceResponse);
        //Since we have not created directories for new path, the feed instance status should be missing
        r = prism.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 5, 0, 0, 0);
    }

   /**
    * Submit the feeds on prism, and request for instance status on server. Request should succeed.
    */
    @Test
    public void testFeedListingFeedSubmitOnPrismRequestOnServer() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        FeedInstanceResult r = cluster.getFeedHelper()
                .getFeedInstanceListing(Util.readEntityName(bundles[0].getDataSets().get(0)),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        validateResponse(r, 5, 5, 0, 0, 0);
    }

    /**
     * Checks that actual number of instances with different statuses are equal to expected number
     * of instances with matching statuses.
     *
     * @param instancesResult kind of response from API which should contain information about
     *                        instances <p/>
     *                        All parameters below reflect number of expected instances with some
     *                        kind of status.
     * @param totalCount      total number of instances.
     * @param missingCount    number of running instances.
     * @param emptyCount  number of suspended instance.
     * @param partialCount    number of waiting instance.
     * @param availableCount     number of killed instance.
     */
    private void validateResponse(FeedInstanceResult instancesResult, int totalCount,
                                 int missingCount, int emptyCount, int partialCount, int availableCount) {
        FeedInstanceResult.Instance[] instances = instancesResult.getInstances();
        LOGGER.info("instances: " + Arrays.toString(instances));
        Assert.assertNotNull(instances, "instances should be not null");
        Assert.assertEquals(instances.length, totalCount, "Total Instances");
        List<String> statuses = new ArrayList<>();
        for (FeedInstanceResult.Instance instance : instances) {
            Assert.assertNotNull(instance.getCluster());
            Assert.assertNotNull(instance.getInstance());
            Assert.assertNotNull(instance.getStatus());
            Assert.assertNotNull(instance.getUri());
            Assert.assertNotNull(instance.getCreationTime());
            Assert.assertNotNull(instance.getSize());
            final String status = instance.getStatus();
            LOGGER.info("status: "+ status + ", instance: " + instance.getInstance());
            statuses.add(status);
        }

        Assert.assertEquals(Collections.frequency(statuses, "MISSING"),
                missingCount, "Missing Instances");
        Assert.assertEquals(Collections.frequency(statuses, "EMPTY"),
                emptyCount, "Empty Instances");
        Assert.assertEquals(Collections.frequency(statuses, "PARTIAL"),
                partialCount, "Partial Instances");
        Assert.assertEquals(Collections.frequency(statuses, "AVAILABLE"),
                availableCount, "Available Instances");
    }
}

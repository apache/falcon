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
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.apache.falcon.entity.v0.feed.Lifecycle;
import org.apache.falcon.entity.v0.feed.Properties;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.feed.RetentionStage;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.FreqType;
import org.apache.falcon.regression.core.enumsAndConstants.RetentionUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.JmsMessageConsumer;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Test with retention.
 */
@Test(groups = { "distributed", "embedded" })
public class RetentionTest extends BaseTestClass {
    private static final String TEST_FOLDERS = "/testFolders/";
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String testHDFSDir = baseTestHDFSDir + TEST_FOLDERS;
    private static final Logger LOGGER = Logger.getLogger(RetentionTest.class);
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private static final int[] GAPS = new int[]{2, 4, 5, 1};

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readRetentionBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].setInputFeedDataPath(testHDFSDir);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeTestClassEntities();
    }

    /**
     * Particular test case for https://issues.apache.org/jira/browse/FALCON-321.
     * @throws Exception
     */
    @Test
    public void testRetentionWithEmptyDirectories() throws Exception {
        testRetention(24, RetentionUnit.HOURS, true, FreqType.DAILY, false);
    }

    /**
     * Tests retention with different parameters. Validates its results based on expected and
     * actual retained data.
     *
     * @param retentionPeriod period for which data should be retained
     * @param retentionUnit type of retention limit attribute
     * @param gaps defines gaps within list of data folders
     * @param freqType feed type
     * @param withData should folders be filled with data or not
     * @throws Exception
     */
    @Test(groups = {"0.1", "0.2", "prism"}, dataProvider = "betterDP", priority = -1)
    public void testRetention(final int retentionPeriod, final RetentionUnit retentionUnit,
            final boolean gaps, final FreqType freqType, final boolean withData) throws Exception {
        bundles[0].setInputFeedDataPath(testHDFSDir + freqType.getPathValue());
        final FeedMerlin feedObject = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedObject.setRetentionValue(retentionUnit.getValue() + "(" + retentionPeriod + ")");

        final ServiceResponse response = prism.getFeedHelper().submitEntity(feedObject.toString());
        if (retentionPeriod > 0) {
            AssertUtil.assertSucceeded(response);

            replenishData(freqType, gaps, withData);

            commonDataRetentionWorkflow(feedObject.toString(), freqType, retentionUnit,
                    retentionPeriod);
        } else {
            AssertUtil.assertFailed(response);
        }
    }

    /**
     * Generates folders based on proposed periodicity and then fills them with data if required.
     *
     * @param freqType feed retention limit type
     * @param gap defines what amount of units should be skipped
     * @param withData should folders be filled with data or not
     * @throws Exception
     */
    private void replenishData(FreqType freqType, boolean gap, boolean withData) throws Exception {
        int skip = 1;
        if (gap) {
            skip = GAPS[new Random().nextInt(GAPS.length)];
        }
        final DateTime today = new DateTime(DateTimeZone.UTC);
        final List<DateTime> times = TimeUtil.getDatesOnEitherSide(
                freqType.addTime(today, -36), freqType.addTime(today, -1), skip, freqType);
        final List<String> dataDates = TimeUtil.convertDatesToString(times, freqType.getFormatter());
        LOGGER.info("dataDates = " + dataDates);
        dataDates.add(HadoopUtil.SOMETHING_RANDOM);
        if (withData) {
            HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE, testHDFSDir, dataDates);
        } else {
            HadoopUtil.createFolders(clusterFS, testHDFSDir, dataDates);
        }
    }

    /**
     * Schedules feed and waits till retention succeeds. Makes validation of data which was removed
     * and which was retained.
     *
     * @param feed analyzed retention feed
     * @param freqType feed type
     * @param retentionUnit type of retention limit attribute
     * @param retentionPeriod period for which data should be retained
     * @throws OozieClientException
     * @throws IOException
     * @throws URISyntaxException
     * @throws AuthenticationException
     * @throws JMSException
     */
    private void commonDataRetentionWorkflow(String feed, FreqType freqType,
            RetentionUnit retentionUnit, int retentionPeriod) throws OozieClientException,
            IOException, URISyntaxException, AuthenticationException, JMSException,
            InterruptedException {
        //get Data created in the cluster
        List<String> initialData = Util.getHadoopDataFromDir(clusterFS, feed, testHDFSDir);

        cluster.getFeedHelper().schedule(feed);
        LOGGER.info(cluster.getClusterHelper().getActiveMQ());
        final String feedName = Util.readEntityName(feed);
        LOGGER.info(feedName);
        JmsMessageConsumer messageConsumer = new JmsMessageConsumer("FALCON." + feedName,
                cluster.getClusterHelper().getActiveMQ());
        messageConsumer.start();
        String bundleId = OozieUtil.getBundles(clusterOC, feedName, EntityType.FEED).get(0);

        List<String> workflows = OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);

        //get current time minus duration of last status check - to get actual time when eviction has started
        final DateTime currentTime = new DateTime(DateTimeZone.UTC).minus(10000);
        LOGGER.info("Current time is " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentTime.toDate()));
        LOGGER.info("workflows: " + workflows);
        messageConsumer.interrupt();
        Util.printMessageData(messageConsumer);

        //now look for cluster data
        List<String> finalData = Util.getHadoopDataFromDir(clusterFS, feed, testHDFSDir);

        //now see if retention value was matched to as expected
        List<String> expectedOutput = filterDataOnRetention(initialData, currentTime, retentionUnit,
                retentionPeriod, freqType);
        LOGGER.info("initialData = " + initialData);
        LOGGER.info("finalData = " + finalData);
        LOGGER.info("expectedOutput = " + expectedOutput);

        final List<String> missingData = new ArrayList<>(initialData);
        missingData.removeAll(expectedOutput);
        validateDataFromFeedQueue(feedName, messageConsumer.getReceivedMessages(), missingData);
        Assert.assertEquals(finalData.size(), expectedOutput.size(),
                "Expected and actual sizes of retained data are different! Please check.");

        Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
                expectedOutput.toArray(new String[expectedOutput.size()])));

        //check that root directory exists
        Assert.assertTrue(clusterFS.exists(new Path(testHDFSDir)), "Base data directory should be present.");
    }

    /**
     * Makes validation based on comparison of data which is expected to be removed with data
     * mentioned in messages from ActiveMQ.
     *
     * @param feedName feed name
     * @param messages messages from ActiveMQ
     * @param missingData data which is expected to be removed after retention succeeded
     * @throws OozieClientException
     * @throws JMSException
     */
    private void validateDataFromFeedQueue(String feedName, List<MapMessage> messages,
            List<String> missingData) throws OozieClientException, JMSException {
        //just verify that each element in queue is same as deleted data!
        List<String> workflowIds = OozieUtil.getWorkflowJobs(clusterOC,
                OozieUtil.getBundles(clusterOC, feedName, EntityType.FEED).get(0));

        //create queue data folderList:
        List<String> deletedFolders = new ArrayList<>();
        for (MapMessage message : messages) {
            if (message != null) {
                Assert.assertEquals(message.getString("entityName"), feedName);
                String[] splitData = message.getString("feedInstancePaths").split(TEST_FOLDERS);
                deletedFolders.add(splitData[splitData.length - 1]);
                Assert.assertEquals(message.getString("operation"), "DELETE");
                Assert.assertEquals(message.getString("workflowId"), workflowIds.get(0));

                //verify other data also
                Assert.assertEquals(message.getJMSDestination().toString(), "topic://FALCON." + feedName);
                Assert.assertEquals(message.getString("status"), "SUCCEEDED");
            }
        }
        Assert.assertEquals(deletedFolders.size(), missingData.size(),
                "Output size is different than expected!");
        Assert.assertTrue(Arrays.deepEquals(missingData.toArray(new String[missingData.size()]),
                        deletedFolders.toArray(new String[deletedFolders.size()])),
                "The missing data and message for delete operation don't correspond");
    }

    /**
     * Evaluates amount of data which is expected to be retained.
     *
     * @param inputData initial data on cluster
     * @param currentTime current date
     * @param retentionUnit type of retention limit attribute
     * @param retentionPeriod period for which data should be retained
     * @param freqType feed type
     * @return list of data folders which are expected to be present on cluster
     */
    private List<String> filterDataOnRetention(List<String> inputData, DateTime currentTime,
            RetentionUnit retentionUnit, int retentionPeriod, FreqType freqType) {
        final List<String> finalData = new ArrayList<>();
        //end date is today's date
        final String startLimit = freqType.getFormatter().print(
                retentionUnit.minusTime(currentTime, retentionPeriod));

        //now to actually check!
        for (String testDate : inputData) {
            if (testDate.equals(HadoopUtil.SOMETHING_RANDOM)
                    || testDate.compareTo(startLimit) > 0) {
                finalData.add(testDate);
            }
        }
        return finalData;
    }

    /**
     * Provides different sets of parameters for retention workflow.
     */
    @DataProvider(name = "betterDP")
    public Object[][] getTestData() {
        // a negative value like -4 should be covered in validation scenarios.
        Integer[] retentionPeriods = new Integer[]{0, 10080, 60, 8, 24};
        RetentionUnit[] retentionUnits = new RetentionUnit[]{
            RetentionUnit.HOURS,
            RetentionUnit.DAYS,
        }; // "minutes","hours", "days",
        Boolean[] gaps = new Boolean[]{false, true};
        FreqType[] freqTypes = new FreqType[]{FreqType.DAILY, FreqType.YEARLY, FreqType.MONTHLY};
        final Boolean[] withData = new Boolean[]{true};

        return MatrixUtil.crossProduct(retentionPeriods, retentionUnits, gaps, freqTypes, withData);
    }

    /**
     * Submit a feed having minutely lifecycle frequency.
     * It would fail since lifecycle retention frequency has to be >= 1 hour.
     */
    @Test
    public void testTooFrequentRetentionLifecycleStage() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 120);

        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(new Frequency("1", Frequency.TimeUnit.minutes));

        FreqType freqType = FreqType.MINUTELY;
        Frequency retentionPeriodGlobal=new Frequency("30", Frequency.TimeUnit.minutes);
        Frequency retentionFrequencyGlobal=new Frequency("15", Frequency.TimeUnit.minutes);

        bundles[0].setInputFeedDataPath(testHDFSDir + freqType.getPathValue());
        final FeedMerlin feedObject = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedObject.setLateArrival(lateArrival);
        feedObject.setValidity(startTime, endTime);
        feedObject.setFrequency(new Frequency("minutes(10)"));
        feedObject.setRetentionValue("minutes(10)");

        feedObject.setLifecycle(createLifecycle(retentionPeriodGlobal, retentionFrequencyGlobal,
                "", "", true));

        final ServiceResponse response = prism.getFeedHelper().submitEntity(feedObject.toString());
        AssertUtil.assertFailedWithMessage(response, "Feed Retention can not be more frequent than hours(1)");

    }


    /**
     * Submits and schedules a feed with lifecycle tag at cluster and global level.
     * Responses are checked and retention is validated correspondingly.
     * Uses lifecycleDPFail dataProvider to handle possible scenarios.
     *
     * @param globalLevel : boolean (whether lifecycle is enabled for global level or not)
     * @param clusterLevel : boolean (whether lifecycle is enabled for cluster level or not)
     */
    @Test(dataProvider = "lifecycleDPFail")
    public void clusterGlobalNoRetentionStageTest(boolean globalLevel, boolean clusterLevel) throws Exception {

        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 120);

        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(new Frequency("1", Frequency.TimeUnit.hours));

        final FeedMerlin feedObject = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedObject.setLateArrival(lateArrival);
        feedObject.setValidity(startTime, endTime);

        if (globalLevel) {
            feedObject.setLifecycle(new Lifecycle());
        }
        if (clusterLevel) {
            feedObject.getClusters().getClusters().get(0).setLifecycle(new Lifecycle());
        }

        final ServiceResponse response = prism.getFeedHelper().submitEntity(feedObject.toString());
        AssertUtil.assertFailedWithMessage(response, "Retention is a mandatory stage, didn't find it for cluster");

    }

    @DataProvider(name = "lifecycleDPFail")
    public Object[][] getLifecycleFail() {
        return new Object[][]{
            {true, true}, // cluster/global : No retention stage. Should fail.
            {true, false}, // global : no retention stage. Should fail.
            {false, true}, // cluster : no retention stage.Should fail.
        };
    }

    /**
     * Submits and schedules a feed with lifecycle tag at cluster and global level.
     * Responses are checked and retention is validated correspondingly.
     * Uses getLifecycleWithGlobalStage dataProvider to handle possible scenarios.
     *
     * @param globalLevel : boolean (whether lifecycle is enabled for global level or not)
     * @param clusterLevel : boolean (whether lifecycle is enabled for cluster level or not)
     */
    @Test(dataProvider = "getLifecycleWithGlobalStage")
    public void retentionStageFromGlobalTest(boolean globalLevel, boolean clusterLevel) throws Exception {

        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 120);

        FreqType freqType = FreqType.HOURLY;
        Frequency retentionPeriodGlobal=new Frequency("2", Frequency.TimeUnit.hours);
        Frequency retentionFrequencyGlobal=new Frequency("1", Frequency.TimeUnit.hours);

        String priorityGlobal = "HIGH";
        String queue = "default";

        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(new Frequency("1", Frequency.TimeUnit.hours));

        bundles[0].setInputFeedDataPath(testHDFSDir + freqType.getPathValue());
        final FeedMerlin feedObject = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedObject.setLateArrival(lateArrival);
        feedObject.setValidity(startTime, endTime);

        if (globalLevel) {
            feedObject.setLifecycle(createLifecycle(retentionPeriodGlobal, retentionFrequencyGlobal,
                    priorityGlobal, queue, true));
        }

        if (clusterLevel) {
            feedObject.getClusters().getClusters().get(0).setLifecycle(new Lifecycle());
        }

        replenishData(freqType, false, false);

        final ServiceResponse response = prism.getFeedHelper().submitEntity(feedObject.toString());

        AssertUtil.assertSucceeded(response);
        commonDataRetentionWorkflow(feedObject.toString(), freqType, RetentionUnit.HOURS,
                retentionPeriodGlobal.getFrequencyAsInt());
        validateFrequency(feedObject.getName(), retentionFrequencyGlobal.getFrequencyAsInt()*60);
        validatePriorityAndQueue(feedObject.getName(), priorityGlobal, queue);

    }

    @DataProvider(name = "getLifecycleWithGlobalStage")
    public Object[][] getLifecycleWithGlobalStage() {
        return new Object[][]{
            {true, false}, // Global level lifecycle. Should pass.
            {true, true}, // Cluster level : no retention stage - (pick from global). Should pass.

        };
    }

    /**
     * Submits and schedules a feed with lifecycle tag at cluster and global level.
     * Responses are checked and retention is validated correspondingly.
     * Uses getLifecycleWithClusterStage dataProvider to handle possible scenarios.
     *
     * @param globalLevel : boolean (whether lifecycle is enabled for global level or not)
     * @param globalWithStage : boolean (whether global lifecycle has retention stage defined or not)
     */
    @Test(dataProvider = "getLifecycleWithClusterStage")
    public void retentionStageFromClusterTest(boolean globalLevel, boolean globalWithStage) throws Exception {

        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 120);

        FreqType freqType = FreqType.HOURLY;
        Frequency retentionPeriodGlobal=new Frequency("2", Frequency.TimeUnit.hours);
        Frequency retentionFrequencyGlobal=new Frequency("1", Frequency.TimeUnit.hours);

        Frequency retentionPeriodCluster=new Frequency("4", Frequency.TimeUnit.hours);
        Frequency retentionFrequencyCluster=new Frequency("3", Frequency.TimeUnit.hours);

        String priorityGlobal = "HIGH";
        String priorityCluster = "LOW";
        String queue = "default";

        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(new Frequency("1", Frequency.TimeUnit.hours));

        bundles[0].setInputFeedDataPath(testHDFSDir + freqType.getPathValue());
        final FeedMerlin feedObject = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedObject.setLateArrival(lateArrival);
        feedObject.setValidity(startTime, endTime);

        if (globalLevel) {
            feedObject.setLifecycle(createLifecycle(retentionPeriodGlobal, retentionFrequencyGlobal,
                    priorityGlobal, queue, globalWithStage));
        }

        feedObject.getClusters().getClusters().get(0).setLifecycle(createLifecycle(retentionPeriodCluster,
                retentionFrequencyCluster, priorityCluster, queue, true));

        replenishData(freqType, false, false);

        final ServiceResponse response = prism.getFeedHelper().submitEntity(feedObject.toString());

        AssertUtil.assertSucceeded(response);
        commonDataRetentionWorkflow(feedObject.toString(), freqType, RetentionUnit.HOURS,
                retentionPeriodCluster.getFrequencyAsInt());
        validateFrequency(feedObject.getName(), retentionFrequencyCluster.getFrequencyAsInt()*60);
        validatePriorityAndQueue(feedObject.getName(), priorityCluster, queue);

    }

    @DataProvider(name = "getLifecycleWithClusterStage")
    public Object[][] getLifecycleWithClusterStage() {
        return new Object[][]{

            {true, true}, // Cluster level lifecylce. Should pass.
            {false, false}, // Cluster level with no global level lifecylce. Should pass.
            {true, false}, // Cluster level with empty global level lifecycle.Should pass.

        };
    }

    /**
     * Method to create lifecycle tag to be used by feed for lifecycle retention.
     * @param retentionPeriod : lifecycle retention period.
     * @param retentionFrequency : lifecycle retention frequency.
     * @param priority : lifecycle retention priority.
     * @param queue : lifecycle retention queue.
     */
    private Lifecycle createLifecycle(Frequency retentionPeriod, Frequency retentionFrequency,
            String priority, String queue, boolean stage) {
        Lifecycle lifecycle = new Lifecycle();
        if (stage) {
            String limitPropertyName = "retention.policy.agebaseddelete.limit";
            Property property = new Property();
            property.setName(limitPropertyName);
            property.setValue(retentionPeriod.getTimeUnit() + "(" + retentionPeriod.getFrequencyAsInt() + ")");

            Properties properties = new Properties();
            properties.getProperties().add(property);
            RetentionStage retentionStage = new RetentionStage();
            retentionStage.setFrequency(new Frequency(retentionFrequency.getTimeUnit()
                    + "(" + retentionFrequency.getFrequencyAsInt() + ")"));

            if (!priority.isEmpty()) {
                retentionStage.setPriority(priority);
            }
            if (!queue.isEmpty()) {
                retentionStage.setQueue(queue);
            }
            retentionStage.setProperties(properties);
            lifecycle.setRetentionStage(retentionStage);
        }
        return lifecycle;
    }

    /**
     * Validates feed retention frequency with expected frequency.
     * @param feedName : feed name.
     * @param frequency : expected frequency.
     */
    private void validateFrequency(String feedName, int frequency)
        throws OozieClientException, JMSException, JSONException {
        List<CoordinatorJob> coordJobs = OozieUtil.getBundleCoordinators(clusterOC,
                OozieUtil.getBundles(clusterOC, feedName, EntityType.FEED).get(0));
        CoordinatorJob coordJobInfo = clusterOC.getCoordJobInfo(coordJobs.get(0).getId());
        Assert.assertEquals(coordJobInfo.getFrequency(), String.valueOf(frequency),
                "Invalid retention frequency : " + frequency);
    }

    /**
     * Validates feed retention queue and priority with expected values.
     * @param feedName : feed name.
     * @param expectedPriority : expected priority.
     * @param expectedQueue : expected queue.
     */
    private void validatePriorityAndQueue(String feedName, String expectedPriority, String expectedQueue)
        throws OozieClientException, JMSException, JSONException {

        Configuration configuration = OozieUtil.getRetentionConfiguration(clusterOC,
                OozieUtil.getBundles(clusterOC, feedName, EntityType.FEED).get(0));
        String priority = configuration.get("jobPriority");
        String queue = configuration.get("queueName");
        Assert.assertEquals(priority, expectedPriority, "Priority should be : " + expectedPriority);
        Assert.assertEquals(queue, expectedQueue, "Queue should be : " + expectedQueue);
    }
}

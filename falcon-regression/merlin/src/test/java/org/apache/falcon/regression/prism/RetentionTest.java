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
import org.apache.falcon.regression.core.enumsAndConstants.FeedType;
import org.apache.falcon.regression.core.enumsAndConstants.RetentionUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.JmsMessageConsumer;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.MathUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Test(groups = "embedded")
public class RetentionTest extends BaseTestClass {
    private static final String TEST_FOLDERS = "testFolders/";
    String baseTestHDFSDir = baseHDFSDir + "/RetentionTest/";
    String testHDFSDir = baseTestHDFSDir + TEST_FOLDERS;
    private static final Logger logger = Logger.getLogger(RetentionTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readRetentionBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].setInputFeedDataPath(testHDFSDir);
        bundles[0].generateUniqueBundle();
        bundles[0].submitClusters(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test
    public void testRetentionWithEmptyDirectories() throws Exception {
        // test for https://issues.apache.org/jira/browse/FALCON-321
        testRetention(24, RetentionUnit.HOURS, true, FeedType.DAILY, false);
    }

    @Test(groups = {"0.1", "0.2", "prism"}, dataProvider = "betterDP", priority = -1)
    public void testRetention(final int retentionPeriod, final RetentionUnit retentionUnit,
        final boolean gaps, final FeedType feedType, final boolean withData) throws Exception {
        bundles[0].setInputFeedDataPath(testHDFSDir + feedType.getPathValue());
        final FeedMerlin feedObject = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedObject.setRetentionValue(retentionUnit.getValue() + "(" + retentionPeriod + ")");

        final ServiceResponse response = prism.getFeedHelper()
            .submitEntity(URLS.SUBMIT_URL, feedObject.toString());
        if (retentionPeriod > 0) {
            AssertUtil.assertSucceeded(response);

            replenishData(feedType, gaps, withData);

            commonDataRetentionWorkflow(feedObject.toString(), feedType, retentionUnit,
                retentionPeriod);
        } else {
            AssertUtil.assertFailed(response);
        }
    }

    private void replenishData(FeedType feedType, boolean gap, boolean withData) throws Exception {
        int skip = 1;
        if (gap) {
            skip = gaps[new Random().nextInt(gaps.length)];
        }

        final DateTime today = new DateTime(DateTimeZone.UTC);
        final List<DateTime> times = TimeUtil.getDatesOnEitherSide(
            feedType.addTime(today, -36), feedType.addTime(today, 36), skip, feedType);
        final List<String> dataDates = TimeUtil.convertDatesToString(times, feedType.getFormatter());
        logger.info("dataDates = " + dataDates);

        HadoopUtil.replenishData(clusterFS, testHDFSDir, dataDates, withData);
    }

    private void commonDataRetentionWorkflow(String feed, FeedType feedType,
        RetentionUnit retentionUnit, int retentionPeriod) throws OozieClientException,
        IOException, URISyntaxException, AuthenticationException, JMSException {
        //get Data created in the cluster
        List<String> initialData = Util.getHadoopDataFromDir(clusterFS, feed, testHDFSDir);

        cluster.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        logger.info(cluster.getClusterHelper().getActiveMQ());
        final String feedName = Util.readEntityName(feed);
        logger.info(feedName);
        JmsMessageConsumer messageConsumer = new JmsMessageConsumer("FALCON." + feedName,
                cluster.getClusterHelper().getActiveMQ());
        messageConsumer.start();

        final DateTime currentTime = new DateTime(DateTimeZone.UTC);
        String bundleId = OozieUtil.getBundles(clusterOC, feedName, EntityType.FEED).get(0);

        List<String> workflows = OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);
        logger.info("workflows: " + workflows);

        messageConsumer.interrupt();
        Util.printMessageData(messageConsumer);
        //now look for cluster data
        List<String> finalData = Util.getHadoopDataFromDir(clusterFS, feed, testHDFSDir);

        //now see if retention value was matched to as expected
        List<String> expectedOutput = filterDataOnRetention(initialData, currentTime, retentionUnit,
            retentionPeriod, feedType);

        logger.info("initialData = " + initialData);
        logger.info("finalData = " + finalData);
        logger.info("expectedOutput = " + expectedOutput);

        final List<String> missingData = new ArrayList<String>(initialData);
        missingData.removeAll(expectedOutput);
        validateDataFromFeedQueue(feedName, messageConsumer.getReceivedMessages(), missingData);

        Assert.assertEquals(finalData.size(), expectedOutput.size(),
            "sizes of outputs are different! please check");

        Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
            expectedOutput.toArray(new String[expectedOutput.size()])));
    }

    private void validateDataFromFeedQueue(String feedName, List<MapMessage> messages,
        List<String> missingData) throws OozieClientException, JMSException {
        //just verify that each element in queue is same as deleted data!
        List<String> workflowIds = OozieUtil.getWorkflowJobs(cluster,
                OozieUtil.getBundles(clusterOC, feedName, EntityType.FEED).get(0));

        //create queuedata folderList:
        List<String> deletedFolders = new ArrayList<String>();

        for (MapMessage message : messages) {
            if (message != null) {
                Assert.assertEquals(message.getString("entityName"), feedName);
                String[] splitData = message.getString("feedInstancePaths").split(TEST_FOLDERS);
                deletedFolders.add(splitData[splitData.length - 1]);
                Assert.assertEquals(message.getString("operation"), "DELETE");
                Assert.assertEquals(message.getString("workflowId"), workflowIds.get(0));

                //verify other data also
                Assert.assertEquals(message.getString("topicName"), "FALCON." + feedName);
                Assert.assertEquals(message.getString("brokerImplClass"),
                    "org.apache.activemq.ActiveMQConnectionFactory");
                Assert.assertEquals(message.getString("status"), "SUCCEEDED");
                Assert.assertEquals(message.getString("brokerUrl"),
                    cluster.getFeedHelper().getActiveMQ());
            }
        }

        Assert.assertEquals(deletedFolders.size(), missingData.size(),
            "Output size is different than expected!");
        Assert.assertTrue(Arrays.deepEquals(missingData.toArray(new String[missingData.size()]),
            deletedFolders.toArray(new String[deletedFolders.size()])),
            "The missing data and message for delete operation don't correspond");
    }

    private List<String> filterDataOnRetention(List<String> inputData, DateTime currentTime,
        RetentionUnit retentionUnit, int retentionPeriod, FeedType feedType) {
        final List<String> finalData = new ArrayList<String>();
        //end date is today's date
        final String startLimit = feedType.getFormatter().print(
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

    final static int[] gaps = new int[]{2, 4, 5, 1};

    @DataProvider(name = "betterDP")
    public Object[][] getTestData(Method m) {
        // a negative value like -4 should be covered in validation scenarios.
        Integer[] retentionPeriods = new Integer[]{0, 10080, 60, 8, 24};
        RetentionUnit[] retentionUnits = new RetentionUnit[]{RetentionUnit.HOURS,
            RetentionUnit.DAYS};// "minutes","hours", "days",
        Boolean[] gaps = new Boolean[]{false, true};
        FeedType[] feedTypes = new FeedType[]{FeedType.DAILY, FeedType.YEARLY, FeedType.MONTHLY};
        final Boolean[] withData = new Boolean[]{true};

        return MathUtil.crossProduct(retentionPeriods, retentionUnits, gaps, feedTypes, withData);
    }

}

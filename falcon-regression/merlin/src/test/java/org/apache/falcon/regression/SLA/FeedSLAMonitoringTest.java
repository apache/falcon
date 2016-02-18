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

package org.apache.falcon.regression.SLA;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;


/**
 * Feed SLA monitoring tests.
 * Test assumes following properties are set in startup.properties of server :
 *      *.feed.sla.statusCheck.frequency.seconds=60
 *      *.feed.sla.lookAheadWindow.millis=60000
 */
@Test(groups = { "distributed", "embedded" })
public class FeedSLAMonitoringTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private List<String> slaFeedNames;
    private List<Frequency> slaFeedFrequencies;
    private String clusterName;
    private static final Logger LOGGER = Logger.getLogger(FeedSLAMonitoringTest.class);

    private String startTime;
    private String endTime;
    private String slaStartTime;
    private String slaEndTime;
    private int noOfFeeds;
    private int statusCheckFrequency;

    private static final Comparator<SchedulableEntityInstance> DEPENDENCY_COMPARATOR =
            new Comparator<SchedulableEntityInstance>() {
                @Override
                public int compare(SchedulableEntityInstance o1, SchedulableEntityInstance o2) {
                    return o1.compareTo(o2);
                }
            };

    /**
     * Submitting 3 feeds with different frequencies and sla values.
     * @throws Exception
     */
    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {

        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        clusterName = bundles[0].getClusterNames().get(0);
        ServiceResponse response =
                prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);

        startTime = TimeUtil.getTimeWrtSystemTime(-10);
        endTime = TimeUtil.addMinsToTime(startTime, 20);
        noOfFeeds=3;

        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        final String oldFeedName = bundles[0].getInputFeedNameFromBundle();
        slaFeedFrequencies = Arrays.asList(new Frequency("1", Frequency.TimeUnit.minutes),
                new Frequency("2", Frequency.TimeUnit.minutes),
                new Frequency("4", Frequency.TimeUnit.minutes));

        slaFeedNames = Arrays.asList(oldFeedName + "-1", oldFeedName + "-2", oldFeedName + "-3");

        //Submit 3 feeds with different frequencies and sla values.
        for (int bIndex = 0; bIndex < noOfFeeds; ++bIndex) {
            final FeedMerlin ipFeed = new FeedMerlin(bundles[0].getInputFeedFromBundle());

            ipFeed.setValidity(startTime, endTime);
            ipFeed.setAvailabilityFlag("_SUCCESS");

            //set slaLow and slaHigh
            ipFeed.setSla(new Frequency("1", Frequency.TimeUnit.minutes),
                    new Frequency("2", Frequency.TimeUnit.minutes));
            ipFeed.setName(slaFeedNames.get(bIndex));
            ipFeed.setFrequency(slaFeedFrequencies.get(bIndex));

            LOGGER.info("Feed is : " + ipFeed.toString());

            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(ipFeed.toString()));
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        cleanTestsDirs();
        removeTestClassEntities();
    }

    /**
     * The following test submits 3 feeds, checks the slaAlert for a given time range and validates its output.
     * It also checks the sla status when feed is deleted , data created with/without _SUCCESS folder.
     * @throws Exception
     */
    @Test
    public void feedSLATest() throws Exception {
        /**TEST : Check sla response for a given time range
         */

        statusCheckFrequency=60; // 60 seconds

        // Map of instanceDate and corresponding list of SchedulableEntityInstance
        Map<String, List<SchedulableEntityInstance>> instanceEntityMap = new HashMap<>();

        slaStartTime = startTime;
        slaEndTime = TimeUtil.addMinsToTime(slaStartTime, 10);
        DateTime slaStartDate = TimeUtil.oozieDateToDate(slaStartTime);
        DateTime slaEndDate = TimeUtil.oozieDateToDate(slaEndTime);

        List<SchedulableEntityInstance> expectedInstances = new ArrayList<>();
        SchedulableEntityInstance expectedSchedulableEntityInstance;

        for (int index = 0; index < noOfFeeds; ++index) {

            DateTime dt = new DateTime(slaStartDate);
            while (!dt.isAfter(slaEndDate)) {

                expectedSchedulableEntityInstance = new SchedulableEntityInstance(slaFeedNames.get(index),
                        clusterName, dt.toDate(), EntityType.FEED);
                expectedSchedulableEntityInstance.setTags("Missed SLA High");
                expectedInstances.add(expectedSchedulableEntityInstance);

                if (!instanceEntityMap.containsKey(dt.toString())) {
                    instanceEntityMap.put(dt.toString(), new ArrayList<SchedulableEntityInstance>());
                }
                instanceEntityMap.get(dt.toString()).add(expectedSchedulableEntityInstance);
                dt = dt.plusMinutes(slaFeedFrequencies.get(index).getFrequencyAsInt());

            }
        }

        TimeUtil.sleepSeconds(statusCheckFrequency);

        SchedulableEntityInstanceResult response = prism.getFeedHelper().getSlaAlert(
                "?start=" + slaStartTime + "&end=" + slaEndTime).getSlaResult();

        LOGGER.info(response.getMessage());

        validateInstances(response, expectedInstances);

        /**TEST : Create missing dependencies with _SUCCESS directory and check sla response
         */

        String dateEntry = (String) instanceEntityMap.keySet().toArray()[1];
        LOGGER.info(dateEntry + "/" + instanceEntityMap.get(dateEntry));
        List<String> dataDates = InstanceUtil.getMinuteDatesToPath(dateEntry, dateEntry, 0);

        HadoopUtil.createFolders(clusterFS, baseTestHDFSDir + "/input/", dataDates);

        //sla response for feeds when _SUCCESS file is missing from dataPath
        response = prism.getFeedHelper().getSlaAlert("?start=" + slaStartTime + "&end=" + slaEndTime).getSlaResult();

        // Response does not change as it checks for _SUCCESS file
        validateInstances(response, expectedInstances);

        //Create _SUCCESS file
        HadoopUtil.recreateDir(clusterFS, baseTestHDFSDir + "/input/" + dataDates.get(0) + "/_SUCCESS");
        for (SchedulableEntityInstance instance : instanceEntityMap.get(dateEntry)) {
            expectedInstances.remove(instance);
        }
        instanceEntityMap.remove(dateEntry);

        TimeUtil.sleepSeconds(statusCheckFrequency);

        //sla response for feeds when _SUCCESS file is available in dataPath
        response = prism.getFeedHelper().getSlaAlert("?start=" + slaStartTime + "&end=" + slaEndTime).getSlaResult();
        validateInstances(response, expectedInstances);

        /** TEST : Delete feed and check sla response
         */
        String deletedFeed = slaFeedNames.get(0);
        prism.getFeedHelper().deleteByName(deletedFeed, null);

        for (Map.Entry<String, List<SchedulableEntityInstance>> entry : instanceEntityMap.entrySet())
        {
            LOGGER.info(entry.getKey() + "/" + entry.getValue());
            for (SchedulableEntityInstance instance : entry.getValue()) {
                if (instance.getEntityName().equals(deletedFeed)) {
                    expectedInstances.remove(instance);
                }
            }

        }
        TimeUtil.sleepSeconds(statusCheckFrequency);
        response = prism.getFeedHelper().getSlaAlert("?start=" + slaStartTime + "&end=" + slaEndTime).getSlaResult();
        validateInstances(response, expectedInstances);

    }

    /**
     * Validating expected response with actual response.
     * @param response SchedulableEntityInstanceResult response
     * @param expectedInstances List of expected instances
     */
    private static void validateInstances(SchedulableEntityInstanceResult response,
            List<SchedulableEntityInstance> expectedInstances) {

        List<SchedulableEntityInstance> actualInstances = Arrays.asList(response.getInstances());

        for (SchedulableEntityInstance instance : actualInstances) {
            instance.setTags("Missed SLA High");
        }

        Collections.sort(expectedInstances, DEPENDENCY_COMPARATOR);
        Collections.sort(actualInstances, DEPENDENCY_COMPARATOR);

        Assert.assertEquals(actualInstances, expectedInstances, "Instances mismatch for");
    }
}

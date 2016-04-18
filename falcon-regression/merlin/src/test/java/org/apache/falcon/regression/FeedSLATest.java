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

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Feed SLA tests.
 */
@Test(groups = { "distributed", "embedded", "sanity" })
public class FeedSLATest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(FeedSLATest.class);

    private FeedMerlin feedMerlin;
    private String startTime;
    private String endTime;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);

        startTime = TimeUtil.getTimeWrtSystemTime(0);
        endTime = TimeUtil.addMinsToTime(startTime, 120);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        ServiceResponse response =
                prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);

        feedMerlin = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feedMerlin.setFrequency(new Frequency("1", Frequency.TimeUnit.hours));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Submit feed with correctly adjusted sla. Response should reflect success.
     *
     */

    @Test
    public void submitValidFeedSLA() throws Exception {

        feedMerlin.clearFeedClusters();
        feedMerlin.addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .build());

        //set slaLow and slaHigh
        feedMerlin.setSla(new Frequency("3", Frequency.TimeUnit.hours), new Frequency("6", Frequency.TimeUnit.hours));

        final ServiceResponse serviceResponse =
                prism.getFeedHelper().submitEntity(feedMerlin.toString());
        AssertUtil.assertSucceeded(serviceResponse);
    }

    /**
     * Submit feed with slaHigh greater than  feed retention. Response should reflect failure.
     *
     */

    @Test
    public void submitFeedWithSLAHigherThanRetention() throws Exception {

        feedMerlin.clearFeedClusters();
        feedMerlin.addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention((new Frequency("2", Frequency.TimeUnit.hours)).toString(), ActionType.DELETE)
                .withValidity(startTime, endTime)
                .build());

        //set slaLow and slaHigh
        feedMerlin.setSla(new Frequency("3", Frequency.TimeUnit.hours), new Frequency("6", Frequency.TimeUnit.hours));

        final ServiceResponse serviceResponse =
                prism.getFeedHelper().submitEntity(feedMerlin.toString());
        String message = "Feed's retention limit: "
                + feedMerlin.getClusters().getClusters().get(0).getRetention().getLimit()
                + " of referenced cluster " + bundles[0].getClusterNames().get(0)
                + " should be more than feed's late arrival cut-off period: "
                + feedMerlin.getSla().getSlaHigh().getTimeUnit()
                + "(" + feedMerlin.getSla().getSlaHigh().getFrequency() + ")"
                + " for feed: " + bundles[0].getInputFeedNameFromBundle();
        validate(serviceResponse, message);
    }


    /**
     * Submit feed with slaHigh less than  slaLow. Response should reflect failure.
     *
     */
    @Test
    public void submitFeedWithSLAHighLowerthanSLALow() throws Exception {

        feedMerlin.clearFeedClusters();
        feedMerlin.addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention((new Frequency("6", Frequency.TimeUnit.hours)).toString(), ActionType.DELETE)
                .withValidity(startTime, endTime)
                .build());

        //set slaLow and slaHigh
        feedMerlin.setSla(new Frequency("4", Frequency.TimeUnit.hours), new Frequency("2", Frequency.TimeUnit.hours));

        final ServiceResponse serviceResponse =
                prism.getFeedHelper().submitEntity(feedMerlin.toString());
        String message = "slaLow of Feed: " + feedMerlin.getSla().getSlaLow().getTimeUnit() + "("
                + feedMerlin.getSla().getSlaLow().getFrequency() + ")is greater than slaHigh: "
                + feedMerlin.getSla().getSlaHigh().getTimeUnit() + "(" + feedMerlin.getSla().getSlaHigh().getFrequency()
                + ") for cluster: " + bundles[0].getClusterNames().get(0);
        validate(serviceResponse, message);
    }

    /**
     * Submit feed with slaHigh and slaLow greater than feed retention. Response should reflect failure.
     *
     */
    @Test
    public void submitFeedWithSLAHighSLALowHigherThanRetention() throws Exception {

        feedMerlin.clearFeedClusters();
        feedMerlin.addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention((new Frequency("4", Frequency.TimeUnit.hours)).toString(), ActionType.DELETE)
                .withValidity(startTime, endTime)
                .build());

        //set slaLow and slaHigh
        feedMerlin.setSla(new Frequency("5", Frequency.TimeUnit.hours), new Frequency("6", Frequency.TimeUnit.hours));

        final ServiceResponse serviceResponse =
                prism.getFeedHelper().submitEntity(feedMerlin.toString());
        String message = "Feed's retention limit: "
                + feedMerlin.getClusters().getClusters().get(0).getRetention().getLimit()
                + " of referenced cluster " + bundles[0].getClusterNames().get(0)
                + " should be more than feed's late arrival cut-off period: "
                + feedMerlin.getSla().getSlaHigh().getTimeUnit() +"(" + feedMerlin.getSla().getSlaHigh().getFrequency()
                + ")" + " for feed: " + bundles[0].getInputFeedNameFromBundle();
        validate(serviceResponse, message);
    }

    /**
     * Submit feed with slaHigh and slaLow having equal value. Response should reflect success.
     *
     */
    @Test
    public void submitFeedWithSameSLAHighSLALow() throws Exception {

        feedMerlin.clearFeedClusters();
        feedMerlin.addFeedCluster(new FeedMerlin.FeedClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention((new Frequency("7", Frequency.TimeUnit.hours)).toString(), ActionType.DELETE)
                .withValidity(startTime, endTime)
                .build());

        //set slaLow and slaHigh
        feedMerlin.setSla(new Frequency("3", Frequency.TimeUnit.hours), new Frequency("3", Frequency.TimeUnit.hours));

        final ServiceResponse serviceResponse =
                prism.getFeedHelper().submitEntity(feedMerlin.toString());
        AssertUtil.assertSucceeded(serviceResponse);
    }

    private void validate(ServiceResponse response, String message) throws Exception {
        AssertUtil.assertFailed(response);
        LOGGER.info("Expected message is : " + message);
        Assert.assertTrue(response.getMessage().contains(message),
                "Correct response was not present in feed schedule. Feed response is : "
                        + response.getMessage());
    }

}

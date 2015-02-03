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
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
* Process SLA tests.
*/
@Test(groups = "embedded")
public class ProcessSLATest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(ProcessSLATest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 20);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(prism);
        bundles[0].setInputFeedDataPath(baseTestHDFSDir + MINUTE_DATE_PATTERN);
        bundles[0].setOutputFeedLocationData(baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].submitFeeds(prism);
        bundles[0].setProcessConcurrency(1);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedule process with correctly adjusted sla. Response should reflect success.
     *
     */
    @Test
    public void scheduleValidProcessSLA() throws Exception {

        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setSla(new Frequency("3", Frequency.TimeUnit.hours),
                new Frequency("6", Frequency.TimeUnit.hours));
        bundles[0].setProcessData(processMerlin.toString());
        ServiceResponse response = prism.getProcessHelper().submitAndSchedule(processMerlin.toString());
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Schedule process with slaStart and slaEnd having equal value. Response should reflect success.
     *
     */
    @Test
    public void scheduleProcessWithSameSLAStartSLAEnd() throws Exception {

        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setSla(new Frequency("3", Frequency.TimeUnit.hours),
                new Frequency("3", Frequency.TimeUnit.hours));
        bundles[0].setProcessData(processMerlin.toString());
        ServiceResponse response = prism.getProcessHelper().submitAndSchedule(processMerlin.toString());
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Schedule process with slaEnd less than slaStart. Response should reflect failure.
     *
     */
    @Test
    public void scheduleProcessWithSLAEndLowerthanSLAStart() throws Exception {

        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setSla(new Frequency("4", Frequency.TimeUnit.hours),
                new Frequency("2", Frequency.TimeUnit.hours));
        bundles[0].setProcessData(processMerlin.toString());
        ServiceResponse response = prism.getProcessHelper().submitAndSchedule(processMerlin.toString());
        LOGGER.info("response : " + response.getMessage());

        String message = "shouldStartIn of Process: " + processMerlin.getSla().getShouldStartIn().getTimeUnit() + "("
                + processMerlin.getSla().getShouldStartIn().getFrequency() + ")is greater than shouldEndIn: "
                + processMerlin.getSla().getShouldEndIn().getTimeUnit() +"("
                + processMerlin.getSla().getShouldEndIn().getFrequency() + ")";
        validate(response, message);
    }

    /**
     * Schedule process with timeout greater than slaStart. Response should reflect success.
     *
     */
    @Test
    public void scheduleProcessWithTimeoutGreaterThanSLAStart() throws Exception {

        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setTimeout(new Frequency("3", Frequency.TimeUnit.hours));
        processMerlin.setSla(new Frequency("2", Frequency.TimeUnit.hours),
                new Frequency("4", Frequency.TimeUnit.hours));
        bundles[0].setProcessData(processMerlin.toString());
        ServiceResponse response = prism.getProcessHelper().submitAndSchedule(processMerlin.toString());
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Schedule process with timeout less than slaStart. Response should reflect failure.
     *
     */
    @Test
    public void scheduleProcessWithTimeoutLessThanSLAStart() throws Exception {

        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setTimeout(new Frequency("1", Frequency.TimeUnit.hours));
        processMerlin.setSla(new Frequency("2", Frequency.TimeUnit.hours),
                new Frequency("4", Frequency.TimeUnit.hours));
        bundles[0].setProcessData(processMerlin.toString());
        ServiceResponse response = prism.getProcessHelper().submitAndSchedule(processMerlin.toString());

        String message = "shouldStartIn of Process: " + processMerlin.getSla().getShouldStartIn().getTimeUnit() + "("
                + processMerlin.getSla().getShouldStartIn().getFrequency() + ") is greater than timeout: "
                +processMerlin.getTimeout().getTimeUnit() +"(" + processMerlin.getTimeout().getFrequency() + ")";
        validate(response, message);
    }

    private void validate(ServiceResponse response, String message) throws Exception {
        AssertUtil.assertFailed(response);
        LOGGER.info("Expected message is : " + message);
        Assert.assertTrue(response.getMessage().contains(message),
                "Correct response was not present in process schedule. Process response is : "
                        + response.getMessage());
    }

}

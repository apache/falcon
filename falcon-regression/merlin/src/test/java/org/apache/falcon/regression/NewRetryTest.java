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


import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests with Retries.
 */
@Test(groups = "embedded")
public class NewRetryTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(NewRetryTest.class);
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);

    private DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
    private final String baseTestDir = cleanAndGetTestDir();
    private final String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private final String lateDir = baseTestDir + "/lateDataTest/testFolders";
    private final String latePath = lateDir + MINUTE_DATE_PATTERN;
    private DateTime startDate;
    private DateTime endDate;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = new Bundle(BundleUtil.readRetryBundle(), cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
        endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);
        bundles[0].setProcessValidity(TimeUtil.dateToOozieDate(startDate.toDate()),
            TimeUtil.dateToOozieDate(endDate.toDate()));

        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feed.setFeedPathValue(latePath).insertLateFeedValue(new Frequency("minutes(8)"));
        bundles[0].getDataSets().remove(bundles[0].getInputFeedFromBundle());
        bundles[0].getDataSets().add(feed.toString());
        bundles[0].setOutputFeedLocationData(baseTestDir + "/output" + MINUTE_DATE_PATTERN);
        bundles[0].submitClusters(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessZeroAttemptUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            // lets create data now:
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);

            //schedule process
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));

            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);


            int defaultRetries = bundles[0].getProcessObject().getRetry().getAttempts();

            retry.setAttempts((0));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            prism.getProcessHelper()
                .update((bundles[0].getProcessData()), bundles[0].getProcessData());
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                bundles[0].getProcessName(), EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, defaultRetries);
            checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessLowerAttemptUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }
        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            //now wait till the process is over
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            for (int attempt = 0;
                 attempt < 20 && !validateFailureRetries(clusterOC, bundleId, 1); ++attempt) {
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertTrue(validateFailureRetries(clusterOC, bundleId, 1),
                "Failure Retry validation failed");


            retry.setAttempts((retry.getAttempts() - 2));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));

            if ((retry.getAttempts() - 2) > 0) {
                Assert.assertTrue(prism.getProcessHelper()
                    .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                    .getMessage().contains("updated successfully"),
                    "process was not updated successfully");
                String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    bundles[0].getProcessName(), EntityType.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(clusterOC, bundleId, retry.getAttempts() - 2);
                if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
                }
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerManageableAttemptUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }
        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);

            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            for (int i = 0; i < 10 && !validateFailureRetries(clusterOC, bundleId, 1); ++i) {
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertTrue(validateFailureRetries(clusterOC, bundleId, 1),
                "Failure Retry validation failed");

            retry.setAttempts((retry.getAttempts() - 1));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                    .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                    .getMessage().contains("updated successfully"),
                "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                bundles[0].getProcessName(), EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, retry.getAttempts() - 1);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerBoundaryAttemptUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }
        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));

            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            for (int attempt = 0;
                 attempt < 10 && !validateFailureRetries(clusterOC, bundleId, 2); ++attempt) {
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertTrue(validateFailureRetries(clusterOC, bundleId, 2),
                "Failure Retry validation failed");


            retry.setAttempts((2));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(
                prism.getProcessHelper()
                    .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                    .getMessage().contains("updated successfully"),
                "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                bundles[0].getProcessName(), EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, 2);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }
        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            retry.setAttempts((4));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                .update(bundles[0].getProcessName(),
                    null).getMessage()
                .contains("updated successfully"), "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                bundles[0].getProcessName(), EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, 4);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessHigherDelayUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }
        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());
        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            retry.setDelay(new Frequency("minutes(" + (retry.getDelay().getFrequency() + 1) + ")"));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(
                prism.getProcessHelper().update(bundles[0].getProcessName(),
                    bundles[0].getProcessData()).getMessage()
                    .contains("updated successfully"), "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                bundles[0].getProcessName(), EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerDelayUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            retry.setDelay(new Frequency(
                "minutes(" + (Integer.parseInt(retry.getDelay().getFrequency()) - 1) + ")"));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                    .update(bundles[0].getProcessName(),
                        bundles[0].getProcessData()).getMessage()
                    .contains("updated successfully"),
                "process was not updated successfully");
            String newBundleId = InstanceUtil
                .getLatestBundleID(cluster, bundles[0].getProcessName(),
                    EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessZeroDelayUpdate(Retry retry) throws Exception {

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        bundles[0].setRetry(retry);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            retry.setDelay(new Frequency("minutes(0)"));

            bundles[0].setRetry(retry);

            LOGGER.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertFalse(
                prism.getProcessHelper().update(bundles[0].getProcessName()
                    , bundles[0].getProcessData()).getMessage().contains("updated successfully"),
                "process was updated successfully!!!");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                bundles[0].getProcessName(), EntityType.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSimpleFailureCase(Retry retry) throws Exception {

        bundles[0].setRetry(retry);

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        bundles[0].setProcessLatePolicy(null);
        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryWhileAutomaticRetriesHappen(Retry retry) throws Exception {

        DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

        bundles[0].setRetry(retry);

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        LOGGER.info("process dates: " + startDate + "," + endDate);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));

            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            for (int attempt = 0;
                 attempt < 10 && !validateFailureRetries(clusterOC, bundleId, 1); ++attempt) {
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertTrue(validateFailureRetries(clusterOC, bundleId, 1),
                "Failure Retry validation failed");

            //now start firing random retries
            LOGGER.info("now firing user reruns:");
            for (int i = 0; i < 1; i++) {
                prism.getProcessHelper()
                    .getProcessInstanceRerun(bundles[0].getProcessName(),
                        "?start=" + timeFormatter.print(startDate).replace("/", "T") + "Z"
                            + "&end=" + timeFormatter.print(endDate).replace("/", "T") + "Z");
            }
            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryAfterAutomaticRetriesHappen(Retry retry) throws Exception {

        DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

        bundles[0].setRetry(retry);

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        LOGGER.info("process dates: " + startDate + "," + endDate);

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(),
                EntityType.PROCESS).get(0);

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());

            LOGGER.info("now firing user reruns:");

            DateTime[] dateBoundaries = getFailureTimeBoundaries(clusterOC, bundleId);
            InstancesResult piResult = prism.getProcessHelper()
                .getProcessInstanceRerun(bundles[0].getProcessName(),
                    "?start=" + timeFormatter.print(dateBoundaries[0]).replace("/", "T") + "Z"
                        + "&end=" + timeFormatter.print(dateBoundaries[dateBoundaries.length - 1])
                         .replace("/", "T") + "Z");

            AssertUtil.assertSucceeded(piResult);

            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts() + 1);

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSuspendedAndResumeCaseWithLateData(Retry retry) throws Exception {

        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feed.setFeedPathValue(latePath);
        feed.insertLateFeedValue(new Frequency("minutes(10)"));
        bundles[0].getDataSets().remove(bundles[0].getInputFeedFromBundle());
        bundles[0].getDataSets().add(feed.toString());
        bundles[0].setRetry(retry);

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);
            List<DateTime> dates = null;

            for (int i = 0; i < 10 && dates == null; ++i) {
                dates = OozieUtil.getStartTimeForRunningCoordinators(cluster, bundleId);
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertNotNull(dates, String
                .format("Start time for running coordinators of bundle: %s should not be null.",
                    bundleId));
            LOGGER.info("Start time: " + formatter.print(startDate));
            LOGGER.info("End time: " + formatter.print(endDate));
            LOGGER.info("candidate nominal time:" + formatter.print(dates.get(0)));

            for (int attempt = 0;
                 attempt < 10 && !validateFailureRetries(clusterOC, bundleId, 1); ++attempt) {
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertTrue(validateFailureRetries(clusterOC, bundleId, 1),
                "Failure Retry validation failed");

            LOGGER.info("now suspending the process altogether....");

            AssertUtil.assertSucceeded(
                cluster.getProcessHelper().suspend(bundles[0].getProcessData()));

            HashMap<String, Integer> initialMap = getFailureRetriesForEachWorkflow(
                clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId));
            LOGGER.info("saved state of workflow retries");

            for (String key : initialMap.keySet()) {
                LOGGER.info(key + "," + initialMap.get(key));
            }

            TimeUnit.MINUTES.sleep(10);


            HashMap<String, Integer> finalMap = getFailureRetriesForEachWorkflow(
                clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId));
            LOGGER.info("final state of process looks like:");

            for (String key : finalMap.keySet()) {
                LOGGER.info(key + "," + finalMap.get(key));
            }

            Assert.assertEquals(initialMap.size(), finalMap.size(),
                "a new workflow retried while process was suspended!!!!");

            for (String key : initialMap.keySet()) {
                Assert.assertEquals(initialMap.get(key), finalMap.get(key),
                    "values are different for workflow: " + key);
            }

            LOGGER.info("now resuming the process...");
            AssertUtil.assertSucceeded(
                cluster.getProcessHelper().resume(bundles[0].getProcessData()));

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInLateDataCase(Retry retry) throws Exception {

        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feed.setFeedPathValue(latePath);

        feed.insertLateFeedValue(getFrequency(retry));

        bundles[0].getDataSets().remove(bundles[0].getInputFeedFromBundle());
        bundles[0].getDataSets().add(feed.toString());

        bundles[0].setRetry(retry);

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }

        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            List<String> initialData =
                Util.getHadoopDataFromDir(clusterFS, bundles[0].getInputFeedFromBundle(),
                    lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);
            List<DateTime> dates = null;

            for (int i = 0; i < 10 && dates == null; ++i) {
                dates = OozieUtil.getStartTimeForRunningCoordinators(cluster, bundleId);
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertNotNull(dates, String
                .format("Start time for running coordinators of bundle: %s should not be null.",
                    bundleId));

            LOGGER.info("Start time: " + formatter.print(startDate));
            LOGGER.info("End time: " + formatter.print(endDate));
            LOGGER.info("candidate nominal time:" + formatter.print(dates.get(0)));
            DateTime now = dates.get(0);

            if (formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0))) > 0) {
                now = startDate;
            }

            //now wait till the process is over
            for (int attempt = 0; attempt < 10 && !validateFailureRetries(
                clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
                 ++attempt) {
                TimeUtil.sleepSeconds(10);
            }
            Assert.assertTrue(
                validateFailureRetries(clusterOC, bundleId,
                    bundles[0].getProcessObject().getRetry().getAttempts()),
                "Failure Retry validation failed");

            String insertionFolder =
                Util.findFolderBetweenGivenTimeStamps(now, now.plusMinutes(5), initialData);
            LOGGER.info("inserting data in folder " + insertionFolder + " at " + DateTime.now());
            HadoopUtil.injectMoreData(clusterFS, lateDir + insertionFolder,
                    OSUtil.OOZIE_EXAMPLE_INPUT_DATA + "lateData");
            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                bundles[0].getProcessObject().getRetry().getAttempts());

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInDeleteAfterPartialRetryCase(Retry retry) throws Exception {

        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        feed.setFeedPathValue(latePath);
        feed.insertLateFeedValue(new Frequency("minutes(1)"));
        bundles[0].getDataSets().remove(bundles[0].getInputFeedFromBundle());
        bundles[0].getDataSets().add(feed.toString());

        bundles[0].setRetry(retry);

        for (String data : bundles[0].getDataSets()) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(data));
        }


        //submit and schedule process
        ServiceResponse response =
            prism.getProcessHelper().submitEntity(bundles[0].getProcessData());
        if (retry.getAttempts() <= 0 || retry.getDelay().getFrequencyAsInt() <= 0) {
            AssertUtil.assertFailed(response);
        } else {
            AssertUtil.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            HadoopUtil.lateDataReplenish(clusterFS, 20, 0, lateDir);
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().schedule(bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = OozieUtil.getBundles(clusterOC,
                bundles[0].getProcessName(), EntityType.PROCESS).get(0);

            validateRetry(clusterOC, bundleId,
                (bundles[0].getProcessObject().getRetry().getAttempts()) / 2);

            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete((bundles[0].getProcessData())));

            if (retry.getPolicy() == PolicyType.EXP_BACKOFF) {
                TimeUnit.MINUTES.sleep(retry.getDelay().getFrequencyAsInt() * ((retry.getAttempts()
                    - (bundles[0].getProcessObject().getRetry().getAttempts()) / 2) ^ 2));
            } else {
                TimeUnit.MINUTES
                    .sleep(retry.getDelay().getFrequencyAsInt()
                        * ((bundles[0].getProcessObject().getRetry().getAttempts())
                        - (bundles[0].getProcessObject().getRetry().getAttempts()) / 2));
            }

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                (bundles[0].getProcessObject().getRetry().getAttempts()) / 2);

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retry, bundleId);
            }
        }
    }


    private void validateRetry(OozieClient oozieClient, String bundleId, int maxNumberOfRetries)
        throws Exception {
        //validate that all failed processes were retried the specified number of times.
        for (int i = 0; i < 60 && getDefaultOozieCoordinator(oozieClient, bundleId) == null; ++i) {
            TimeUtil.sleepSeconds(10);
        }
        final CoordinatorJob defaultCoordinator = getDefaultOozieCoordinator(oozieClient, bundleId);
        Assert.assertNotNull(defaultCoordinator, "Unexpected value of defaultCoordinator");

        for (int i = 0;
             i < 60 && !validateFailureRetries(oozieClient, bundleId, maxNumberOfRetries); ++i) {
            LOGGER.info("desired state not reached, attempt number: " + i);
            TimeUtil.sleepSeconds(10);
        }
        Assert.assertTrue(validateFailureRetries(oozieClient, bundleId, maxNumberOfRetries),
            "all retries were not attempted correctly!");
    }


    private boolean validateFailureRetries(OozieClient oozieClient, String bundleId,
                                           int maxNumberOfRetries) throws Exception {
        final CoordinatorJob coordinator = getDefaultOozieCoordinator(clusterOC, bundleId);
        if (maxNumberOfRetries < 0) {
            maxNumberOfRetries = 0;
        }
        LOGGER.info("coordinator: " + coordinator);
        HashMap<String, Boolean> workflowMap = new HashMap<String, Boolean>();

        if (coordinator == null || coordinator.getActions().size() == 0) {
            return false;
        }
        LOGGER.info("coordinator.getActions(): " + coordinator.getActions());
        for (CoordinatorAction action : coordinator.getActions()) {

            if (null == action.getExternalId()) {
                return false;
            }


            WorkflowJob actionInfo = oozieClient.getJobInfo(action.getExternalId());
            LOGGER
                .info("actionInfo: " + actionInfo + " actionInfo.getRun(): " + actionInfo.getRun());


            if (!(actionInfo.getStatus() == WorkflowJob.Status.SUCCEEDED
                || actionInfo.getStatus() == WorkflowJob.Status.RUNNING)) {
                if (actionInfo.getRun() == maxNumberOfRetries) {
                    workflowMap.put(actionInfo.getId(), true);
                } else {
                    Assert.assertTrue(actionInfo.getRun() < maxNumberOfRetries,
                        "The workflow exceeded the max number of retries specified for it!!!!");
                    workflowMap.put(actionInfo.getId(), false);
                }

            } else if (actionInfo.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                workflowMap.put(actionInfo.getId(), true);
            }
        }

        //first make sure that the map has all the entries for the coordinator:
        if (workflowMap.size() != coordinator.getActions().size()) {
            return false;
        } else {
            boolean result = true;

            for (String key : workflowMap.keySet()) {
                result &= workflowMap.get(key);
            }

            return result;
        }
    }

    public CoordinatorJob getDefaultOozieCoordinator(OozieClient oozieClient, String bundleId)
        throws Exception {
        BundleJob bundlejob = oozieClient.getBundleJobInfo(bundleId);

        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if (coord.getAppName().contains("DEFAULT")) {
                return oozieClient.getCoordJobInfo(coord.getId());
            }
        }
        return null;
    }

    @DataProvider(name = "DP")
    public Object[][] getData() {

        String[] retryTypes = new String[]{"periodic", "exp-backoff"}; //,"exp-backoff"
        Integer[] delays = new Integer[]{2, 0}; //removing -1 since this should be checked at
                                                // validation level while setting
        String[] delayUnits = new String[]{"minutes"};
        Integer[] retryAttempts = new Integer[]{2, 0, 3}; //0,-1,2

        Object[][] crossProd = MatrixUtil
            .crossProduct(delays, delayUnits, retryTypes, retryAttempts);
        Object[][] testData = new Object[crossProd.length][1];
        for (int i = 0; i < crossProd.length; ++i) {
            final Integer delay = (Integer) crossProd[i][0];
            final String delayUnit = (String) crossProd[i][1];
            final String retryType = (String) crossProd[i][2];
            final Integer retry = (Integer) crossProd[i][3];
            testData[i][0] = getRetry(delay, delayUnit, retryType, retry);
        }
        return testData;
    }

    private void waitTillCertainPercentageOfProcessHasStarted(OozieClient oozieClient,
                                                              String bundleId, int percentage)
        throws Exception {
        OozieUtil.waitForCoordinatorJobCreation(oozieClient, bundleId);
        CoordinatorJob defaultCoordinator = getDefaultOozieCoordinator(oozieClient, bundleId);

        // make sure default coordinator is not null before we proceed
        for (int i = 0; i < 120 && (defaultCoordinator == null || defaultCoordinator.getStatus()
            == CoordinatorJob.Status.PREP); ++i) {
            TimeUtil.sleepSeconds(10);
            defaultCoordinator = getDefaultOozieCoordinator(oozieClient, bundleId);
        }
        Assert.assertNotNull(defaultCoordinator, "default coordinator is null");
        Assert.assertNotEquals(defaultCoordinator.getStatus(), CoordinatorJob.Status.PREP,
            "Unexpected state for coordinator job: " + defaultCoordinator.getId());
        int totalCount = defaultCoordinator.getActions().size();

        int percentageConversion = (percentage * totalCount) / 100;

        while (percentageConversion > 0) {
            int doneBynow = 0;
            for (CoordinatorAction action : defaultCoordinator.getActions()) {
                CoordinatorAction actionInfo = oozieClient.getCoordActionInfo(action.getId());
                if (actionInfo.getStatus() == CoordinatorAction.Status.RUNNING) {
                    doneBynow++;
                }
            }
            if (doneBynow >= percentageConversion) {
                break;
            }
            TimeUtil.sleepSeconds(10);
        }
    }


    private HashMap<String, Integer> getFailureRetriesForEachWorkflow(OozieClient oozieClient,
                                                                      CoordinatorJob coordinator)
        throws Exception {
        HashMap<String, Integer> workflowRetryMap = new HashMap<String, Integer>();
        for (CoordinatorAction action : coordinator.getActions()) {

            if (null == action.getExternalId()) {
                continue;
            }

            WorkflowJob actionInfo = oozieClient.getJobInfo(action.getExternalId());
            LOGGER.info("adding workflow " + actionInfo.getId() + " to the map");
            workflowRetryMap.put(actionInfo.getId(), actionInfo.getRun());
        }
        return workflowRetryMap;
    }

    private DateTime[] getFailureTimeBoundaries(OozieClient oozieClient, String bundleId)
        throws Exception {
        List<DateTime> dateList = new ArrayList<DateTime>();

        CoordinatorJob coordinator = getDefaultOozieCoordinator(oozieClient, bundleId);

        for (CoordinatorAction action : coordinator.getActions()) {
            if (action.getExternalId() != null) {

                WorkflowJob jobInfo = oozieClient.getJobInfo(action.getExternalId());
                if (jobInfo.getRun() > 0) {
                    dateList.add(new DateTime(jobInfo.getStartTime(), DateTimeZone.UTC));
                }
            }
        }
        Collections.sort(dateList);
        return dateList.toArray(new DateTime[dateList.size()]);
    }

    private void checkIfRetriesWereTriggeredCorrectly(ColoHelper coloHelper, Retry retry,
                                                      String bundleId) throws Exception {
        //it is presumed that this delay here will be expressed in minutes. Hourly/daily is
        // unfeasible to check :)

        final DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("HH:mm:ss");

        final OozieClient oozieClient = coloHelper.getFeedHelper().getOozieClient();
        final CoordinatorJob coordinator = getDefaultOozieCoordinator(oozieClient, bundleId);

        for (CoordinatorAction action : coordinator.getActions()) {
            CoordinatorAction coordAction = oozieClient.getCoordActionInfo(action.getExternalId());
            if (!(coordAction.getStatus() == CoordinatorAction.Status.SUCCEEDED)) {
                int expectedDelay = retry.getDelay().getFrequencyAsInt();
                //first get data from logs:
                List<String> instanceRetryTimes =
                    Util.getInstanceRetryTimes(coloHelper, action.getExternalId());
                List<String> instanceFinishTimes =
                    Util.getInstanceFinishTimes(coloHelper, action.getExternalId());

                LOGGER.info("finish times look like:");
                for (String line : instanceFinishTimes) {
                    LOGGER.info(line);
                }

                LOGGER.info("retry times look like:");
                for (String line : instanceRetryTimes) {
                    LOGGER.info(line);
                }

                LOGGER.info("checking timelines for retry type " + retry.getPolicy().value()
                    + " for delay " + expectedDelay + " for workflow id: " + action.getExternalId());

                if (retry.getPolicy() == PolicyType.PERIODIC) {
                    //in this case the delay unit will always be a constant time diff
                    for (int i = 0; i < instanceFinishTimes.size() - 1; i++) {
                        DateTime temp = timeFormatter.parseDateTime(instanceFinishTimes.get(i));

                        Assert.assertEquals(temp.plusMinutes(expectedDelay).getMillis(),
                            timeFormatter.parseDateTime(instanceRetryTimes.get(i)).getMillis(),
                            5000, "oops! this is out of expected delay range for workflow id  "
                                + action.getExternalId());
                    }
                } else {
                    //check for exponential
                    for (int i = 0; i < instanceFinishTimes.size() - 1; i++) {
                        DateTime temp = timeFormatter.parseDateTime(instanceFinishTimes.get(i));
                        Assert.assertEquals(temp.plusMinutes(expectedDelay).getMillis(),
                            timeFormatter.parseDateTime(instanceRetryTimes.get(i)).getMillis(),
                            5000, "oops! this is out of expected delay range for workflow id "
                                + action.getExternalId());
                        expectedDelay *= 2;
                    }
                }
            }
        }

    }

    private Retry getRetry(int delay, String delayUnits, String retryType,
                           int retryAttempts) {
        Retry retry = new Retry() {
            @Override
            public String toString(){
                return String.format("Frequency: %s; Attempts: %s; PolicyType: %s",
                    this.getDelay(), this.getAttempts(), this.getPolicy());
            }
        };
        retry.setAttempts(retryAttempts);
        retry.setDelay(new Frequency(delayUnits + "(" + delay + ")"));
        retry.setPolicy(PolicyType.fromValue(retryType));
        return retry;
    }

    private Frequency getFrequency(Retry retry) {
        int delay = retry.getDelay().getFrequencyAsInt();
        if (delay == 0) {
            delay = 1;
        }
        int attempts = retry.getAttempts();
        if (attempts == 0) {
            attempts = 1;
        }

        if (retry.getPolicy() == PolicyType.EXP_BACKOFF) {
            delay = (Math.abs(delay)) * (2 ^ (Math.abs(attempts)));
        } else {
            delay = Math.abs(delay * attempts);
        }

        return new Frequency(retry.getDelay().getTimeUnit() + "(" + delay + ")");

    }
}


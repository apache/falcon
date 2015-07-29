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
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.process.ExecutionType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.HadoopFileEditor;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.APIResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Minutes;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * test for process update.
 */
@Test(groups = "distributed")
public class NewPrismProcessUpdateTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String inputFeedPath = baseTestDir + MINUTE_DATE_PATTERN;
    private String workflowPath = baseTestDir + "/falcon-oozie-wf";
    private String workflowPath2 = baseTestDir + "/falcon-oozie-wf2";
    private String aggregatorPath = baseTestDir + "/aggregator";
    private String aggregator1Path = baseTestDir + "/aggregator1";
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private FileSystem cluster1FS = serverFS.get(0);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private OozieClient cluster3OC = serverOC.get(2);
    private static final Logger LOGGER = Logger.getLogger(NewPrismProcessUpdateTest.class);

    @BeforeMethod(alwaysRun = true)
    public void testSetup() throws Exception {
        final Bundle b = BundleUtil.readUpdateBundle();
        bundles[0] = new Bundle(b, cluster1);
        bundles[0].generateUniqueBundle(this);
        bundles[1] = new Bundle(b, cluster2);
        bundles[1].generateUniqueBundle(this);
        bundles[2] = new Bundle(b, cluster3);
        bundles[2].generateUniqueBundle(this);
        setBundleWFPath(bundles[0], bundles[1], bundles[2]);
        bundles[1].addClusterToBundle(bundles[2].getClusters().get(0),
                ClusterType.TARGET, null, null);
        usualGrind(bundles[1]);
        Util.restartService(cluster3.getClusterHelper());
    }

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        for (String wfPath : new String[] { workflowPath, workflowPath2, aggregatorPath, aggregator1Path }) {
            uploadDirToClusters(wfPath, OSUtil.RESOURCES_OOZIE);
        }
        Util.restartService(cluster3.getClusterHelper());
        Util.restartService(cluster1.getClusterHelper());
        Util.restartService(cluster2.getClusterHelper());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunningMonthly()
        throws Exception {
        final String startTIme = TimeUtil.getTimeWrtSystemTime(-20);
        String endTime = TimeUtil.getTimeWrtSystemTime(4000 * 60);
        bundles[1].setProcessPeriodicity(1, TimeUnit.months);
        bundles[1].setOutputFeedPeriodicity(1, TimeUnit.months);
        bundles[1].setProcessValidity(startTIme, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);
        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        ProcessMerlin updatedProcess = new ProcessMerlin(bundles[1].getProcessObject());
        updatedProcess.setFrequency(new Frequency("5", TimeUnit.minutes));

        LOGGER.info("updated process: " + Util.prettyPrintXml(updatedProcess.toString()));

        //now to update
        while (Util
                .parseResponse(prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), updatedProcess.toString()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("update didn't SUCCEED in last attempt");
            TimeUtil.sleepSeconds(10);
        }

        String prismString = getResponse(prism, bundles[1].getProcessData(), true);
        Assert.assertEquals(new ProcessMerlin(prismString).getFrequency(),
                new ProcessMerlin(updatedProcess).getFrequency());
        TimeUtil.sleepSeconds(60);
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, false);
        waitingForBundleFinish(cluster3, oldBundleId, 5);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 1, 10);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);

    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    //failing due to falcon bug : https://issues.apache.org/jira/browse/FALCON-458
    public void updateProcessRollStartTimeForwardInEachColoWithOneProcessRunning()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(7);
        bundles[1].setProcessValidity(startTime, endTime);
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        List<String> oldNominalTimes =
                OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId, EntityType.PROCESS);

        String newStartTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getStart()
        ), 20);
        String newEndTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getStart()
        ), 25);

        bundles[1].setProcessValidity(newStartTime, newEndTime);
        bundles[1].setProcessConcurrency(10);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        LOGGER.info("updated process: " + Util.prettyPrintXml(bundles[1].getProcessData()));
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), Util.prettyPrintXml(bundles[1]
                                .getProcessData())))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("update didn't SUCCEED in last attempt");
            TimeUtil.sleepSeconds(10);
        }

        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, false);

        dualComparison(prism, cluster3, bundles[1].getProcessData());

        waitingForBundleFinish(cluster3, oldBundleId, 15);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS).size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                                .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd())));
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        int expectedNumberOfWorkflows =
                getExpectedNumberOfWorkflowInstances(newStartTime, TimeUtil
                        .dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getEnd()));
        Assert.assertEquals(OozieUtil.getNumberOfWorkflowInstances(cluster3OC, oldBundleId),
                expectedNumberOfWorkflows);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1800000)
    public void updateProcessConcurrencyWorkflowExecutionInEachColoWithOneColoDown()
        throws Exception {
        //bundles[1].generateUniqueBundle(this);
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        TimeUtil.sleepSeconds(25);

        int initialConcurrency = bundles[1].getProcessObject().getParallel();

        bundles[1].setProcessConcurrency(bundles[1].getProcessObject().getParallel() + 3);
        bundles[1].setProcessWorkflow(workflowPath2);
        bundles[1].getProcessObject().setOrder(getRandomExecutionType(bundles[1]));

        //stop cluster 3 where process is scheduled
        Util.shutDownService(cluster3.getProcessHelper());

        //now to update
        AssertUtil.assertPartial(prism.getProcessHelper()
            .update(bundles[1].getProcessData(), bundles[1].getProcessData()));
        ProcessMerlin process = new ProcessMerlin(getResponse(prism, bundles[1].getProcessData(), true));
        Assert.assertEquals(process.getParallel(), initialConcurrency);
        Assert.assertEquals(process.getWorkflow().getPath(), workflowPath);
        Assert.assertEquals(process.getOrder(), bundles[1].getProcessObject().getOrder());

        String coloString = getResponse(cluster2, bundles[1].getProcessData(), true);
        Assert.assertEquals(new ProcessMerlin(coloString).getWorkflow().getPath(), workflowPath2);

        Util.startService(cluster3.getProcessHelper());
        dualComparisonFailure(prism, cluster2, bundles[1].getProcessData());
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        AssertUtil
                .checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("WARNING: update did not succeed, retrying ");
            TimeUtil.sleepSeconds(20);
        }
        process = new ProcessMerlin(getResponse(prism, bundles[1].getProcessData(), true));
        Assert.assertEquals(process.getParallel(), initialConcurrency + 3);
        Assert.assertEquals(process.getWorkflow().getPath(), workflowPath2);
        Assert.assertEquals(process.getOrder(), bundles[1].getProcessObject().getOrder());
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        AssertUtil
                .checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        waitingForBundleFinish(cluster3, oldBundleId);
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd()));
        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");

    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-2);
        String endTime = TimeUtil.getTimeWrtSystemTime(20);
        bundles[1].setProcessValidity(startTime, endTime);
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);
        LOGGER.info("original process: " + Util.prettyPrintXml(bundles[1].getProcessData()));

        ProcessMerlin updatedProcess = new ProcessMerlin(bundles[1].getProcessObject());
        updatedProcess.setFrequency(new Frequency("7", TimeUnit.minutes));

        LOGGER.info("updated process: " + updatedProcess);

        //now to update
        ServiceResponse response =
            prism.getProcessHelper().update(bundles[1].getProcessData(), updatedProcess.toString());
        AssertUtil.assertSucceeded(response);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, false);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 1, 10);

        String prismString = getResponse(prism, bundles[1].getProcessData(), true);
        Assert.assertEquals(new ProcessMerlin(prismString).getFrequency(),
                updatedProcess.getFrequency());
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessNameInEachColoWithOneProcessRunning() throws Exception {
        //bundles[1].generateUniqueBundle(this);
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String originalProcessData = bundles[1].getProcessData();
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        TimeUtil.sleepSeconds(20);
        List<String> oldNominalTimes =
                OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId, EntityType.PROCESS);
        bundles[1].setProcessName(Util.getEntityPrefix(this) + "-myNewProcessName");

        //now to update
        ServiceResponse response =
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData());
        AssertUtil.assertFailed(response);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                originalProcessData, false, false);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneProcessRunning()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-2);
        String endTime = TimeUtil.getTimeWrtSystemTime(10);
        bundles[1].setProcessValidity(startTime, endTime);

        //bundles[1].generateUniqueBundle(this);
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        //now to update
        DateTime updateTime = new DateTime(DateTimeZone.UTC);
        TimeUtil.sleepSeconds(60);
        List<String> oldNominalTimes =
                OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId, EntityType.PROCESS);
        LOGGER.info("updating at " + updateTime);
        while (Util
                .parseResponse(updateProcessConcurrency(bundles[1],
                        bundles[1].getProcessObject().getParallel() + 3))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("WARNING: update did not scceed, retyring ");
            TimeUtil.sleepSeconds(20);
        }

        String prismString = getResponse(prism, bundles[1].getProcessData(), true);
        Assert.assertEquals(new ProcessMerlin(prismString).getParallel(),
                bundles[1].getProcessObject().getParallel() + 3);
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(),
                false, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        // future : should be verified using cord xml
        Job.Status status = OozieUtil.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                bundles[1].getProcessName(), EntityType.PROCESS);

        boolean doesExist = false;
        OozieUtil.createMissingDependencies(cluster3, EntityType.PROCESS, bundles[1].getProcessName(), 0);
        while (status != Job.Status.SUCCEEDED && status != Job.Status.FAILED
                &&
                status != Job.Status.DONEWITHERROR) {
            int statusCount = InstanceUtil
                    .getInstanceCountWithStatus(cluster3OC,
                            bundles[1].getProcessName(),
                            org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                            EntityType.PROCESS);
            if (statusCount == bundles[1].getProcessObject().getParallel() + 3) {
                doesExist = true;
                break;
            }
            status = OozieUtil.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                    bundles[1].getProcessName(), EntityType.PROCESS);
            Assert.assertNotNull(status,
                    "status must not be null!");
            TimeUtil.sleepSeconds(30);
        }

        Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
        int expectedNumberOfInstances =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd()));
        Assert.assertEquals(OozieUtil.getNumberOfWorkflowInstances(cluster3OC, oldBundleId),
                expectedNumberOfInstances);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessIncreaseValidityInEachColoWithOneProcessRunning() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(8);
        bundles[1].setProcessValidity(startTime, endTime);
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        String newEndTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getEnd()
        ), 4);
        bundles[1].setProcessValidity(TimeUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                                .getStart()),
                newEndTime);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        ServiceResponse response = prism.getProcessHelper()
                .update(bundles[1].getProcessData(), bundles[1].getProcessData());
        for (int i = 0; i < 10
                &&
                Util.parseResponse(response).getStatus() != APIResult.Status.SUCCEEDED; ++i) {
            response = prism.getProcessHelper()
                    .update(bundles[1].getProcessData(), bundles[1].getProcessData());
            TimeUtil.sleepSeconds(6);
        }
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED, "Process update did not succeed.");

        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), false, true);

        int i = 0;

        while (OozieUtil.getNumberOfWorkflowInstances(cluster3OC, oldBundleId)
                != getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                                .getStart()
                ), TimeUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                .getValidity()
                                .getEnd()))
                && i < 10) {
            TimeUtil.sleepSeconds(1);
            i++;
        }

        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        waitingForBundleFinish(cluster3, oldBundleId);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        int finalNumberOfInstances = InstanceUtil
                .getProcessInstanceList(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS)
                .size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                                .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd())));
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneProcessSuspended()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(7);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().suspend(bundles[1].getProcessData()));
        //now to update
        while (Util
                .parseResponse(updateProcessConcurrency(bundles[1],
                        bundles[1].getProcessObject().getParallel() + 3))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("WARNING: update did not scceed, retyring ");
            TimeUtil.sleepSeconds(20);
        }

        String prismString = getResponse(prism, bundles[1].getProcessData(), true);
        Assert.assertEquals(new ProcessMerlin(prismString).getParallel(),
                bundles[1].getProcessObject().getParallel() + 3);
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), false, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        AssertUtil.assertSucceeded(cluster3.getProcessHelper().resume(bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster3OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        Job.Status status = OozieUtil.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                bundles[1].getProcessName(), EntityType.PROCESS);

        boolean doesExist = false;
        OozieUtil.createMissingDependencies(cluster3, EntityType.PROCESS, bundles[1].getProcessName(), 0);
        while (status != Job.Status.SUCCEEDED && status != Job.Status.FAILED
                &&
                status != Job.Status.DONEWITHERROR) {
            if (InstanceUtil
                    .getInstanceCountWithStatus(cluster3OC,
                            bundles[1].getProcessName(),
                            org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                            EntityType.PROCESS)
                    ==
                    bundles[1].getProcessObject().getParallel()) {
                doesExist = true;
                break;
            }
            status = OozieUtil.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                    bundles[1].getProcessName(), EntityType.PROCESS);
        }

        Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
        waitingForBundleFinish(cluster3, oldBundleId);

        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd()));

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneColoDown() throws Exception {

        String startTime = TimeUtil.getTimeWrtSystemTime(-1);
        String endTime = TimeUtil.getTimeWrtSystemTime(5);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another

        LOGGER.info("process to be scheduled: " + Util.prettyPrintXml(bundles[1].getProcessData()));

        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        //now to update
        Util.shutDownService(cluster3.getClusterHelper());

        ServiceResponse response =
                updateProcessConcurrency(bundles[1],
                        bundles[1].getProcessObject().getParallel() + 3);
        AssertUtil.assertPartial(response);

        Util.startService(cluster3.getClusterHelper());

        String prismString = getResponse(prism, bundles[1].getProcessData(), true);
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        Assert.assertEquals(new ProcessMerlin(prismString).getParallel(),
                bundles[1].getProcessObject().getParallel());

        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1],
                Job.Status.RUNNING);

        while (Util
                .parseResponse(updateProcessConcurrency(bundles[1],
                        bundles[1].getProcessObject().getParallel() + 3))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("WARNING: update did not scceed, retyring ");
            TimeUtil.sleepSeconds(20);
        }
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        dualComparison(prism, cluster2, bundles[1].getProcessData());

        Job.Status status =
                OozieUtil.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                        bundles[1].getProcessName(), EntityType.PROCESS);

        boolean doesExist = false;
        OozieUtil.createMissingDependencies(cluster3, EntityType.PROCESS, bundles[1].getProcessName(), 0);
        while (status != Job.Status.SUCCEEDED && status != Job.Status.FAILED
                &&
                status != Job.Status.DONEWITHERROR) {
            if (InstanceUtil
                    .getInstanceCountWithStatus(cluster3OC,
                            bundles[1].getProcessName(),
                            org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                            EntityType.PROCESS)
                    ==
                    bundles[1].getProcessObject().getParallel() + 3) {
                doesExist = true;
                break;
            }
            status = OozieUtil.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                    bundles[1].getProcessName(), EntityType.PROCESS);
            TimeUtil.sleepSeconds(30);
        }
        Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
        OozieUtil.verifyNewBundleCreation(cluster3OC, OozieUtil
                        .getLatestBundleID(cluster3OC,
                            bundles[1].getProcessName(), EntityType.PROCESS),
                oldNominalTimes, bundles[1].getProcessData(), false,
                true
        );

        waitingForBundleFinish(cluster3, oldBundleId);

        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd()));
        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessConcurrencyExecutionWorkflowInEachColoWithOneProcessRunning()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-1);
        String endTime = TimeUtil.getTimeWrtSystemTime(7);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        int initialConcurrency = bundles[1].getProcessObject().getParallel();

        bundles[1].setProcessConcurrency(bundles[1].getProcessObject().getParallel() + 3);
        bundles[1].setProcessWorkflow(aggregator1Path);
        bundles[1].getProcessObject().setOrder(getRandomExecutionType(bundles[1]));

        //now to update

        String updateTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();

        LOGGER.info("updating @ " + updateTime);

        while (Util.parseResponse(
                prism.getProcessHelper().update((bundles[1].getProcessData()), bundles[1]
                        .getProcessData())).getStatus() != APIResult.Status.SUCCEEDED) {
            TimeUtil.sleepSeconds(10);
        }
        ProcessMerlin process = new ProcessMerlin(getResponse(prism, bundles[1].getProcessData(), true));
        Assert.assertEquals(process.getParallel(), initialConcurrency + 3);
        Assert.assertEquals(process.getWorkflow().getPath(), aggregator1Path);
        Assert.assertEquals(process.getOrder(), bundles[1].getProcessObject().getOrder());
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS).size();
        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd()));
        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessConcurrencyExecutionWorkflowInEachColoWithOneProcessSuspended()
        throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(2);
        String endTime = TimeUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        int initialConcurrency = bundles[1].getProcessObject().getParallel();

        bundles[1].setProcessConcurrency(bundles[1].getProcessObject().getParallel() + 3);
        bundles[1].setProcessWorkflow(aggregator1Path);
        bundles[1].getProcessObject().setOrder(getRandomExecutionType(bundles[1]));
        //suspend
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().suspend(bundles[1].getProcessData()));

        //now to update
        String updateTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();
        LOGGER.info("updating @ " + updateTime);
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            TimeUtil.sleepSeconds(10);
        }

        AssertUtil.assertSucceeded(cluster3.getProcessHelper().resume(bundles[1].getProcessData()));

        ProcessMerlin process = new ProcessMerlin(getResponse(prism, bundles[1].getProcessData(), true));
        Assert.assertEquals(process.getParallel(), initialConcurrency + 3);
        Assert.assertEquals(process.getWorkflow().getPath(), aggregator1Path);
        Assert.assertEquals(process.getOrder(), bundles[1].getProcessObject().getOrder());
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster3OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        TimeUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getClusters()
                                                .get(0).getValidity()
                                                .getEnd()));
        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneProcessRunning() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-1);
        String endTime = TimeUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        TimeUtil.sleepSeconds(20);
        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        String newFeedName = bundles[1].getInputFeedNameFromBundle() + "2";
        FeedMerlin inputFeed = new FeedMerlin(bundles[1].getInputFeedFromBundle());

        bundles[1].addProcessInput(newFeedName, "inputData2");
        inputFeed.setName(newFeedName);

        LOGGER.info(inputFeed);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(inputFeed.toString()));

        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            TimeUtil.sleepSeconds(20);
        }
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, false);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 1, 10);

        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneProcessSuspended() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(1);
        String endTime = TimeUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        String newFeedName = bundles[1].getInputFeedNameFromBundle() + "2";
        FeedMerlin inputFeed = new FeedMerlin(bundles[1].getInputFeedFromBundle());

        bundles[1].addProcessInput(newFeedName, "inputData2");
        inputFeed.setName(newFeedName);

        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().suspend(bundles[1].getProcessData()));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(inputFeed.toString()));

        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            TimeUtil.sleepSeconds(10);
        }

        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, false);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 1, 10);

        AssertUtil.assertSucceeded(cluster3.getProcessHelper().resume(bundles[1].getProcessData()));

        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster3OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneColoDown() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(10);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        String originalProcess = bundles[1].getProcessData();
        String newFeedName = bundles[1].getInputFeedNameFromBundle() + "2";
        FeedMerlin inputFeed = new FeedMerlin(bundles[1].getInputFeedFromBundle());
        bundles[1].addProcessInput(newFeedName, "inputData2");
        inputFeed.setName(newFeedName);
        String updatedProcess = bundles[1].getProcessData();

        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(originalProcess));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, originalProcess, 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    Util.readEntityName(originalProcess), EntityType.PROCESS);

        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, originalProcess, 0, 10);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        //submit new feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(inputFeed.toString()));

        Util.shutDownService(cluster3.getProcessHelper());

        AssertUtil.assertPartial(
                prism.getProcessHelper()
                        .update(updatedProcess, updatedProcess));

        Util.startService(cluster3.getProcessHelper());
        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        Assert.assertFalse(Util.isDefinitionSame(cluster2, prism, originalProcess));

        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), false, false);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1],
                Job.Status.RUNNING);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        while (Util.parseResponse(
                prism.getProcessHelper().update(updatedProcess, updatedProcess)).getStatus()
                != APIResult.Status.SUCCEEDED) {
            LOGGER.info("update didnt SUCCEED in last attempt");
            TimeUtil.sleepSeconds(10);
        }
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        Assert.assertTrue(Util.isDefinitionSame(cluster2, prism, originalProcess));
        bundles[1].verifyDependencyListing(cluster2);
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                updatedProcess, true, false);
        waitingForBundleFinish(cluster3, oldBundleId);

        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 1, 10);

        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1],
                Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessDecreaseValidityInEachColoWithOneProcessRunning() throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        String newEndTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getEnd()
        ), -2);
        bundles[1].setProcessValidity(TimeUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                                .getStart()),
                newEndTime);
        while (Util.parseResponse(
                (prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData())))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("update didnt SUCCEED in last attempt");
            TimeUtil.sleepSeconds(10);
        }
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), false, true);

        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        waitingForBundleFinish(cluster3, oldBundleId);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances = InstanceUtil
                .getProcessInstanceList(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS)
                .size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(bundles[1]
                                .getProcessObject().getClusters().getClusters().get(0).getValidity()
                                .getStart(),
                        bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                .getValidity().getEnd()));
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        int expectedNumberOfWorkflows =
                getExpectedNumberOfWorkflowInstances(TimeUtil.dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                        .getValidity().getStart()),
                        newEndTime);
        Assert.assertEquals(OozieUtil.getNumberOfWorkflowInstances(cluster3OC, oldBundleId),
                expectedNumberOfWorkflows);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessIncreaseValidityInEachColoWithOneProcessSuspended() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-1);
        String endTime = TimeUtil.getTimeWrtSystemTime(3);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        TimeUtil.sleepSeconds(30);
        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        String newEndTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getEnd()
        ), 4);
        bundles[1].setProcessValidity(TimeUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                                .getStart()),
            newEndTime);

        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().suspend(bundles[1].getProcessData()));
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            LOGGER.info("update didnt SUCCEED in last attempt");
            TimeUtil.sleepSeconds(10);
        }
        AssertUtil.assertSucceeded(cluster3.getProcessHelper().resume(bundles[1].getProcessData()));

        dualComparison(prism, cluster2, bundles[1].getProcessData());

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        waitingForBundleFinish(cluster3, oldBundleId);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances = InstanceUtil
                .getProcessInstanceList(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS)
                .size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(bundles[1]
                                .getProcessObject().getClusters().getClusters().get(0).getValidity()
                                .getStart(),
                        bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                .getValidity().getEnd()));
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    private void setBundleWFPath(Bundle... bundles) {
        for (Bundle bundle : bundles) {
            bundle.setProcessWorkflow(workflowPath);
        }
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunningDaily() throws Exception {
        //set daily process
        final String startTime = TimeUtil.getTimeWrtSystemTime(-20);
        String endTime = TimeUtil.getTimeWrtSystemTime(4000);
        bundles[1].setProcessPeriodicity(1, TimeUnit.days);
        bundles[1].setOutputFeedPeriodicity(1, TimeUnit.days);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes =
                OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId, EntityType.PROCESS);

        LOGGER.info("original process: " + Util.prettyPrintXml(bundles[1].getProcessData()));

        ProcessMerlin updatedProcess = new ProcessMerlin(bundles[1].getProcessObject());
        updatedProcess.setFrequency(new Frequency("5", TimeUnit.minutes));

        LOGGER.info("updated process: " + updatedProcess);

        //now to update
        ServiceResponse response =
            prism.getProcessHelper().update(bundles[1].getProcessData(), updatedProcess.toString());
        AssertUtil.assertSucceeded(response);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 1, 10);

        String prismString = dualComparison(prism, cluster2, bundles[1].getProcessData());
        Assert.assertEquals(new ProcessMerlin(prismString).getFrequency(),
                new Frequency("" + 5, TimeUnit.minutes));
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void
    updateProcessFrequencyInEachColoWithOneProcessRunningDailyToMonthlyWithStartChange()
        throws Exception {
        //set daily process
        final String startTime = TimeUtil.getTimeWrtSystemTime(-20);
        String endTime = TimeUtil.getTimeWrtSystemTime(4000 * 60);
        bundles[1].setProcessPeriodicity(1, TimeUnit.days);
        bundles[1].setOutputFeedPeriodicity(1, TimeUnit.days);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        LOGGER.info("original process: " + Util.prettyPrintXml(bundles[1].getProcessData()));

        ProcessMerlin updatedProcess = new ProcessMerlin(bundles[1].getProcessObject());
        updatedProcess.setFrequency(new Frequency("1", TimeUnit.months));
        updatedProcess.setValidity(TimeUtil.getTimeWrtSystemTime(10), endTime);

        LOGGER.info("updated process: " + updatedProcess);

        //now to update
        ServiceResponse response =
            prism.getProcessHelper().update(bundles[1].getProcessData(), updatedProcess.toString());
        AssertUtil.assertSucceeded(response);
        String prismString = dualComparison(prism, cluster3, bundles[1].getProcessData());
        Assert.assertEquals(new ProcessMerlin(prismString).getFrequency(),
                new Frequency("" + 1, TimeUnit.months));
        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessRollStartTimeBackwardsToPastInEachColoWithOneProcessRunning()
        throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        TimeUtil.sleepSeconds(30);
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS);

        List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId,
                EntityType.PROCESS);

        String newStartTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getStart()
        ), -3);
        bundles[1].setProcessValidity(newStartTime, TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getEnd()
        ));

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);
        OozieUtil.createMissingDependencies(cluster3, EntityType.PROCESS, bundles[1].getProcessName(), 0);
        AssertUtil.assertSucceeded(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()));

        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, true);
        bundles[1].verifyDependencyListing(cluster2);
        dualComparison(prism, cluster3, bundles[1].getProcessData());
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessRollStartTimeForwardInEachColoWithOneProcessSuspended()
        throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        TimeUtil.sleepSeconds(30);

        OozieUtil.getNumberOfWorkflowInstances(cluster3OC, oldBundleId);
        String oldStartTime = TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getStart()
        );
        String newStartTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getStart()
        ), 3);
        bundles[1].setProcessValidity(newStartTime, TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getEnd()
        ));

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().suspend(bundles[1].getProcessData()));

        AssertUtil.assertSucceeded(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()));

        dualComparison(prism, cluster2, bundles[1].getProcessData());

        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3OC, bundles[1].getProcessName(),
                   EntityType.PROCESS).size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(oldStartTime,
                        bundles[1].getProcessObject().getClusters().getClusters().get(0)
                                .getValidity().getEnd()));
        Assert.assertEquals(InstanceUtil
                .getProcessInstanceList(cluster3OC,
                        bundles[1].getProcessName(), EntityType.PROCESS)
                .size(), getExpectedNumberOfWorkflowInstances(newStartTime,
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity().getEnd()));

        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = { "multiCluster" }, timeOut = 1200000)
    public void updateProcessRollStartTimeBackwardsInEachColoWithOneProcessSuspended()
        throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().schedule(bundles[1].getProcessData()));
        String oldBundleId = OozieUtil
                .getLatestBundleID(cluster3OC,
                    bundles[1].getProcessName(), EntityType.PROCESS);
        TimeUtil.sleepSeconds(30);

        String newStartTime = TimeUtil.addMinsToTime(TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getStart()
        ), -3);
        bundles[1].setProcessValidity(newStartTime, TimeUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getClusters().get(0).getValidity()
                        .getEnd()
        ));
        InstanceUtil.waitTillInstancesAreCreated(cluster3OC, bundles[1].getProcessData(), 0, 10);

        waitForProcessToReachACertainState(cluster3OC, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(
                cluster3.getProcessHelper().suspend(bundles[1].getProcessData()));
        AssertUtil.assertSucceeded(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()));
        AssertUtil.assertSucceeded(cluster3.getProcessHelper().resume(bundles[1].getProcessData()));
        List<String> oldNominalTimes =
                OozieUtil.getActionsNominalTime(cluster3OC, oldBundleId, EntityType.PROCESS);

        OozieUtil.verifyNewBundleCreation(cluster3OC, oldBundleId, oldNominalTimes,
                bundles[1].getProcessData(), true, false);

        bundles[1].verifyDependencyListing(cluster2);

        dualComparison(prism, cluster3, bundles[1].getProcessData());
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(timeOut = 1200000)
    public void
    updateProcessWorkflowXml() throws URISyntaxException, JAXBException,
            IOException, OozieClientException, AuthenticationException, InterruptedException {
        Bundle b = BundleUtil.readELBundle();
        HadoopFileEditor hadoopFileEditor = null;
        try {

            b = new Bundle(b, cluster1);
            b.setProcessWorkflow(workflowPath);
            b.generateUniqueBundle(this);

            b.setProcessValidity(TimeUtil.getTimeWrtSystemTime(-10),
                    TimeUtil.getTimeWrtSystemTime(15));
            b.submitFeedsScheduleProcess(prism);

            InstanceUtil.waitTillInstancesAreCreated(cluster1OC, b.getProcessData(), 0, 10);
            OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS,
                    b.getProcessName(), 0);
            InstanceUtil.waitTillInstanceReachState(serverOC.get(0),
                    Util.readEntityName(b.getProcessData()), 0, CoordinatorAction.Status.RUNNING,
                    EntityType.PROCESS);

            //save old data
            String oldBundleID = OozieUtil
                    .getLatestBundleID(cluster1OC,
                        Util.readEntityName(b.getProcessData()), EntityType.PROCESS);

            List<String> oldNominalTimes = OozieUtil.getActionsNominalTime(cluster1OC,
                    oldBundleID,
                    EntityType.PROCESS);

            //update workflow.xml
            hadoopFileEditor = new HadoopFileEditor(cluster1FS);
            hadoopFileEditor.edit(b.getProcessObject().getWorkflow().getPath() + "/workflow.xml", "</workflow-app>",
                    "<!-- some comment -->");

            //update
            prism.getProcessHelper().update(b.getProcessData(),
                    b.getProcessData());

            TimeUtil.sleepSeconds(20);
            //verify new bundle creation
            OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleID, oldNominalTimes,
                    b.getProcessData(), true, true);

        } finally {
            b.deleteBundle(prism);
            if (hadoopFileEditor != null) {
                hadoopFileEditor.restore();
            }
        }

    }

    public ServiceResponse updateProcessConcurrency(Bundle bundle, int concurrency)
        throws Exception {
        String oldData = bundle.getProcessData();
        ProcessMerlin updatedProcess = new ProcessMerlin(bundle.getProcessObject());
        updatedProcess.setParallel(concurrency);

        return prism.getProcessHelper()
                .update(oldData, updatedProcess.toString());
    }

    /**
     * this method compares process xml definition from 2 falcon servers / prism and expects them to
     * be identical. If the definitions are identical then the definition from @param coloHelper1
     * is @return are response.
     */
    private String dualComparison(ColoHelper coloHelper1, ColoHelper coloHelper2,
            String processData) throws Exception {
        String colo1Response = getResponse(coloHelper1, processData, true);
        String colo2Response = getResponse(coloHelper2, processData, true);
        Assert.assertTrue(XmlUtil.isIdentical(colo1Response, colo2Response),
                "Process definition should have been identical");
        return getResponse(coloHelper1, processData, true);
    }

    /**
     * this method compares process xml definition from 2 falcon servers / prism and expects them to
     * be different.
     */
    private void dualComparisonFailure(ColoHelper coloHelper1, ColoHelper coloHelper2,
            String processData) throws Exception {
        Assert.assertFalse(XmlUtil.isIdentical(getResponse(coloHelper1, processData, true),
                getResponse(coloHelper2, processData, true)), "Process definition should not have been "
                + "identical");
    }

    private String getResponse(ColoHelper prism, String processData, boolean bool)
        throws Exception {
        ServiceResponse response = prism.getProcessHelper().getEntityDefinition(processData);
        if (bool) {
            AssertUtil.assertSucceeded(response);
        } else {
            AssertUtil.assertFailed(response);
        }
        String result = response.getMessage();
        Assert.assertNotNull(result);

        return result;

    }

    private void waitForProcessToReachACertainState(OozieClient oozieClient, Bundle bundle,
            Job.Status state)
        throws Exception {

        while (OozieUtil.getOozieJobStatus(oozieClient,
                bundle.getProcessName(), EntityType.PROCESS) != state) {
            //keep waiting
            TimeUtil.sleepSeconds(10);
        }

        //now check if the coordinator is in desired state
        CoordinatorJob coord = getDefaultOozieCoord(oozieClient, OozieUtil
                .getLatestBundleID(oozieClient, bundle.getProcessName(),
                    EntityType.PROCESS));

        while (coord.getStatus() != state) {
            TimeUtil.sleepSeconds(10);
            coord = getDefaultOozieCoord(oozieClient, OozieUtil
                    .getLatestBundleID(oozieClient, bundle.getProcessName(),
                        EntityType.PROCESS));
        }
    }

    private Bundle usualGrind(Bundle b) throws Exception {
        b.setInputFeedDataPath(inputFeedPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        final String starTime = TimeUtil.getTimeWrtSystemTime(3);
        String endTime = TimeUtil.getTimeWrtSystemTime(7);
        b.setProcessPeriodicity(1, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(1, TimeUnit.minutes);
        b.setProcessValidity(starTime, endTime);
        return b;
    }

    private ExecutionType getRandomExecutionType(Bundle bundle) throws Exception {
        ExecutionType current = bundle.getProcessObject().getOrder();
        Random r = new Random();
        ExecutionType[] values = ExecutionType.values();
        int i;
        do {

            i = r.nextInt(values.length);
        } while (current == values[i]);
        return values[i];
    }

    public ServiceResponse updateProcessFrequency(Bundle bundle,
            org.apache.falcon.entity.v0.Frequency frequency)
        throws Exception {
        String oldData = bundle.getProcessData();
        ProcessMerlin updatedProcess = new ProcessMerlin(bundle.getProcessObject());
        updatedProcess.setFrequency(frequency);
        return prism.getProcessHelper()
                .update(oldData, updatedProcess.toString());
    }

    //need to expand this function more later
    private int getExpectedNumberOfWorkflowInstances(String start, String end) {
        DateTime startDate = new DateTime(start);
        DateTime endDate = new DateTime(end);
        Minutes minutes = Minutes.minutesBetween((startDate), (endDate));
        return minutes.getMinutes();
    }

    private int getExpectedNumberOfWorkflowInstances(Date start, Date end) {
        DateTime startDate = new DateTime(start);
        DateTime endDate = new DateTime(end);
        Minutes minutes = Minutes.minutesBetween((startDate), (endDate));
        return minutes.getMinutes();
    }

    private int getExpectedNumberOfWorkflowInstances(String start, Date end) {
        DateTime startDate = new DateTime(start);
        DateTime endDate = new DateTime(end);
        Minutes minutes = Minutes.minutesBetween((startDate), (endDate));
        return minutes.getMinutes();
    }

    private void waitingForBundleFinish(ColoHelper coloHelper, String bundleId, int minutes)
        throws Exception {
        int wait = 0;
        while (!OozieUtil.isBundleOver(coloHelper, bundleId)) {
            //create missing dependencies if new instance have come up
            OozieUtil.createMissingDependenciesForBundle(coloHelper, bundleId);

            //keep waiting
            LOGGER.info("bundle " + bundleId + " not over .. waiting");
            TimeUtil.sleepSeconds(60);
            wait++;
            if (wait == minutes) {
                Assert.assertTrue(false);
                break;
            }
        }
    }

    private void waitingForBundleFinish(ColoHelper coloHelper, String bundleId) throws Exception {
        waitingForBundleFinish(coloHelper, bundleId, 15);
    }

    private CoordinatorJob getDefaultOozieCoord(OozieClient oozieClient, String bundleId) throws Exception {
        BundleJob bundlejob = oozieClient.getBundleJobInfo(bundleId);
        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if (coord.getAppName().contains("DEFAULT")) {
                return oozieClient.getCoordJobInfo(coord.getId());
            }
        }
        return null;
    }
}

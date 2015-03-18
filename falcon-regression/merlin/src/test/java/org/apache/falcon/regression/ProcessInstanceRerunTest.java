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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.enumsAndConstants.ResponseErrors;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Test Suite for instance rerun.
 */
@Test(groups = "embedded")
public class ProcessInstanceRerunTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestDir + "/output-data" + MINUTE_DATE_PATTERN;
    private String feedInputTimedOutPath = baseTestDir + "/timedout" + MINUTE_DATE_PATTERN;
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceRerunTest.class);
    private static final double TIMEOUT = 10;
    private String processName;
    private String start = "?start=2010-01-02T01:00Z";

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
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        processName = bundles[0].getProcessName();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedule process. Kill some instances. Rerun some of that killed. Check that
     * instances were rerun correctly and other are still killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunSomeKilled02() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
            start + "&end=2010-01-02T01:16Z");
        InstanceUtil.validateResponse(r, 4, 0, 0, 0, 4);
        List<String> wfIDs = InstanceUtil.getWorkflows(cluster, processName);
        prism.getProcessHelper().getProcessInstanceRerun(processName,
            start + "&end=2010-01-02T01:11Z");
        InstanceUtil.areWorkflowsRunning(clusterOC, wfIDs, 6, 5, 1, 0);
    }

    /**
     * Schedule process. Kill some instances. Rerun some of these killed without using -start or
     * -end parameters. Should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunKilledWOParams() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                start + "&end=2010-01-02T01:16Z");
        InstanceUtil.validateResponse(r, 4, 0, 0, 0, 4);
        r = prism.getProcessHelper().getProcessInstanceRerun(processName,
                null);
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Kill some instances. Rerun some of these killed using only
     * -end parameter. Should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunKilledWOStartParam() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                start + "&end=2010-01-02T01:16Z");
        InstanceUtil.validateResponse(r, 4, 0, 0, 0, 4);
        r = prism.getProcessHelper().getProcessInstanceRerun(processName,
                "?end=2010-01-02T01:11Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Kill some instances. Rerun some of these killed using only
     * -start parameter. Should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunKilledWOEndParam() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                start + "&end=2010-01-02T01:16Z");
        InstanceUtil.validateResponse(r, 4, 0, 0, 0, 4);
        r = prism.getProcessHelper().getProcessInstanceRerun(processName,
                start);
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Kill all instances. Rerun them. Check that they were rerun.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunMultipleKilled() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        String process = bundles[0].getProcessData();
        LOGGER.info("process: " + Util.prettyPrintXml(process));
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(processName, start + "&end=2010-01-02T01:11Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        List<String> wfIDs =  InstanceUtil.getWorkflows(cluster, processName);
        prism.getProcessHelper().
            getProcessInstanceRerun(processName, start + "&end=2010-01-02T01:11Z");
        InstanceUtil.areWorkflowsRunning(clusterOC, wfIDs, 3, 3, 0, 0);
    }

    /**
     * Schedule process. Kill some instances. Rerun them. Check that there are no killed
     * instances left.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunSomeKilled01() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(processName, start + "&end=2010-01-02T01:11Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        List<String> wfIDs = InstanceUtil.getWorkflows(cluster, processName);
        prism.getProcessHelper().getProcessInstanceRerun(processName,
            start + "&end=2010-01-02T01:11Z");
        TimeUtil.sleepSeconds(TIMEOUT);
        InstanceUtil.areWorkflowsRunning(clusterOC, wfIDs, 6, 6, 0, 0);
    }

    /**
     * Schedule process. Kill single instance. Rerun it. Check it was rerun.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunSingleKilled() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        prism.getProcessHelper().getProcessInstanceKill(processName,
                start + "&end=2010-01-02T01:01Z");
        String wfID = InstanceUtil.getWorkflows(cluster, processName, Status.KILLED).get(0);
        prism.getProcessHelper().getProcessInstanceRerun(processName,
                start + "&end=2010-01-02T01:01Z");
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(clusterOC, wfID));
    }

    /**
     * Schedule process. Wait till it got succeeded. Rerun first succeeded instance. Check if it
     * is running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunSingleSucceeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        String process = bundles[0].getProcessData();
        InstanceUtil.waitTillInstancesAreCreated(cluster, process, 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        String wfID = InstanceUtil.getWorkflows(cluster, processName, Status.RUNNING,
            Status.SUCCEEDED).get(0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 0, CoordinatorAction
            .Status.SUCCEEDED, EntityType.PROCESS);
        prism.getProcessHelper().getProcessInstanceRerun(processName,
                start + "&end=2010-01-02T01:01Z&force=true");
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(clusterOC, wfID));
    }

    /**
     * Schedule process. Suspend its instances. Try to rerun them. Check that instances weren't
     * rerun and are still suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunSingleSuspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:06Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(2);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 2,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            start + "&end=2010-01-02T01:06Z");
        prism.getProcessHelper().getProcessInstanceRerun(processName,
            start + "&end=2010-01-02T01:06Z");
        Assert.assertEquals(InstanceUtil.getInstanceStatus(cluster, processName, 0, 1),
                CoordinatorAction.Status.SUSPENDED);
    }

    /**
     * Schedule process. Wait till its instances succeed. Rerun them all. Check they are running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunMultipleSucceeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        List<String> wfIDs = InstanceUtil.getWorkflows(cluster, processName);
        prism.getProcessHelper().getProcessInstanceRerun(processName,
                start + "&end=2010-01-02T01:11Z&force=true");
        InstanceUtil.areWorkflowsRunning(clusterOC, wfIDs, 3, 3, 0, 0);
    }

    /**
     * Schedule process with invalid input feed data path. Wait till process got timed-out. Rerun
     * it's instances. Check that they were rerun and are waiting (wait for input data).
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerunTimedOut() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputTimedOutPath);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessTimeOut(2, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        CoordinatorAction.Status s;
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.TIMEDOUT, EntityType.PROCESS);
        prism.getProcessHelper().getProcessInstanceRerun(processName,
            start + "&end=2010-01-02T01:11Z");
        s = InstanceUtil.getInstanceStatus(cluster, processName, 0, 0);
        Assert.assertEquals(s, CoordinatorAction.Status.WAITING,
            "instance should have been in WAITING state");
    }
}

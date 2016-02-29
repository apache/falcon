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

package org.apache.falcon.regression.triage;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test Suite for process InstanceDependency corresponding to FALCON-1039.
 */
@Test(groups = { "distributed", "embedded" })
public class ProcessInstanceDependencyTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestDir + "/output-data" + MINUTE_DATE_PATTERN;
    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceDependencyTest.class);
    private String processName;

    private String startTime = "2015-06-06T09:37Z";
    private String endTime = "2015-06-06T10:37Z";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(prism);

        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        processName = bundles[0].getProcessName();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(groups = { "singleCluster" }, dataProvider = "getELData")
    public void testData(String[] elTime, String[] param) throws Exception {
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessPeriodicity(10, TimeUnit.minutes);
        bundles[0].setDatasetInstances(elTime[0], elTime[1]);
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1, CoordinatorAction.Status.SUCCEEDED,
                EntityType.PROCESS, 5);

        InstanceDependencyResult r = prism.getProcessHelper()
                .getInstanceDependencies(processName, "?instanceTime=" + param[0], null);

        if (param[1].equals("true")) {
            AssertUtil.assertSucceeded(r);
            InstanceUtil.assertProcessInstances(r, clusterOC,
                    OozieUtil.getBundles(clusterOC, processName, EntityType.PROCESS).get(0), param[0]);
        } else {
            AssertUtil.assertFailedInstance(r);
        }
    }

    @Test(groups = { "singleCluster" }, dataProvider = "getInstanceTime")
    public void testProcessWithOptionalInput(String instanceTime, String flag) throws Exception {

        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(10, TimeUnit.minutes);
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);

        ProcessMerlin process = bundles[0].getProcessObject();
        process.getInputs().getInputs().get(0).setOptional(true);

        bundles[0].setProcessData(process.toString());
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6, CoordinatorAction.Status.RUNNING,
                EntityType.PROCESS, 5);

        InstanceDependencyResult r = prism.getProcessHelper()
                .getInstanceDependencies(processName, "?instanceTime=" + instanceTime, null);

        if (flag.equals("true")) {
            AssertUtil.assertSucceeded(r);
            InstanceUtil.assertProcessInstances(r, clusterOC,
                    OozieUtil.getBundles(clusterOC, processName, EntityType.PROCESS).get(0), instanceTime);
        } else {
            AssertUtil.assertFailedInstance(r);
        }
    }

    @Test(groups = { "singleCluster" }, dataProvider = "getInstanceTime")
    public void testWithMultipleProcess(String instanceTime, String flag) throws Exception {

        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessPeriodicity(10, TimeUnit.minutes);
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1, CoordinatorAction.Status.RUNNING,
                EntityType.PROCESS, 5);

        ProcessMerlin process = new ProcessMerlin(bundles[0].getProcessObject().toString());
        process.setName("Process-producer-1");
        LOGGER.info("process : " + process.toString());

        prism.getProcessHelper().submitEntity(process.toString());
        InstanceDependencyResult r = prism.getProcessHelper()
                .getInstanceDependencies(processName, "?instanceTime=" + instanceTime, null);

        if (flag.equals("true")) {
            AssertUtil.assertSucceeded(r);
            InstanceUtil.assertProcessInstances(r, clusterOC,
                    OozieUtil.getBundles(clusterOC, processName, EntityType.PROCESS).get(0), instanceTime);
        } else {
            AssertUtil.assertFailedInstance(r);
        }
    }

    @DataProvider
    public Object[][] getInstanceTime() {
        return new Object[][] { { startTime, "true" },
            { TimeUtil.addMinsToTime(startTime, 10), "true" },
            { TimeUtil.addMinsToTime(startTime, 20), "true" },
            { TimeUtil.addMinsToTime(startTime, 30), "true" },
            { TimeUtil.addMinsToTime(startTime, 40), "true" },
            { TimeUtil.addMinsToTime(startTime, 50), "true" },
            { TimeUtil.addMinsToTime(startTime, 60), "false" },
            { TimeUtil.addMinsToTime(startTime, 80), "false" },
            { TimeUtil.addMinsToTime(startTime, -10), "false" },
            { TimeUtil.addMinsToTime(startTime, 25), "false" }, };
    }

    @DataProvider
    public Object[][] getELData() {
        String[][] elData = new String[][] { { "now(0,-30)", "now(0,30)" }, { "today(0,0)", "now(0,30)" }, };
        String[][] timeHelper = new String[][] { { startTime, "true" },
            { TimeUtil.addMinsToTime(startTime, 10), "true" },
            { TimeUtil.addMinsToTime(startTime, 20), "true" },
            { TimeUtil.addMinsToTime(startTime, 30), "true" },
            { TimeUtil.addMinsToTime(startTime, 40), "true" },
            { TimeUtil.addMinsToTime(startTime, 50), "true" },
            { TimeUtil.addMinsToTime(startTime, 60), "false" },
            { TimeUtil.addMinsToTime(startTime, 80), "false" },
            { TimeUtil.addMinsToTime(startTime, -10), "false" },
            { TimeUtil.addMinsToTime(startTime, 25), "false" }, };
        return MatrixUtil.crossProductNew(elData, timeHelper);
    }
}

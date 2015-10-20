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
import org.apache.falcon.entity.v0.process.LateInput;
import org.apache.falcon.entity.v0.process.LateProcess;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests related to update feature.
 */
@Test(groups = "embedded")
public class ProcessUpdateTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(ProcessUpdateTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
    }

    /**
     * Test for https://issues.apache.org/jira/browse/FALCON-99.
     * Scenario: schedule a process which doesn't have late data handling and then update it to have it.
     * Check that new coordinator was created.
     */
    @Test
    public void updateProcessWithLateData() throws Exception {
        String start = TimeUtil.getTimeWrtSystemTime(-60);
        String end = TimeUtil.getTimeWrtSystemTime(10);
        bundles[0].submitAndScheduleAllFeeds();
        ProcessMerlin process = bundles[0].getProcessObject();
        process.setValidity(start, end);
        process.setLateProcess(null);
        cluster.getProcessHelper().submitAndSchedule(process.toString());
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, process.toString(), 0);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, process.getName(), EntityType.PROCESS);

        //update process to have late data handling
        LateProcess lateProcess = new LateProcess();
        lateProcess.setDelay(new Frequency("hours(1)"));
        lateProcess.setPolicy(PolicyType.EXP_BACKOFF);
        LateInput lateInput = new LateInput();
        lateInput.setInput("inputData");
        lateInput.setWorkflowPath(aggregateWorkflowDir);
        lateProcess.getLateInputs().add(lateInput);
        process.setLateProcess(lateProcess);
        LOGGER.info("Updated process xml: " + Util.prettyPrintXml(process.toString()));
        AssertUtil.assertSucceeded(cluster.getProcessHelper().update(process.toString(), process.toString()));

        //check that new coordinator was created
        String newBundleId = OozieUtil.getLatestBundleID(clusterOC, process.getName(), EntityType.PROCESS);
        Assert.assertNotEquals(bundleId, newBundleId, "New Bundle should be created.");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

}

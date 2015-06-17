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
import org.apache.falcon.entity.v0.process.*;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.*;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Process late data test.
 */
@Test(groups = "embedded")
public class ProcessLateRerunTest extends BaseTestClass {
    private ColoHelper cluster1 = servers.get(0);
    private OozieClient cluster1OC = serverOC.get(0);
    private FileSystem cluster1FS = serverFS.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(ProcessLateRerunTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readLateDataBundle();
        bundles[0] = new Bundle(bundle, servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Test demonstrates rerunning process for late arrival of data.
     * Initially there is no input data and empty folders are processed.
     * It checks the number of rerun attempts once late data has been added
     * ensuring that late rerun happened.
     */
    @Test(enabled = true)
    public void testProcessLateRerunOnEmptyFolder() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 30);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(10, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(10, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(2);

        String inputName = bundles[0].getProcessObject().getFirstInputName();
        bundles[0].setProcessLatePolicy(getLateData(2, "minutes", "periodic", inputName, aggregateWorkflowDir));

        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        TimeUtil.sleepSeconds(10);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, bundles[0].getProcessData(), 0);

        getAndCreateDependencies(cluster1, bundles[0], cluster1OC, cluster1FS, false, 1);

        int sleepMins = 6;
        for(int i=0; i < sleepMins; i++) {
            LOGGER.info("Waiting...");
            TimeUtil.sleepSeconds(60);
        }
        InstanceUtil.waitTillInstanceReachState(cluster1OC, bundles[0].getProcessName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        List<String> bundleList =  OozieUtil.getBundles(cluster1.getFeedHelper().getOozieClient(),
            bundles[0].getProcessName(), EntityType.PROCESS);
        String bundleID = bundleList.get(0);

        OozieUtil.validateRetryAttempts(cluster1OC, bundleID, EntityType.PROCESS, 1);
    }

    /**
     * Test demonstrates rerunning process for late arrival of data.
     * Initially there is some data which is processed. It checks the number of rerun attempts
     * once further more data has been added ensuring that late rerun happened.
     */
    @Test(enabled = true)
    public void testProcessLateRerunWithData() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 30);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(2);

        String inputName = bundles[0].getProcessObject().getFirstInputName();

        bundles[0].setProcessLatePolicy(getLateData(4, "minutes", "periodic", inputName, aggregateWorkflowDir));
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        TimeUtil.sleepSeconds(10);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, bundles[0].getProcessData(), 0);

        getAndCreateDependencies(cluster1, bundles[0], cluster1OC, cluster1FS, true, 1);

        int sleepMins = 6;
        for(int i=0; i < sleepMins; i++) {
            LOGGER.info("Waiting...");
            TimeUtil.sleepSeconds(60);
        }
        InstanceUtil.waitTillInstanceReachState(cluster1OC, bundles[0].getProcessName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        List<String> bundleList =  OozieUtil.getBundles(cluster1.getFeedHelper().getOozieClient(),
            bundles[0].getProcessName(), EntityType.PROCESS);
        String bundleID = bundleList.get(0);

        OozieUtil.validateRetryAttempts(cluster1OC, bundleID, EntityType.PROCESS, 1);
    }

    /**
     * Test demonstrates rerunning process for late arrival of data for multiple input folders.
     * It checks the number of rerun attempts once further more data has been added ensuring that late rerun happened.
     */
    @Test(enabled = true)
    public void testProcessLateRerunWithMultipleFolders() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 30);
        String startInstance = "now(0,-5)";
        String endInstance = "now(0,0)";
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(10, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(10, Frequency.TimeUnit.minutes);
        String inputName = bundles[0].getProcessObject().getFirstInputName();

        bundles[0].setProcessLatePolicy(getLateData(4, "minutes", "periodic", inputName, aggregateWorkflowDir));
        bundles[0].setProcessConcurrency(2);

        // Increase the window of input for process
        bundles[0].setDatasetInstances(startInstance, endInstance);
        bundles[0].submitAndScheduleProcess();

        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        TimeUtil.sleepSeconds(10);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, bundles[0].getProcessData(), 0);

        getAndCreateDependencies(cluster1, bundles[0], cluster1OC, cluster1FS, false, 3);

        int sleepMins = 6;
        for(int i=0; i < sleepMins; i++) {
            LOGGER.info("Waiting...");
            TimeUtil.sleepSeconds(60);
        }
        InstanceUtil.waitTillInstanceReachState(cluster1OC, bundles[0].getProcessName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        List<String> bundleList =  OozieUtil.getBundles(cluster1.getFeedHelper().getOozieClient(),
            bundles[0].getProcessName(), EntityType.PROCESS);
        String bundleID = bundleList.get(0);

        OozieUtil.validateRetryAttempts(cluster1OC, bundleID, EntityType.PROCESS, 1);
    }

    /**
     * Test demonstrates rerunning process for late arrival of data for gate folders.
     * Late rerun will not work on gate folder, so no retry attempt on the appended data.
     */
    @Test(enabled = true)
    public void testProcessLateRerunWithGate() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 30);
        String startInstance = "now(0,-5)";
        String endInstance = "now(0,0)";
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(10, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(10, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(2);

        // Increase the window of input for process
        bundles[0].setDatasetInstances(startInstance, endInstance);

        ProcessMerlin process = bundles[0].getProcessObject();
        String inputName = process.getFirstInputName();
        Input tempFeed = process.getInputs().getInputs().get(0);

        Input gateInput = new Input();
        gateInput.setName("Gate");
        gateInput.setFeed(tempFeed.getFeed());
        gateInput.setEnd("now(0,1)");
        gateInput.setStart("now(0,1)");
        process.getInputs().getInputs().add(gateInput);
        bundles[0].setProcessData(process.toString());

        bundles[0].setProcessLatePolicy(getLateData(4, "minutes", "periodic", inputName, aggregateWorkflowDir));

        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        TimeUtil.sleepSeconds(10);
        InstanceUtil.waitTillInstancesAreCreated(cluster1OC, bundles[0].getProcessData(), 0);

        getAndCreateDependencies(cluster1, bundles[0], cluster1OC, cluster1FS, false, 7);

        int sleepMins = 6;
        for(int i=0; i < sleepMins; i++) {
            LOGGER.info("Waiting...");
            TimeUtil.sleepSeconds(60);
        }

        InstanceUtil.waitTillInstanceReachState(cluster1OC, bundles[0].getProcessName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        List<String> bundleList =  OozieUtil.getBundles(cluster1.getFeedHelper().getOozieClient(),
            bundles[0].getProcessName(), EntityType.PROCESS);
        String bundleID = bundleList.get(0);

        OozieUtil.validateRetryAttempts(cluster1OC, bundleID, EntityType.PROCESS, 0);
    }

    /*
    dataFlag - denotes whether process should run initially on empty folders or folders containing data
    dataFolder - denotes the folder where you want to upload data for late rerun
     */
    private void getAndCreateDependencies(ColoHelper prismHelper, Bundle bundle,
                                          OozieClient oozieClient, FileSystem clusterFS,
                                          boolean dataFlag, int dataFolder) {
        try {
            List<String> bundles = null;
            for (int i = 0; i < 10; ++i) {
                bundles = OozieUtil.getBundles(prismHelper.getFeedHelper().getOozieClient(),
                    bundle.getProcessName(), EntityType.PROCESS);
                if (bundles.size() > 0) {
                    break;
                }
                TimeUtil.sleepSeconds(30);
            }
            Assert.assertTrue(bundles != null && bundles.size() > 0, "Bundle job not created.");
            String bundleID = bundles.get(0);
            LOGGER.info("bundle id: " + bundleID);
            List<String> missingDependencies = OozieUtil.getMissingDependencies(oozieClient, bundleID);
            for (int i = 0; i < 10 && missingDependencies == null; ++i) {
                TimeUtil.sleepSeconds(30);
                missingDependencies = OozieUtil.getMissingDependencies(oozieClient, bundleID);
            }
            Assert.assertNotNull(missingDependencies, "Missing dependencies not found.");

            //print missing dependencies
            for (String dependency : missingDependencies) {
                LOGGER.info("dependency from job: " + dependency);
            }

            //create missing dependencies
            LOGGER.info("Creating missing dependencies...");
            OozieUtil.createMissingDependencies(prismHelper, EntityType.PROCESS, bundle.getProcessName(), 0, 0);

            //Adding data to empty folders depending on dataFlag
            if (dataFlag) {
                int tempCount = 1;
                for (String location : missingDependencies) {
                    if (tempCount==1) {
                        LOGGER.info("Transferring data to : " + location);
                        HadoopUtil.copyDataToFolder(clusterFS, location, OSUtil.NORMAL_INPUT + "dataFile.xml");
                        tempCount++;
                    }
                }
            }

            //Process succeeding on empty folders
            LOGGER.info("Waiting for process to succeed...");
            InstanceUtil.waitTillInstanceReachState(oozieClient, bundle.getProcessName(), 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

            TimeUtil.sleepSeconds(30);

            //Adding data to check late rerun
            int tempCounter = 1;
            for (String dependency : missingDependencies) {
                if (tempCounter==dataFolder) {
                    LOGGER.info("Transferring late data to : " + dependency);
                    HadoopUtil.copyDataToFolder(clusterFS, dependency, OSUtil.NORMAL_INPUT + "dataFile.properties");
                }
                tempCounter++;
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e);
        }
    }

    private static LateProcess getLateData(int delay, String delayUnits, String retryType,
                                           String inputData, String workflowDir) {
        LateInput lateInput = new LateInput();
        lateInput.setInput(inputData);
        lateInput.setWorkflowPath(workflowDir);
        LateProcess lateProcess = new LateProcess();
        lateProcess.setDelay(new Frequency(delayUnits + "(" + delay + ")"));
        lateProcess.setPolicy(PolicyType.fromValue(retryType));
        lateProcess.getLateInputs().add(lateInput);
        return lateProcess;
    }
}

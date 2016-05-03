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
package org.apache.falcon.regression.nativeScheduler;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Schedule process via native scheduler.
 */

@Test(groups = "distributed")
public class NativeScheduleTest extends BaseTestClass {
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(NativeScheduleTest.class);
    private String startTime;
    private String endTime;



    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.concat(OSUtil.RESOURCES, "sleep"));
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        startTime = TimeUtil.getTimeWrtSystemTime(-10);
        endTime = TimeUtil.addMinsToTime(startTime, 50);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        Bundle bundle = BundleUtil.readELBundle();

        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
            bundles[i].submitClusters(prism);
            bundles[i].setProcessConcurrency(2);
            bundles[i].setProcessValidity(startTime, endTime);
            bundles[i].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }


    /**
     * Successfully schedule process via native scheduler through prism and server on single cluster.
     * Schedule the same process on oozie. It should fail.
     */
    @Test
    public void scheduleProcessWithNativeUsingProperties() throws Exception {
        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setInputs(null);
        processMerlin.setOutputs(null);
        LOGGER.info(processMerlin.toString());

        ServiceResponse response = prism.getProcessHelper().submitEntity(processMerlin.toString());
        AssertUtil.assertSucceeded(response);

        // Schedule with prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

        // Schedule with server
        response = cluster1.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertFailed(response);

        // Schedule with oozie via prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertFailed(response);

        // Schedule with oozie via server
        response = cluster1.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

    }

    /**
     * Successfully schedule process via oozie scheduler (using properties) through prism and server on single cluster.
     * Schedule the same process on native scheduler. It should fail.
     */
    @Test
    public void scheduleProcessWithOozieUsingProperties() throws Exception {
        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setInputs(null);
        processMerlin.setOutputs(null);
        LOGGER.info(processMerlin.toString());

        ServiceResponse response = prism.getProcessHelper().submitEntity(processMerlin.toString());
        AssertUtil.assertSucceeded(response);

        // Schedule with prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertSucceeded(response);

        // Schedule with server
        response = cluster1.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertSucceeded(response);

        // Schedule with native via server
        response = cluster1.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertFailed(response);

        // Schedule with native via prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertFailed(response);

    }

    /**
     * Successfully schedule process via oozie scheduler(without properties) through prism and server on single cluster.
     * Schedule the same process on native using properties. It should fail.
     */
    @Test
    public void scheduleProcessWithOozieWithNoParams() throws Exception {
        ProcessMerlin processMerlin = bundles[0].getProcessObject();
        processMerlin.setInputs(null);
        processMerlin.setOutputs(null);
        LOGGER.info(processMerlin.toString());

        ServiceResponse response = prism.getProcessHelper().submitEntity(processMerlin.toString());
        AssertUtil.assertSucceeded(response);

        // Schedule with prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null, "");
        AssertUtil.assertSucceeded(response);

        // Schedule with native via server
        response = cluster1.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertFailed(response);

        // Schedule with native via prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertFailed(response);

    }

    /**
     * Successfully schedule process via native scheduler through prism and server on multiple cluster.
     * Schedule the same process on oozie. It should fail.
     */
    @Test(groups = {"prism", "0.2", "multiCluster"})
    public void scheduleProcessWithNativeOnTwoClusters() throws Exception {

        ProcessMerlin processMerlinNative = bundles[0].getProcessObject();
        processMerlinNative.clearProcessCluster();
        processMerlinNative.addProcessCluster(
                new ProcessMerlin.ProcessClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                        .withValidity(startTime, endTime).build());
        processMerlinNative.addProcessCluster(
                new ProcessMerlin.ProcessClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                        .withValidity(startTime, endTime).build());
        processMerlinNative.setInputs(null);
        processMerlinNative.setOutputs(null);
        LOGGER.info(processMerlinNative.toString());

        // Schedule with native via prism
        ServiceResponse response = prism.getProcessHelper().
                submitAndSchedule(processMerlinNative.toString(), null, "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

        // Schedule with native via server1
        response = cluster1.getProcessHelper().schedule(processMerlinNative.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

        // Schedule with native via server2
        response = cluster2.getProcessHelper().schedule(processMerlinNative.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

        // Schedule with oozie via prism
        response = prism.getProcessHelper().schedule(processMerlinNative.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertFailed(response);

        // Schedule with oozie via server1
        response = cluster1.getProcessHelper().schedule(processMerlinNative.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertFailed(response);

        // Schedule with oozie via server2
        response = cluster2.getProcessHelper().schedule(processMerlinNative.toString(), null,
                "properties=falcon.scheduler:oozie");
        AssertUtil.assertFailed(response);

    }

}

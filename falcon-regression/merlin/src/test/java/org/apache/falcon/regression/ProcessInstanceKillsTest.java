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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.InstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Process instance kill tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceKillsTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String testDir = "/ProcessInstanceKillsTest";
    private String baseTestHDFSDir = baseHDFSDir + testDir;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir +
        "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String feedOutputPath =
        baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceKillsTest.class);
    private static final double TIMEOUT = 15;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle b = BundleUtil.readELBundle();
        b.generateUniqueBundle();
        b = new Bundle(b, cluster);

        String startDate = "2010-01-01T23:20Z";
        String endDate = "2010-01-02T01:21Z";

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        LOGGER.info("test name: " + method.getName());

        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedInputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        LOGGER.info("tearDown " + method.getName());
        removeBundles();
    }

    /**
     * Schedule process. Perform -kill action using only -start parameter. Check that action
     * succeeded and only one instance was killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillSingle() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.KILLED);
    }

    /**
     * Schedule process. Check that in case when -start and -end parameters are equal -kill
     * action results in the same way as in case with only -start parameter is used. Only one
     * instance should be killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillStartAndEndSame() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
        bundles[0].setProcessConcurrency(2);
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(10);
        bundles[0].submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T00:03Z&end=2010-01-02T00:03Z");
        InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);
    }

    /**
     * Schedule process. Perform -kill action on instances between -start and -end dates which
     * expose range of last 3 instances which have been materialized already and those which
     * should be. Check that only existent instances are killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillKillNonMatrelized() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T00:03Z&end=2010-01-02T00:30Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        LOGGER.info(r.toString());
    }

    /**
     * Generate data. Schedule process. Try to perform -kill
     * operation using -start and -end which are both in future with respect to process start.
     *
     * @throws Exception TODO amend test with validations
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillBothStartAndEndInFuture01() throws Exception {
        /*
        both start and end r in future with respect to process start end
         */
        String startTime = TimeUtil.getTimeWrtSystemTime(-20);
        String endTime = TimeUtil.getTimeWrtSystemTime(400);
        String startTimeData = TimeUtil.getTimeWrtSystemTime(-50);
        String endTimeData = TimeUtil.getTimeWrtSystemTime(50);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTimeData, endTimeData, 1);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            baseTestHDFSDir + "/input01", dataDates);
        bundles[0].setInputFeedDataPath(feedInputPath.replace("input/","input01/"));
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        String startTimeRequest = TimeUtil.getTimeWrtSystemTime(-17);
        String endTimeRequest = TimeUtil.getTimeWrtSystemTime(23);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + startTimeRequest + "&end=" + endTimeRequest);
        LOGGER.info(r.toString());
    }

    /**
     * Schedule process. Check that -kill action is not performed when time range between -start
     * and -end parameters is in future and don't include existing instances.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillBothStartAndEndInFuture() throws Exception {
        /*
         both start and end r in future with respect to current time
          */
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:21Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        String startTime = TimeUtil.getTimeWrtSystemTime(1);
        String endTime = TimeUtil.getTimeWrtSystemTime(40);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime);
        LOGGER.info(r.getMessage());
        Assert.assertEquals(r.getInstances(), null);
    }

    /**
     * Schedule process. Perform -kill action within time range which includes 3 running instances.
     * Get status of instances within wider range. Check that only mentioned 3 instances are
     * killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillMultipleInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(TIMEOUT);
        prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:15Z");
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 2, 0, 0, 3);
    }

    /**
     * Schedule process. Perform -kill action on last expected instance. Get status of instances
     * which are in wider range. Check that only last is killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillLastInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(TIMEOUT);
        prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:20Z");
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 4, 0, 0, 1);
    }

    /**
     * Schedule process. Suspend one running instance. Perform -kill action on it. Check that
     * mentioned instance is really killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillSuspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.KILLED);
    }

    /**
     * Schedule single instance process. Wait till it finished. Try to kill the instance. Check
     * that instance still succeeded.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillSucceeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), Util.getProcessName(bundles[0]
            .getProcessData()), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.SUCCEEDED);
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        LOGGER.info("in @AfterClass");
        Bundle b = BundleUtil.readELBundle();
        b = new Bundle(b, cluster);
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }

}

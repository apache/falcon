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
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Process instance kill tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceKillsTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceKillsTest.class);
    private String processName;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        processName = bundles[0].getProcessName();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedule process. Perform -kill action for only one instance. Check that action
     * succeeded and only one instance was killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillSingle() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:01Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.KILLED);
    }

    /**
     * Schedule process. Check that in case when -start and -end parameters are equal zero
     * instances are killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillStartAndEndSame() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
        bundles[0].setProcessConcurrency(2);
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(10);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 2,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(processName, "?start=2010-01-02T00:03Z&end=2010-01-02T00:03Z");
        Assert.assertNull(r.getInstances(), "There should be zero instances killed");
    }

    /**
     * Schedule process. Provide data for all instances except the last,
     * thus making it non-materialized (waiting). Try to -kill last 3 instances.
     * Check that only running instances were affected.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillKillNotRunning() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T00:00Z", "2010-01-02T00:26Z");
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);

        //create data for first 5 instances, 6th should be non-materialized
        String bundleId = OozieUtil.getSequenceBundleID(clusterOC, processName,
            EntityType.PROCESS, 0);
        for(CoordinatorJob c : clusterOC.getBundleJobInfo(bundleId).getCoordinators()) {
            List<CoordinatorAction> actions = clusterOC.getCoordJobInfo(c.getId()).getActions();
            if (actions.size() == 6) {
                for(int i = 0; i < 5; i++) {
                    CoordinatorAction action = actions.get(i);
                    HadoopUtil.createHDFSFolders(cluster, Arrays
                        .asList(action.getMissingDependencies().split("#")));
                }
                break;
            }
        }
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 3);
        InstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(processName, "?start=2010-01-02T00:14Z&end=2010-01-02T00:26Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 1, 2);
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

        bundles[0].setInputFeedDataPath(feedInputPath.replace("input/", "input01/"));
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        String startTimeRequest = TimeUtil.getTimeWrtSystemTime(-17);
        String endTimeRequest = TimeUtil.getTimeWrtSystemTime(23);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
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
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        String startTime = TimeUtil.getTimeWrtSystemTime(1);
        String endTime = TimeUtil.getTimeWrtSystemTime(40);
        InstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(processName, "?start=" + startTime + "&end=" + endTime);
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
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        prism.getProcessHelper()
                .getProcessInstanceKill(processName, "?start=2010-01-02T01:05Z&end=2010-01-02T01:16Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(r, 5, 2, 0, 0, 3);
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
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 10);
        prism.getProcessHelper().getProcessInstanceKill(processName,
                "?start=2010-01-02T01:20Z&end=2010-01-02T01:21Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(r, 5, 4, 0, 0, 1);
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
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:04Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:04Z");
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
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), processName, 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:04Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.SUCCEEDED);
    }

    /**
     * Schedule process. Perform -kill action using only -start parameter. Check that action
     * succeeded and only one instance was killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillWOEndParam() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                "?start=2010-01-02T01:00Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Perform -kill action using only -end parameter. Check that action
     * succeeded and only one instance was killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillWOStartParam() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                "?end=2010-01-02T01:01Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Perform -kill action without start or end params. Check that action
     * succeeded and only one instance was killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKillWOParams() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceKill(processName,
                null);
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }
}

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
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;


/**
 * Process instance suspend tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceSuspendTest extends BaseTestClass {

    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String processName;
    private OozieClient clusterOC = serverOC.get(0);

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        processName = bundles[0].getProcessName();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        HadoopUtil.deleteDirIfExists(baseTestHDFSDir, clusterFS);
    }

    /**
     * Schedule process. Try to suspend instances with start/end parameters which are
     * wider then process validity range. Succeeds.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendLargeRange() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(result, 5, 5, 0, 0, 0);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
        result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
        InstanceUtil.validateResponse(result, 5, 0, 5, 0, 0);
    }

    /**
     * Schedule single-instance process. Wait till instance succeed. Try to suspend
     * succeeded instance. Action should be performed successfully as indempotent action.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendSucceeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:01Z");
        AssertUtil.assertSucceeded(r);
    }

    /**
     * Schedule process. Check that all instances are running. Suspend them. Check that all are
     * suspended. In every action valid time range is used.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendAll() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(result, 5, 5, 0, 0, 0);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(result, 5, 0, 5, 0, 0);
    }

    /**
     * Schedule process and try to perform -suspend action without date range parameters.
     * Attempt should fail. Will fail because of jira : https://issues.apache.org/jira/browse/FALCON-710
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendWoParams() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].submitFeedsScheduleProcess(prism);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceSuspend(processName, null);
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process with 3 running and 2 waiting instances expected. Suspend ones which are
     * running. Check that now 3 are suspended and 2 are still waiting.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendStartAndEnd() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
        InstanceUtil.validateResponse(result, 5, 3, 0, 2, 0);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
        InstanceUtil.validateResponse(result, 5, 0, 3, 2, 0);
    }

    /**
     * Try to suspend process which wasn't submitted and scheduled. Action should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendNonExistent() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceSuspend("invalidName", "?start=2010-01-02T01:20Z");
        InstanceUtil.validateError(r, ResponseErrors.PROCESS_NOT_FOUND);
    }

    /**
     * Schedule process. Perform -suspend action using only -start parameter which points to start
     * time of process. Attempt suspends all instances
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendOnlyStart() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Perform -suspend action using only -end parameter.
     * Should fail with appropriate status message.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendOnlyEnd() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceSuspend(processName,
                "?end=2010-01-02T01:05Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process with a number of instances running. Perform -suspend action using params
     * such that they aim to suspend the last instance. Check that only
     * the last instance is suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspendSuspendLast() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        InstancesResult result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(result, 5, 5, 0, 0, 0);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
                "?start=2010-01-02T01:20Z&end=2010-01-02T01:23Z");
        result = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(result, 5, 4, 1, 0, 0);
    }
}

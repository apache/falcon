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
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

/**
 * Process instance status tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceStatusTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private String feedInputTimedOutPath = baseTestHDFSDir + "/timedoutStatus"
        + MINUTE_DATE_PATTERN;
    private String feedOutputTimedOutPath =
        baseTestHDFSDir + "/output-data/timedoutStatus" + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceStatusTest.class);
    private static final double TIMEOUT = 15;
    private String processName;
    private OozieClient clusterOC = serverOC.get(0);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    /**
     *  Configures general process definition which particular properties can be overwritten.
     */
    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        processName = bundles[0].getProcessName();
        HadoopUtil.deleteDirIfExists(baseTestHDFSDir + "/input", clusterFS);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * time out is set as 3 minutes .... getStatus is for a large range in past.
     * 6 instance should be materialized and one in running and other in waiting
     * Adding logging information test as part of FALCON-813.
     * In case status does not contain jobId of instance the test should fail.
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusStartAndEndCheckNoInstanceAfterEndDate()
        throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-03T10:22Z");
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1, Status.RUNNING, EntityType.PROCESS);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T10:20Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
        InstanceUtil.validateResponse(r, 6, 1, 0, 5, 0);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Perform -getStatus using only -start parameter within time-range of non-materialized
     * instances. There should be no instances returned in response.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusOnlyStartAfterMat() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-03T10:22Z");
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T05:00Z");
        AssertUtil.assertSucceeded(r);
        Assert.assertEquals(r.getInstances(), null);
    }

    /**
     * Schedule process. Perform -getStatus using -end parameter which is out of process
     * validity range. Attempt should succeed with end defaulted to entity end.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusEndOutOfRange() throws Exception {
        HadoopUtil.deleteDirIfExists(baseTestHDFSDir + "/input", clusterFS);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:30Z");
        InstanceUtil.validateResponse(r, 5, 0, 0, 5, 0);
    }

    /**
     * Schedule process and try to -getStatus without date parameters. Attempt should succeed. Start defaults
     * to start of entity and end defaults to end of entity.
     * Adding logging information test as part of status information.
     * In case status does not contain jobId of instance the test should fail.
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusDateEmpty()
        throws JAXBException, AuthenticationException, IOException, URISyntaxException,
        OozieClientException, InterruptedException {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:30Z");
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), processName, 5,
            Status.RUNNING, EntityType.PROCESS);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName, null);
        InstanceUtil.validateResponse(r, 6, 5, 0, 1, 0);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Schedule process with number of instances. Perform -getStatus request with valid
     * parameters. Attempt should succeed.
     * Adding logging information test as part of status information.
     * In case status does not contain jobId of instance the test should fail.
    *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusStartAndEnd() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), processName, 1 ,
               Status.RUNNING, EntityType.PROCESS);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Schedule process. Perform -getStatus using -start parameter which is out of process
     * validity range. Attempt should succeed, with start defaulted to entity start time.
     * Adding logging information test as part of status information.
     * In case status does not contain jobId of instance the test should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusStartOutOfRange() throws Exception {
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            Status.RUNNING, EntityType.PROCESS, 5);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T00:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Schedule and then delete process. Try to get the status of its instances. Attempt should
     * fail with an appropriate code.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusKilled() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateError(r, ResponseErrors.PROCESS_NOT_FOUND);
    }

    /**
     * Schedule process and then suspend it. -getStatus of first instance only -start parameter.
     * Instance should be suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusOnlyStartSuspended() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1, Status.RUNNING, EntityType.PROCESS);
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z");
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(InstanceUtil.instancesInResultWithStatus(r, WorkflowStatus.SUSPENDED), 1);
    }

    /**
     * Schedule process. Try to -getStatus using -start/-end parameters with values which were
     * reversed i.e. -start is further than -end. Attempt should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusReverseDateRange() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 0);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), processName, 1,
            Status.RUNNING, EntityType.PROCESS);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:20Z&end=2010-01-02T01:07Z");
        InstanceUtil.validateError(r, ResponseErrors.START_BEFORE_SCHEDULED);
    }

    /**
     * Schedule process. Perform -getStatus using -start/-end parameters which are out of process
     * validity range. Attempt should succeed, with start/end defaulted to entity's start/end.
     * Adding logging information test as part of status information.
     * In case status does not contain jobId of instance the test should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusStartEndOutOfRange() throws Exception {
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(2);
        bundles[0].submitFeedsScheduleProcess(prism);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 2,
            Status.RUNNING, EntityType.PROCESS, 5);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
        InstanceUtil.validateResponse(r, 5, 2, 0, 3, 0);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Schedule process. Suspend and then resume it. -getStatus of its instances. Check that
     * response reflects that instances are running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusResumed() throws Exception {
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitFeedsScheduleProcess(prism);
        String process = bundles[0].getProcessData();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            Status.RUNNING, EntityType.PROCESS, 5);
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(process));
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.SUSPENDED);
        AssertUtil.assertSucceeded(prism.getProcessHelper().resume(process));
        TimeUtil.sleepSeconds(TIMEOUT);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            Status.RUNNING, EntityType.PROCESS, 5);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    /**
     * Schedule process. -getStatus of it's first instance using only -start parameter which
     * points to start time of process validity. Check that response reflects expected status of
     * instance.
     * Adding logging information test as part of status information.
     * In case status does not contain jobId of instance the test should fail.
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusOnlyStart() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
            Status.RUNNING, EntityType.PROCESS, 5);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z");
        InstanceUtil.validateResponse(r, 5, 1, 0, 4, 0);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Schedule process. Try to perform -getStatus using valid -start parameter but invalid
     * process name. Attempt should fail with an appropriate status code.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusInvalidName() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].submitFeedsScheduleProcess(prism);
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceStatus("invalidProcess", "?start=2010-01-01T01:00Z");
        InstanceUtil.validateError(r, ResponseErrors.PROCESS_NOT_FOUND);
    }

    /**
     * Schedule process. Try to -getStatus without time range parameters. Attempt succeeds.
     * Adding logging information test as part of status information.
     * In case status does not contain jobId of instance the test should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusWoParams() throws Exception {
        bundles[0].setProcessConcurrency(5);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        bundles[0].submitFeedsScheduleProcess(prism);
        String bundleId = OozieUtil.getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 5,
            Status.RUNNING, EntityType.PROCESS, 5);
        List<String> oozieWfIDs = OozieUtil.getWorkflow(clusterOC, bundleId);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName, null);
        InstanceUtil.validateResponse(r, 5, 5, 0, 0, 0);
        List<String> instanceWfIDs = InstanceUtil.getWorkflowJobIds(r);
        Assert.assertTrue(matchWorkflows(instanceWfIDs, oozieWfIDs), "No job ids exposed in status message");
    }

    /**
     * Schedule process with timeout set to 2 minutes. Wait till it become timed-out. -getStatus
     * of that process. Check that all materialized instances are failed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatusTimedOut() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputTimedOutPath);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessTimeOut(2, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputTimedOutPath);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3, Status.TIMEDOUT,
            EntityType.PROCESS);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.validateFailedInstances(r, 3);
    }

    /*
    * Function to match the workflows obtained from instance status and oozie.
    */
    private boolean matchWorkflows(List<String> instanceWf, List<String> oozieWf) {
        Collections.sort(instanceWf);
        Collections.sort(oozieWf);
        if (instanceWf.size() != oozieWf.size()) {
            return false;
        }
        for (int index = 0; index < instanceWf.size(); index++) {
            if (!instanceWf.get(index).contains(oozieWf.get(index))) {
                return false;
            }
        }
        return true;
    }

}

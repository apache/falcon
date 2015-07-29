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
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.enumsAndConstants.ResponseErrors;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Process instance resume tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceResumeTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceResumeTest.class);
    private String processName;
    private String wholeRange = "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z";

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
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        processName = bundles[0].getProcessName();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedule process. Suspend some instances. Attempt to -resume instance using single -end
     * parameter. Instances up to the end date (exclusive) will be resumed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeOnlyEnd() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            wholeRange);
        InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
        r = prism.getProcessHelper().getProcessInstanceResume(processName,
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        InstanceUtil.validateResponse(r, 3, 3, 0, 0, 0);
    }

    /**
     * Schedule process. Suspend some instances. Try to perform -resume using time range which
     * effects only on one instance. Check that this instance was resumed es expected.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeResumeSome() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            wholeRange);
        InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
        prism.getProcessHelper().getProcessInstanceResume(processName,
            "?start=2010-01-02T01:05Z&end=2010-01-02T01:16Z");
        r = prism.getProcessHelper().getProcessInstanceStatus(processName, wholeRange);
        InstanceUtil.validateResponse(r, 6, 5, 1, 0, 0);
    }

    /**
     * Schedule process. Suspend some instances. Try to perform -resume using time range which
     * effects on all instances. Check that there are no suspended instances.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeResumeMany() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        String withinRange = "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z";
        prism.getProcessHelper().getProcessInstanceSuspend(processName, withinRange);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            wholeRange);
        InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
        prism.getProcessHelper().getProcessInstanceResume(processName, withinRange);
        r = prism.getProcessHelper().getProcessInstanceStatus(processName, wholeRange);
        InstanceUtil.validateResponse(r, 6, 6, 0, 0, 0);
    }

    /**
     * Schedule process. Suspend first instance. Resume that instance using only -start parameter.
     * Check that mentioned instance was resumed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeSingle() throws Exception {
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 2);
        String param = "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z";
        prism.getProcessHelper().getProcessInstanceSuspend(processName, param);
        prism.getProcessHelper().getProcessInstanceResume(processName, param);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName, param);
        InstanceUtil.validateResponse(r, 6, 1, 0, 5, 0);
    }

    /**
     * Attempt to resume instances of non-existent process should fail with an appropriate
     * status code.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeNonExistent() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceResume("invalidName",
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        InstanceUtil.validateError(r, ResponseErrors.PROCESS_NOT_FOUND);
    }

    /**
     * Attempt to perform -resume action without time range parameters should fail with an
     + appropriate status code or message. Will fail now due to jira: https://issues.apache.org/jira/browse/FALCON-710
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeNoParams() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceResume(processName, null);
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Attempt to perform -resume action without -end parameter. Should fail with an
     + appropriate status code or message.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeWOEndParam() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceResume(processName, "?start=2010-01-02T01:00Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Attempt to perform -resume action without -start parameter. Should fail with an
     + appropriate status code or message.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeWOStartParam() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        InstancesResult r = prism.getProcessHelper().getProcessInstanceResume(processName, "?end=2010-01-02T01:15Z");
        InstanceUtil.validateError(r, ResponseErrors.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process, remove it. Try to -resume it's instance. Attempt should fail with
     * an appropriate status code.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeDeleted() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        prism.getProcessHelper().delete(bundles[0].getProcessData());
        InstancesResult r = prism.getProcessHelper().getProcessInstanceResume(processName,
            "?start=2010-01-02T01:05Z");
        InstanceUtil.validateError(r, ResponseErrors.PROCESS_NOT_FOUND);
    }

    /**
     * Schedule process. Try to resume entity which wasn't suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeNonSuspended() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        String start = "?start=2010-01-02T01:05Z&end=2010-01-02T01:26Z";
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName, start);
        InstanceUtil.validateResponse(r, 5, 5, 0, 0, 0);
        r = prism.getProcessHelper().getProcessInstanceResume(processName, start);
        InstanceUtil.validateResponse(r, 5, 5, 0, 0, 0);
    }

    /**
     * Schedule process. Suspend last instance. Resume it using parameter which points to
     * expected materialization time of last instance. Check that there are no suspended
     * instances among all which belong to current process.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResumeLastInstance() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 6,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        String last = "?start=2010-01-02T01:25Z&end=2010-01-02T01:26Z";
        prism.getProcessHelper().getProcessInstanceSuspend(processName, last);
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(processName,
            wholeRange);
        InstanceUtil.validateResponse(r, 6, 5, 1, 0, 0);
        prism.getProcessHelper().getProcessInstanceResume(processName, last);
        r = prism.getProcessHelper().getProcessInstanceStatus(processName, wholeRange);
        InstanceUtil.validateResponse(r, 6, 6, 0, 0, 0);
    }
}

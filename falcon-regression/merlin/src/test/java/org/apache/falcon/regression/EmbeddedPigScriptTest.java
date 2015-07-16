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
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ResponseErrors;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Embedded pig script test.
 */
@Test(groups = "embedded")
public class EmbeddedPigScriptTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String pigTestDir = cleanAndGetTestDir();
    private String pigScriptDir = pigTestDir + "/pig";
    private String pigScriptLocation = pigScriptDir + "/id.pig";
    private String inputPath = pigTestDir + "/input" + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(EmbeddedPigScriptTest.class);
    private static final double TIMEOUT = 15;
    private String processName;
    private String process;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");

        //copy pig script
        HadoopUtil.uploadDir(clusterFS, pigScriptDir, OSUtil.concat(OSUtil.RESOURCES, "pig"));
        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundle = new Bundle(bundle, cluster);
        String startDate = "2010-01-02T00:40Z";
        String endDate = "2010-01-02T01:10Z";
        bundle.setInputFeedDataPath(inputPath);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT,
            bundle.getFeedDataPathPrefix(), dataDates);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(inputPath);
        bundles[0].setOutputFeedLocationData(pigTestDir + "/output-data" + MINUTE_DATE_PATTERN);
        bundles[0].setProcessWorkflow(pigScriptLocation);
        bundles[0].setProcessInputNames("INPUT");
        bundles[0].setProcessOutputNames("OUTPUT");
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:10Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);

        final ProcessMerlin processElement = bundles[0].getProcessObject();
        processElement.clearProperties().withProperty("queueName", "default");
        processElement.getWorkflow().setEngine(EngineType.PIG);
        bundles[0].setProcessData(processElement.toString());
        bundles[0].submitFeedsScheduleProcess(prism);
        process = bundles[0].getProcessData();
        processName = Util.readEntityName(process);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(groups = {"singleCluster"}, timeOut = 600000)
    public void getResumedProcessInstance() throws Exception {
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);
        prism.getProcessHelper().suspend(process);
        TimeUtil.sleepSeconds(TIMEOUT);
        ServiceResponse status = prism.getProcessHelper().getStatus(process);
        Assert.assertTrue(status.getMessage().contains("SUSPENDED"), "Process not suspended.");
        prism.getProcessHelper().resume(process);
        TimeUtil.sleepSeconds(TIMEOUT);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);
        InstancesResult r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"}, timeOut = 600000)
    public void getSuspendedProcessInstance() throws Exception {
        prism.getProcessHelper().suspend(process);
        TimeUtil.sleepSeconds(TIMEOUT);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.SUSPENDED);
        InstancesResult r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @Test(groups = {"singleCluster"}, timeOut = 600000)
    public void getRunningProcessInstance() throws Exception {
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"}, timeOut = 600000)
    public void getKilledProcessInstance() throws Exception {
        prism.getProcessHelper().delete(process);
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateError(r, ResponseErrors.PROCESS_NOT_FOUND);
    }

    @Test(groups = {"singleCluster"}, timeOut = 6000000)
    public void getSucceededProcessInstance() throws Exception {
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);
        InstancesResult r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
        int counter = OSUtil.IS_WINDOWS ? 100 : 50;
        OozieUtil.waitForBundleToReachState(clusterOC, bundles[0].getProcessName(), Job.Status.SUCCEEDED, counter);
        r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateSuccessWOInstances(r);
    }
}

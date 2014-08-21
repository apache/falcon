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
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.InstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.response.ResponseKeys;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Embedded pig script test.
 */
@Test(groups = "embedded")
public class EmbeddedPigScriptTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    private String prefix;
    String pigTestDir = baseHDFSDir + "/EmbeddedPigScriptTest";
    String pigScriptDir = pigTestDir + "/EmbeddedPigScriptTest/pig";
    String pigScriptLocation = pigScriptDir + "/id.pig";
    String inputPath = pigTestDir + "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private static final Logger logger = Logger.getLogger(EmbeddedPigScriptTest.class);
    private static final double TIMEOUT = 15;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        logger.info("in @BeforeClass");
        //copy pig script
        HadoopUtil.uploadDir(clusterFS, pigScriptDir, OSUtil.RESOURCES + "pig");

        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, cluster);

        String startDate = "2010-01-02T00:40Z";
        String endDate = "2010-01-02T01:10Z";

        bundle.setInputFeedDataPath(inputPath);
        prefix = bundle.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        List<String> dataDates =
            TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setInputFeedDataPath(inputPath);
        bundles[0].setOutputFeedLocationData(
            pigTestDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessWorkflow(pigScriptLocation);
        bundles[0].setProcessInputNames("INPUT");
        bundles[0].setProcessOutputNames("OUTPUT");
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:10Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);

        final Process processElement = bundles[0].getProcessObject();
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.getProperties().add(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        bundles[0].setProcessData(processElement.toString());
        bundles[0].submitFeedsScheduleProcess(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(groups = {"singleCluster"})
    public void getResumedProcessInstance() throws Exception {
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        TimeUtil.sleepSeconds(TIMEOUT);
        ServiceResponse status =
            prism.getProcessHelper().getStatus(URLS.STATUS_URL, bundles[0].getProcessData());
        Assert.assertTrue(status.getMessage().contains("SUSPENDED"), "Process not suspended.");
        prism.getProcessHelper().resume(URLS.RESUME_URL, bundles[0].getProcessData());
        TimeUtil.sleepSeconds(TIMEOUT);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getSuspendedProcessInstance() throws Exception {
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        TimeUtil.sleepSeconds(TIMEOUT);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.SUSPENDED);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @Test(groups = {"singleCluster"})
    public void getRunningProcessInstance() throws Exception {
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getKilledProcessInstance() throws Exception {
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        Assert.assertEquals(r.getStatusCode(), ResponseKeys.PROCESS_NOT_FOUND,
            "Unexpected status code");
    }

    @Test(groups = {"singleCluster"})
    public void getSucceededProcessInstance() throws Exception {
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);

        int counter = OSUtil.IS_WINDOWS ? 100 : 50;
        InstanceUtil.waitForBundleToReachState(cluster, Util.getProcessName(bundles[0]
            .getProcessData()), Job.Status.SUCCEEDED, counter);
        r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        logger.info("in @AfterClass");
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }
}

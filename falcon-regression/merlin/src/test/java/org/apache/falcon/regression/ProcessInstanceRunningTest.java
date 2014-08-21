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
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.InstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.response.ResponseKeys;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Regression for instance running api.
 */
@Test(groups = "embedded")
public class ProcessInstanceRunningTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceRunningTest";
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir +
        "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String feedOutputPath =
        baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private static final Logger LOGGER = Logger.getLogger(ProcessInstanceRunningTest.class);
    private static final double TIMEOUT = 15;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, cluster);

        String startDate = "2010-01-02T00:40Z";
        String endDate = "2010-01-02T01:11Z";

        bundle.setInputFeedDataPath(feedInputPath);
        String prefix = bundle.getFeedDataPathPrefix();
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
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Run process. Suspend it and then resume. Get all -running instances. Response should
     * contain all process instances.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getResumedProcessInstance() throws Exception {
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        TimeUtil.sleepSeconds(TIMEOUT);
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(URLS.SUSPEND_URL,
            bundles[0].getProcessData()));
        TimeUtil.sleepSeconds(TIMEOUT);
        AssertUtil.assertSucceeded(prism.getProcessHelper().resume(URLS.RESUME_URL,
            bundles[0].getProcessData()));
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    /**
     * Run process. Suspend it. Try to get -running instances. Response should be
     * successful but shouldn't contain any instance.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getSuspendedProcessInstance() throws Exception {
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(URLS.SUSPEND_URL,
            bundles[0].getProcessData()));
        TimeUtil.sleepSeconds(TIMEOUT);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    /**
     * Run process. Get -running instances. Check that response contains expected number of
     * instances.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getRunningProcessInstance() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    /**
     * Attempt to get -running instances of nonexistent process should result in error.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getNonExistenceProcessInstance() throws Exception {
        InstancesResult r =
            prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, "invalidName");
        Assert.assertEquals(r.getStatusCode(), ResponseKeys.PROCESS_NOT_FOUND,
            "Unexpected status code");
    }

    /**
     * Attempt to get -running instances of deleted process should result in error.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getKilledProcessInstance() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        InstancesResult r = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        Assert.assertEquals(r.getStatusCode(), ResponseKeys.PROCESS_NOT_FOUND,
            "Unexpected status code");
    }

    /**
     * Launch process and wait till it got succeeded. Try to get -running instances. Response
     * should reflect success but shouldn't contain any of instances.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void getSucceededProcessInstance() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitForBundleToReachState(cluster, Util.getProcessName(bundles[0]
            .getProcessData()), Job.Status.SUCCEEDED);
        InstancesResult result = prism.getProcessHelper()
            .getRunningInstance(URLS.INSTANCE_RUNNING,
                Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccessWOInstances(result);
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

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

package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
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
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.List;


@Test(groups = "embedded")
public class RescheduleProcessInFinalStatesTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String baseTestDir = baseHDFSDir + "/RescheduleProcessInFinalStates";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    String inputPath = baseTestDir + "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private static final Logger logger = Logger.getLogger(RescheduleProcessInFinalStatesTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        logger.info("in @BeforeClass");
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle b = BundleUtil.readELBundle();
        b.generateUniqueBundle();
        b = new Bundle(b, cluster);
        b.setProcessWorkflow(aggregateWorkflowDir);

        String startDate = "2010-01-02T00:40Z";
        String endDate = "2010-01-02T01:20Z";

        b.setInputFeedDataPath(inputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setInputFeedDataPath(inputPath);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:15Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(
            baseTestDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitFeedsScheduleProcess(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Wait till process succeed and delete it. Check that entity is absent on server. Reschedule
     * it and check that it succeeds after some time.
     *
     * @throws Exception
     */
    @Test(enabled = true)
    public void rescheduleSucceeded() throws Exception {
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        checkNotFoundDefinition(bundles[0].getProcessData());

        //submit and schedule process again
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
    }

    /**
     * Fully duplicates rescheduleSucceeded().
     * TODO : modify test to match test case
     * Make process run into FAILED state. Delete it and check that entity was removed.
     * Run it again and check that process succeeds.
     *
     * @throws Exception
     */
    @Test(enabled = false)
    public void rescheduleFailed() throws Exception {
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        checkNotFoundDefinition(bundles[0].getProcessData());

        //submit and schedule process again
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
    }

    /**
     * Make process got DOWN WITH ERROR state. Delete it. Check that entity is absent on the
     * server. Reschedule it and check that it succeeds in some time.
     * DWE mean Done With Error In Oozie
     *
     * @throws Exception
     */
    @Test(enabled = true)
    public void rescheduleDWE() throws Exception {
        prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z");
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.DONEWITHERROR);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        checkNotFoundDefinition(bundles[0].getProcessData());

        //submit and schedule process again
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
    }

    /**
     * Make process run into DOWN WITH ERROR state. Delete it. Check that entity is absent on the
     * server. Reschedule it and check that it succeeds in some time.
     **/
    @Test(enabled = true)
    public void rescheduleKilled() throws Exception {
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.KILLED);
        checkNotFoundDefinition(bundles[0].getProcessData());

        //submit and schedule process again
        AssertUtil.assertSucceeded(prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
    }

    /**
     * Tries to get entity definition and checks it is absent (-get definition should return
     * process not found)
     *
     * @param process process entity definition
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     * @throws JAXBException
     */
    private void checkNotFoundDefinition(String process)
        throws URISyntaxException, IOException, AuthenticationException, JAXBException {
        ServiceResponse r = prism.getProcessHelper()
            .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, process);
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        AssertUtil.assertFailed(r);
    }
}

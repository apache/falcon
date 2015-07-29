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
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.FreqType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Test process with different frequency combinations.
 */
@Test(groups = "embedded")
public class ProcessFrequencyTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ProcessFrequencyTest.class);
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";

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
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Test Process submission with different frequency. Expecting process workflow to run
     * successfully.
     * @throws Exception
     */
    @Test(dataProvider = "generateProcessFrequencies")
    public void testProcessWithFrequency(final FreqType freqType, final int freqAmount)
        throws Exception {
        final String startDate = "2010-01-02T01:00Z";
        final String endDate = "2010-01-02T01:01Z";
        final String inputPath = baseTestHDFSDir + "/input/";
        bundles[0].setInputFeedDataPath(inputPath + freqType.getPathValue());
        bundles[0].setOutputFeedLocationData(
            baseTestHDFSDir + "/output-data/" + freqType.getPathValue());
        bundles[0].setProcessPeriodicity(freqAmount, freqType.getFalconTimeUnit());
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].setProcessValidity(startDate, endDate);
        HadoopUtil.deleteDirIfExists(inputPath, clusterFS);
        bundles[0].submitFeedsScheduleProcess(prism);

        //upload data
        final String startPath = inputPath + freqType.getFormatter().print(
            TimeUtil.oozieDateToDate(startDate));
        HadoopUtil.copyDataToFolder(clusterFS, startPath, OSUtil.NORMAL_INPUT);

        final String processName = bundles[0].getProcessName();
        //InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 5);
        InstancesResult r = prism.getProcessHelper().getRunningInstance(processName);
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @DataProvider(name = "generateProcessFrequencies")
    public Object[][] generateProcessFrequencies() {
        return new Object[][] {
            {FreqType.MINUTELY, 2, },
            {FreqType.HOURLY, 3, },
            {FreqType.DAILY, 5, },
            {FreqType.MONTHLY, 7, },
        };
    }

    /**
     * Test Process submission with bad frequency. Expecting submissions to fails.
     * @throws Exception
     */
    @Test
    public void testProcessWithBadFrequency()
        throws Exception {
        final String startDate = "2010-01-02T01:00Z";
        final String endDate = "2010-01-02T01:01Z";
        final String inputPath = baseTestHDFSDir + "/input/";
        final FreqType freqType = FreqType.MINUTELY;
        bundles[0].setInputFeedDataPath(inputPath + freqType.getPathValue());
        bundles[0].setOutputFeedLocationData(
            baseTestHDFSDir + "/output-data/" + freqType.getPathValue());
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);

        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].setProcessValidity(startDate, endDate);
        final ProcessMerlin processMerlin = bundles[0].getProcessObject();
        //a frequency can be bad in two ways - it can have bad amount or it can have bad unit
        //submit process with bad amount
        processMerlin.setFrequency(new Frequency("BadAmount", freqType.getFalconTimeUnit()));
        AssertUtil.assertFailed(prism.getProcessHelper().submitEntity(processMerlin.toString()));

        //submit process with bad unit
        processMerlin.setFrequency(new Frequency("2993", freqType.getFalconTimeUnit()));
        final String process = processMerlin.toString();
        final String newProcess = process.replaceAll("minutes\\(2993\\)", "BadUnit(2993)");
        AssertUtil.assertFailed(prism.getProcessHelper().submitEntity(newProcess));
    }

}

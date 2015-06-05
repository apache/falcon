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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * EL Expression test.
 */
@Test(groups = "embedded")
public class ELExpFutureAndLatestTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(ELExpFutureAndLatestTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle b = BundleUtil.readELBundle();
        b.generateUniqueBundle(this);
        b = new Bundle(b, cluster);

        String startDate = TimeUtil.getTimeWrtSystemTime(-20);
        String endDate = TimeUtil.getTimeWrtSystemTime(70);

        b.setInputFeedDataPath(baseTestDir + "/testData" + MINUTE_DATE_PATTERN);
        b.setProcessWorkflow(aggregateWorkflowDir);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 1);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT,
            b.getFeedDataPathPrefix(), dataDates);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(baseTestDir + "/testData" + MINUTE_DATE_PATTERN);
        bundles[0].setOutputFeedLocationData(baseTestDir + "/output" + MINUTE_DATE_PATTERN);
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setInputFeedValidity("2010-04-01T00:00Z", "2099-04-01T00:00Z");
        String processStart = TimeUtil.getTimeWrtSystemTime(-3);
        String processEnd = TimeUtil.getTimeWrtSystemTime(8);
        LOGGER.info("processStart: " + processStart + " processEnd: " + processEnd);
        bundles[0].setProcessValidity(processStart, processEnd);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(groups = {"singleCluster"})
    public void latestTest() throws Exception {
        bundles[0].setDatasetInstances("latest(-3)", "latest(0)");
        AssertUtil.assertSucceeded(bundles[0].submitFeedsScheduleProcess(prism));
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }

    @Test(groups = {"singleCluster"})
    public void futureTest() throws Exception {
        bundles[0].setDatasetInstances("future(0,10)", "future(3,10)");
        AssertUtil.assertSucceeded(bundles[0].submitFeedsScheduleProcess(prism));
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }
}

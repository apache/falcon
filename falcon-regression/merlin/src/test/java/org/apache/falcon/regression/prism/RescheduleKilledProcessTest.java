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

import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Tests with rescheduling killed process.
 */
@Test(groups = "embedded")
public class RescheduleKilledProcessTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(RescheduleKilledProcessTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
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
     *  Run process and delete it. Submit and schedule once more.
     *
     * @throws Exception
     */
    @Test(enabled = false, timeOut = 1200000)
    public void rescheduleKilledProcess() throws Exception {
        String processStartTime = TimeUtil.getTimeWrtSystemTime(-11);
        String processEndTime = TimeUtil.getTimeWrtSystemTime(6);
        ProcessMerlin process = bundles[0].getProcessObject();
        process.setName(Util.getEntityPrefix(this) + "-zeroInputProcess"
            + new Random().nextInt());
        List<String> feed = new ArrayList<>();
        feed.add(bundles[0].getOutputFeedFromBundle());
        process.setProcessFeeds(feed, 0, 0, 1);

        process.clearProcessCluster();
        process.addProcessCluster(
            new ProcessMerlin.ProcessClusterBuilder(
                Util.readEntityName(bundles[0].getClusters().get(0)))
                .withValidity(processStartTime, processEndTime)
                .build()
        );
        bundles[0].setProcessData(process.toString());

        bundles[0].submitFeedsScheduleProcess(prism);

        String processData = bundles[0].getProcessData();
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().schedule(processData));
    }

    /**
     * Submit and schedule a process. Then remove it. Repeat all procedure twice.
     *
     * @throws Exception
     */
    @Test(enabled = true, timeOut = 1200000)
    public void rescheduleKilledProcess02() throws Exception {
        bundles[0].setProcessValidity(TimeUtil.getTimeWrtSystemTime(-11),
            TimeUtil.getTimeWrtSystemTime(6));

        bundles[0].setInputFeedDataPath(baseTestHDFSDir + "/rawLogs" + MINUTE_DATE_PATTERN);

        LOGGER.info("process: " + Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitFeedsScheduleProcess(prism);
        String processData = bundles[0].getProcessData();
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().schedule(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(processData));
        AssertUtil.assertSucceeded(prism.getProcessHelper().schedule(processData));
    }
}

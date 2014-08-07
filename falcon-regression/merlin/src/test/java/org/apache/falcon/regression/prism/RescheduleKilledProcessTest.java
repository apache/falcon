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
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Test(groups = "embedded")
public class RescheduleKilledProcessTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    String aggregateWorkflowDir = baseHDFSDir + "/RescheduleKilledProcessTest/aggregator";
    private static final Logger logger = Logger.getLogger(RescheduleKilledProcessTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
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
        String process = bundles[0].getProcessData();
        process = InstanceUtil.setProcessName(process, "zeroInputProcess" + new Random().nextInt());
        List<String> feed = new ArrayList<String>();
        feed.add(bundles[0].getOutputFeedFromBundle());
        final ProcessMerlin processMerlin = new ProcessMerlin(process);
        processMerlin.setProcessFeeds(feed, 0, 0, 1);
        process = processMerlin.toString();

        process = InstanceUtil.setProcessCluster(process, null,
            XmlUtil.createProcessValidity(processStartTime, "2099-01-01T00:00Z"));
        process = InstanceUtil
            .setProcessCluster(process, Util.readEntityName(bundles[0].getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));
        bundles[0].setProcessData(process);

        bundles[0].submitFeedsScheduleProcess(prism);

        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundles[0].getProcessData()));
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

        bundles[0].setInputFeedDataPath(
            baseHDFSDir + "/rawLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        logger.info("process: " + Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitFeedsScheduleProcess(prism);

        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL,
            bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            bundles[0].getProcessData()));
    }
}

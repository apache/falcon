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

package org.apache.falcon.regression.triage;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test Suite for feed InstanceDependency corresponding to FALCON-1039.
 */
@Test(groups = "embedded")
public class FeedInstanceDependencyTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestDir + "/output-data" + MINUTE_DATE_PATTERN;
    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private static final Logger LOGGER = Logger.getLogger(FeedInstanceDependencyTest.class);
    private String processName;

    private String startTime = "2015-06-06T09:37Z";
    private String endTime = "2015-06-06T10:37Z";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(prism);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        processName = bundles[0].getProcessName();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(groups = { "singleCluster" }, dataProvider = "testDataProvider")
    public void testData(String... processTime) throws Exception {
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessPeriodicity(10, TimeUnit.minutes);
        bundles[0].setInputFeedValidity("2014-01-01T01:00Z", "2016-12-12T22:00Z");
        bundles[0].setOutputFeedValidity("2014-01-01T01:00Z", "2016-12-12T22:00Z");
        bundles[0].setDatasetInstances("now(0,-20)", "now(0,20)");
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess(prism);
        String[] expTime = new String[processTime.length - 3];
        System.arraycopy(processTime, 3, expTime, 0, processTime.length - 3);

        List<String> expectedTime = Arrays.asList(expTime);

        InstanceDependencyResult r = null;
        if (processTime[1].equals("Input")) {
            r = prism.getFeedHelper()
                    .getInstanceDependencies(bundles[0].getInputFeedNameFromBundle(), "?instanceTime=" + processTime[2],
                            null);
        }
        if (processTime[1].equals("Output")) {
            r = prism.getFeedHelper().getInstanceDependencies(bundles[0].getOutputFeedNameFromBundle(),
                    "?instanceTime=" + processTime[2], null);
        }

        if (processTime[0].equals("true") && r != null) {
            AssertUtil.assertSucceeded(r);
            InstanceUtil.assertFeedInstances(r, processName, processTime[1], expectedTime);
        } else if (processTime[0].equals("emptyMessage") && r != null) {
            AssertUtil.assertSucceeded(r);
        } else {
            AssertUtil.assertFailedInstance(r);
        }
    }

    @DataProvider
    public static Object[][] testDataProvider() {
        return new Object[][] {
            new String[] { "true", "Input", "2015-06-06T09:35Z", "2015-06-06T09:37Z", "2015-06-06T09:47Z",
                "2015-06-06T09:57Z", },
            new String[] { "true", "Input", "2015-06-06T09:40Z", "2015-06-06T09:37Z", "2015-06-06T09:47Z",
                "2015-06-06T09:57Z", },
            new String[] { "true", "Input", "2015-06-06T09:45Z", "2015-06-06T09:37Z", "2015-06-06T09:47Z",
                "2015-06-06T09:57Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T09:50Z", "2015-06-06T09:37Z", "2015-06-06T09:47Z",
                "2015-06-06T09:57Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:00Z", "2015-06-06T10:17Z", "2015-06-06T09:57Z",
                "2015-06-06T09:47Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:05Z", "2015-06-06T10:17Z", "2015-06-06T09:57Z",
                "2015-06-06T10:27Z", "2015-06-06T09:47Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:10Z", "2015-06-06T10:17Z", "2015-06-06T09:57Z",
                "2015-06-06T10:27Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:15Z", "2015-06-06T10:17Z", "2015-06-06T09:57Z",
                "2015-06-06T10:27Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:20Z", "2015-06-06T10:17Z", "2015-06-06T10:27Z",
                "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:25Z", "2015-06-06T10:17Z", "2015-06-06T10:27Z",
                "2015-06-06T10:07Z", },
            new String[] { "true", "Input", "2015-06-06T10:30Z", "2015-06-06T10:17Z", "2015-06-06T10:27Z", },
            new String[] { "true", "Input", "2015-06-06T10:35Z", "2015-06-06T10:17Z", "2015-06-06T10:27Z", },
            new String[] { "true", "Input", "2015-06-06T10:40Z", "2015-06-06T10:27Z", },

            new String[] { "true", "Output", "2015-06-06T09:35Z", "2015-06-06T09:37Z", },
            new String[] { "true", "Output", "2015-06-06T09:45Z", "2015-06-06T09:47Z", },
            new String[] { "true", "Output", "2015-06-06T09:55Z", "2015-06-06T09:57Z", },
            new String[] { "true", "Output", "2015-06-06T10:05Z", "2015-06-06T10:07Z", },
            new String[] { "true", "Output", "2015-06-06T10:15Z", "2015-06-06T10:17Z", },
            new String[] { "true", "Output", "2015-06-06T10:25Z", "2015-06-06T10:27Z", },

            new String[] { "emptyMessage", "Output", "2015-06-06T09:40Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T09:50Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:00Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:10Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:20Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:30Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:35Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:40Z", },
            new String[] { "emptyMessage", "Output", "2015-06-06T10:45Z", },
            new String[] { "false", "Output", "2017-06-06T10:45Z", },
            new String[] { "false", "Output", "2013-06-06T10:45Z", },
            new String[] { "false", "Output", "2017-06-06T10:48Z", },
            new String[] { "false", "Output", "2013-06-06T10:51Z", },

            new String[] { "false", "Input", "2017-06-06T10:45Z", },
            new String[] { "false", "Input", "2013-06-06T10:45Z", },
            new String[] { "false", "Input", "2017-06-06T10:48Z", },
            new String[] { "false", "Input", "2013-06-06T10:51Z", },

            new String[] { "false", "Output", "2015-06-06T09:51Z", },
            new String[] { "false", "Input", "2015-06-06T09:51Z", },
        };
    }

    @Test(groups = { "singleCluster" })
    public void testMultipleData() throws Exception {

        bundles[0].setProcessValidity("2015-06-06T09:35Z", "2015-06-06T09:45Z");
        bundles[0].setProcessConcurrency(6);

        bundles[0].setProcessPeriodicity(10, TimeUnit.minutes);
        bundles[0].setInputFeedValidity("2014-01-01T01:00Z", "2016-12-12T22:00Z");
        bundles[0].setOutputFeedValidity("2014-01-01T01:00Z", "2016-12-12T22:00Z");
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);

        ProcessMerlin processFirst = new ProcessMerlin(bundles[0].getProcessObject().toString());
        processFirst.setName("Process-producer-1");
        LOGGER.info("process : " + processFirst.toString());

        prism.getProcessHelper().submitEntity(processFirst.toString());

        ProcessMerlin processSecond = new ProcessMerlin(bundles[0].getProcessObject().toString());
        processSecond.setName("Process-producer-2");
        LOGGER.info("process : " + processSecond.toString());

        prism.getProcessHelper().submitEntity(processSecond.toString());

        InstanceDependencyResult r;

        // For Input feed
        r = prism.getFeedHelper()
                .getInstanceDependencies(bundles[0].getInputFeedNameFromBundle(), "?instanceTime=2015-06-06T09:45Z",
                        null);
        AssertUtil.assertSucceeded(r);

        // For Output Feed
        r = prism.getFeedHelper()
                .getInstanceDependencies(bundles[0].getOutputFeedNameFromBundle(), "?instanceTime=2015-06-06T09:45Z",
                        null);
        AssertUtil.assertSucceeded(r);
    }

}

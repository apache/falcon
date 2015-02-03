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
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * EL Expression Current and last week test.
 */

@Test(groups = "embedded")
public class ELExpCurrentAndLastWeekTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(ELExpCurrentAndLastWeekTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundle = new Bundle(bundle, cluster);

        bundle.setInputFeedDataPath(baseTestDir + "/testData" + MINUTE_DATE_PATTERN);
        bundle.setProcessWorkflow(aggregateWorkflowDir);

    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        String processStart = "2015-02-17T10:30Z";
        String processEnd = "2015-02-17T10:50Z";
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(baseTestDir + "/testData" + MINUTE_DATE_PATTERN);
        bundles[0].setOutputFeedLocationData(baseTestDir + "/output" + MINUTE_DATE_PATTERN);
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setInputFeedValidity("2010-04-01T00:00Z", "2099-04-01T00:00Z");
        LOGGER.info("processStart: " + processStart + " processEnd: " + processEnd);
        bundles[0].setProcessValidity(processStart, processEnd);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /* Test cases to check currentWeek and lastWeek EL-Expressions.
    * Finding the missing dependencies of coordiantor based on
    * given el definition in entity and creating them.
    * These should match with the expected missing dependencies.
    * In case they dont match, the test should fail.
    *
    * */
    @Test(groups = {"singleCluster"}, dataProvider = "EL-DP-Cases")
    public void currentAndLastWeekTest(String startInstance, String endInstance,
            String firstDep, String endDep) throws Exception {
        bundles[0].setDatasetInstances(startInstance, endInstance);

        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);

        List<String> missingDependencies = getMissingDependencies(cluster, bundles[0]);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, bundles[0].getProcessName(), 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        List<String> qaDependencies = getQADepedencyList(bundles[0], firstDep, endDep);
        Assert.assertTrue(matchDependencies(missingDependencies, qaDependencies));
    }

    @DataProvider(name = "EL-DP-Cases")
    public Object[][] getELData() {
        return new Object[][]{
            {"currentWeek('WED',2,15)", "currentWeek('WED',2,25)", "2015-02-11T02:15Z", "2015-02-11T02:25Z"},
            {"currentWeek('WED',21,60)", "currentWeek('WED',22,10)", "2015-02-11T22:00Z", "2015-02-11T22:10Z"},
            {"currentWeek('WED',24,60)", "currentWeek('THU',01,10)", "2015-02-12T01:00Z", "2015-02-12T01:10Z"},
            {"currentWeek('WED',04,-60)", "currentWeek('WED',04,10)", "2015-02-11T03:00Z", "2015-02-11T04:10Z"},
            {"currentWeek('SAT',-04,-60)", "currentWeek('SAT',-04,-40)", "2015-02-13T19:00Z", "2015-02-13T19:20Z"},
            {"currentWeek('SAT',-24,-60)", "currentWeek('SAT',-24,-40)", "2015-02-12T23:00Z", "2015-02-12T23:20Z"},
            {"lastWeek('THU',-24,-20)", "lastWeek('THU',-24,-05)", "2015-02-03T23:40Z", "2015-02-03T23:55Z"},
            {"lastWeek('WED',2,15)", "lastWeek('WED',2,25)", "2015-02-04T02:15Z", "2015-02-04T02:25Z"},
            {"lastWeek('WED',21,60)", "lastWeek('WED',22,10)", "2015-02-04T22:00Z", "2015-02-04T22:10Z"},
            {"lastWeek('WED',24,60)", "lastWeek('THU',01,10)", "2015-02-05T01:00Z", "2015-02-05T01:10Z"},
            {"lastWeek('WED',04,-60)", "lastWeek('WED',04,10)", "2015-02-04T03:00Z", "2015-02-04T04:10Z"},
            {"lastWeek('FRI',01,05)", "lastWeek('FRI',01,20)", "2015-02-06T01:05Z", "2015-02-06T01:20Z"},
            {"lastWeek('FRI',01,60)", "lastWeek('FRI',02,20)", "2015-02-06T02:00Z", "2015-02-06T02:20Z"},
            {"lastWeek('FRI',24,00)", "lastWeek('SAT',00,20)", "2015-02-07T00:00Z", "2015-02-07T00:20Z"},
            {"lastWeek('THU',-04,-20)", "lastWeek('THU',-04,-05)", "2015-02-04T19:40Z", "2015-02-04T19:55Z"},
        };
    }

    private boolean matchDependencies(List<String> fromJob, List<String> qaList) {
        Collections.sort(fromJob);
        Collections.sort(qaList);
        if (fromJob.size() != qaList.size()) {
            return false;
        }
        for (int index = 0; index < fromJob.size(); index++) {
            if (!fromJob.get(index).contains(qaList.get(index))) {
                return false;
            }
        }
        return true;
    }

    private List<String> getMissingDependencies(ColoHelper prismHelper, Bundle bundle) throws OozieClientException {
        List<String> bundles = OozieUtil.getBundles(prismHelper.getFeedHelper().getOozieClient(),
                bundle.getProcessName(), EntityType.PROCESS);
        String coordID = bundles.get(0);
        List<String> missingDependencies =
                OozieUtil.getMissingDependencies(prismHelper, coordID);
        for (int i = 0; i < 10 && missingDependencies == null; ++i) {
            TimeUtil.sleepSeconds(30);
            missingDependencies = OozieUtil.getMissingDependencies(prismHelper, coordID);
        }
        Assert.assertNotNull(missingDependencies, "Missing dependencies not found.");
        return missingDependencies;
    }

    private List<String> getQADepedencyList(Bundle bundle, String firstDep, String endDep) {
        String path = baseTestDir + "/testData/";
        List<String> returnList = new ArrayList<String>();
        List<String> dataSets = TimeUtil.getMinuteDatesOnEitherSide(firstDep,
                endDep, bundle.getInitialDatasetFrequency());
        for (String str : dataSets) {
            returnList.add(path + str);
        }
        return returnList;
    }

}

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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Test delays in feed.
 */
@Test(groups = "distributed")
public class FeedDelayTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(FeedDelayTest.class);
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private FileSystem cluster1FS = serverFS.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String targetPath = baseTestDir + "/target";
    private String targetDataLocation = targetPath + MINUTE_DATE_PATTERN;
    private String sourcePath = baseTestDir + "/source";
    private String feedInputPath = sourcePath + MINUTE_DATE_PATTERN;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);

        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        cleanTestsDirs();
    }

    /* Test cases to check delay feature in feed.
    * Finding the missing dependencies of coordiantor based on
    * given delay in entity and creating them.
    * These should match with the expected missing dependencies.
    * Also checking the startTime of replicated instance with the expected value.
    * In case they dont match, the test should fail.
    * @param sourceStartTime : start time of source cluster
    * @param targetStartTime : start time of target cluster
    * @param sourceDelay : delay in source cluster
    * @param targetDelay : delay in target cluster
    * @param flag : true if (sourceStartTime < targetStartTime) else false
    * */
    @Test(enabled = true, dataProvider = "Feed-Delay-Cases", timeOut = 12000000)
    public void delayTest(String sourceStartTime, String targetStartTime,
            String sourceDelay, String targetDelay, boolean flag) throws Exception {

        bundles[0].setInputFeedDataPath(feedInputPath);
        Bundle.submitCluster(bundles[0], bundles[1]);
        String feed = bundles[0].getDataSets().get(0);

        feed = FeedMerlin.fromString(feed).clearFeedClusters().toString();

        //set cluster1 as source
        feed = FeedMerlin.fromString(feed).addFeedCluster(
                new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                        .withRetention("hours(15)", ActionType.DELETE)
                        .withValidity(sourceStartTime, "2099-10-01T12:10Z")
                        .withClusterType(ClusterType.SOURCE)
                        .withDelay(new Frequency(sourceDelay))
                        .withDataLocation(feedInputPath)
                        .build()).toString();
        //set cluster2 as target
        feed = FeedMerlin.fromString(feed).addFeedCluster(
                new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                        .withRetention("hours(15)", ActionType.DELETE)
                        .withValidity(targetStartTime, "2099-10-01T12:25Z")
                        .withClusterType(ClusterType.TARGET)
                        .withDelay(new Frequency(targetDelay))
                        .withDataLocation(targetDataLocation)
                        .build()).toString();

        feed = FeedMerlin.fromString(feed).withProperty("timeout", "minutes(35)").toString();
        feed = FeedMerlin.fromString(feed).withProperty("parallel", "3").toString();

        LOGGER.info("feed : " + Util.prettyPrintXml(feed));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));

        //check if coordinator exists
        InstanceUtil.waitTillInstancesAreCreated(cluster2OC, feed, 0);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster2OC, Util.readEntityName(feed), "REPLICATION"), 1);

        //Finding bundleId of replicated instance on target
        String bundleId = OozieUtil.getLatestBundleID(cluster2OC, Util.readEntityName(feed), EntityType.FEED);

        //Finding startTime of replicated instance on target
        String startTimeO0zie = OozieUtil.getCoordStartTime(cluster2OC, feed, 0);
        String startTimeExpected = getStartTime(sourceStartTime, targetStartTime, new Frequency(sourceDelay), flag);

        List<String> missingDep = getAndCreateDependencies(cluster1FS, cluster1.getPrefix(), cluster2OC, bundleId);
        List<String> qaDep = new ArrayList<>();

        if (flag) {
            qaDep.add(sourcePath + "/" + sourceStartTime.replaceAll("-", "/").
                    replaceAll("T", "/").replaceAll(":", "/").replaceAll("Z", "/"));
        } else {
            qaDep.add(targetPath + "/" + sourceStartTime.replaceAll("-", "/").
                    replaceAll("T", "/").replaceAll(":", "/").replaceAll("Z", "/"));
        }

        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 0,
                CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);

        Assert.assertTrue(startTimeO0zie.equals(startTimeExpected),
                "Start time of bundle should be " + startTimeExpected + " but it is " + startTimeO0zie);
        matchDependencies(missingDep, qaDep);
        LOGGER.info("Done");
    }

    @DataProvider(name = "Feed-Delay-Cases")
    public Object[][] getDelayCases() {
        return new Object[][] {
            { TimeUtil.getTimeWrtSystemTime(-120), TimeUtil.getTimeWrtSystemTime(-120),
                "minutes(40)", "minutes(20)", true, },
            { TimeUtil.getTimeWrtSystemTime(-120), TimeUtil.getTimeWrtSystemTime(-120),
                "minutes(20)", "minutes(40)", true, },
            { TimeUtil.getTimeWrtSystemTime(-120), TimeUtil.getTimeWrtSystemTime(-240),
                "minutes(40)", "minutes(20)", true, },
            { TimeUtil.getTimeWrtSystemTime(-120), TimeUtil.getTimeWrtSystemTime(-60),
                "minutes(40)", "minutes(20)", false, },
        };
    }

    private List<String> getAndCreateDependencies(FileSystem sourceFS, String sourcePrefix, OozieClient targetOC,
                                                  String bundleId) throws OozieClientException, IOException {
        List<String> missingDependencies = OozieUtil.getMissingDependencies(targetOC, bundleId);
        for (int i = 0; i < 10 && missingDependencies == null; ++i) {
            TimeUtil.sleepSeconds(30);
            LOGGER.info("sleeping...");
            missingDependencies = OozieUtil.getMissingDependencies(targetOC, bundleId);
        }
        Assert.assertNotNull(missingDependencies, "Missing dependencies not found.");

        // Creating missing dependencies
        HadoopUtil.createFolders(sourceFS, sourcePrefix, missingDependencies);

        //Adding data to empty folders
        for (String location : missingDependencies) {
            LOGGER.info("Transferring data to : " + location);
            HadoopUtil.copyDataToFolder(sourceFS, location, OSUtil.RESOURCES + "dataFile.xml");
        }
        return missingDependencies;
    }

    private String getStartTime(String sourceStartTime, String targetStartTime, Frequency sourceDelay, boolean flag) {
        String finalDate;
        if (flag) {
            finalDate = TimeUtil.addMinsToTime(sourceStartTime, sourceDelay.getFrequencyAsInt());
        } else {
            finalDate = TimeUtil.addMinsToTime(targetStartTime, sourceDelay.getFrequencyAsInt());
        }
        return finalDate;
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
}

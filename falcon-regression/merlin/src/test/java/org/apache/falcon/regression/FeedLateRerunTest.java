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
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * This test submits and schedules feed and then check for replication.
 * On adding further late data it checks whether the data has been replicated correctly in the given late cut-off time.
 * Assuming that late frequency set in server is 3 minutes. Although value can be changed according to requirement.
 */
@Test(groups = { "distributed", "embedded", "sanity" })
public class FeedLateRerunTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private FileSystem cluster1FS = serverFS.get(0);
    private FileSystem cluster2FS = serverFS.get(1);
    private OozieClient cluster2OC = serverOC.get(1);
    private String baseTestDir = cleanAndGetTestDir();
    private String feedDataLocation = baseTestDir + "/source" + MINUTE_DATE_PATTERN;
    private String targetPath = baseTestDir + "/target";
    private String targetDataLocation = targetPath + MINUTE_DATE_PATTERN;
    private static final Logger LOGGER = Logger.getLogger(FeedLateRerunTest.class);
    private String source = null;
    private String target = null;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws JAXBException, IOException {
        Bundle bundle = BundleUtil.readFeedReplicationBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);
        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @Test(dataProvider = "dataFlagProvider")
    public void testLateRerun(boolean dataFlag)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException,
        OozieClientException, JAXBException {
        Bundle.submitCluster(bundles[0], bundles[1]);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 30);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);

        //configure feed
        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        feed.setFilePath(feedDataLocation);
        //erase all clusters from feed definition
        feed.clearFeedClusters();
        //set cluster1 as source
        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .withClusterType(ClusterType.SOURCE)
                .build());
        //set cluster2 as target
        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[1].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(targetDataLocation)
                .build());
        String entityName = feed.getName();

        //submit and schedule feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed.toString()));

        //check if coordinator exists
        InstanceUtil.waitTillInstancesAreCreated(cluster2OC, feed.toString(), 0);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(cluster2OC, entityName, "REPLICATION"), 1);

        //Finding bundleId of replicated instance on target
        String bundleId = OozieUtil.getLatestBundleID(cluster2OC, entityName, EntityType.FEED);

        //Finding and creating missing dependencies
        List<String> missingDependencies = getAndCreateDependencies(
            cluster1FS, cluster1.getPrefix(), cluster2OC, bundleId, dataFlag, entityName);
        int count = 1;
        for (String location : missingDependencies) {
            if (count==1) {
                source = location;
                count++;
            }
        }
        source=splitPathFromIp(source, "8020");
        LOGGER.info("source : " + source);
        target = source.replace("source", "target");
        LOGGER.info("target : " + target);
        /* Sleep for some time ( as is defined in runtime property of server ).
           Let the instance rerun and then it should succeed.*/
        int sleepMins = 8;
        for(int i=0; i < sleepMins; i++) {
            LOGGER.info("Waiting...");
            TimeUtil.sleepSeconds(60);
        }
        String bundleID = OozieUtil.getLatestBundleID(cluster2OC, entityName, EntityType.FEED);
        OozieUtil.validateRetryAttempts(cluster2OC, bundleID, EntityType.FEED, 1);

        //check if data has been replicated correctly
        List<Path> cluster1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, new Path(HadoopUtil.cutProtocol(source)));
        List<Path> cluster2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, new Path(HadoopUtil.cutProtocol(target)));
        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster2ReplicatedData);
    }

    private String splitPathFromIp(String src, String port) {
        String reqSrc, tempSrc = "";
        if (src.contains(":")) {
            String[] tempPath = src.split(":");
            for (String aTempPath : tempPath) {
                if (aTempPath.startsWith(port)) {
                    tempSrc = aTempPath;
                }
            }
        }
        if (tempSrc.isEmpty()) {
            reqSrc = src;
        } else {
            reqSrc=tempSrc.replace(port, "");
        }
        return reqSrc;
    }

    /* prismHelper1 - source colo, prismHelper2 - target colo */
    private List<String> getAndCreateDependencies(FileSystem sourceFS, String prefix, OozieClient targetOC,
            String bundleId, boolean dataFlag, String entityName) throws OozieClientException, IOException {
        List<String> missingDependencies = OozieUtil.getMissingDependencies(targetOC, bundleId);
        for (int i = 0; i < 10 && missingDependencies == null; ++i) {
            TimeUtil.sleepSeconds(30);
            LOGGER.info("sleeping...");
            missingDependencies = OozieUtil.getMissingDependencies(targetOC, bundleId);
        }
        Assert.assertNotNull(missingDependencies, "Missing dependencies not found.");
        //print missing dependencies
        for (String dependency : missingDependencies) {
            LOGGER.info("dependency from job: " + dependency);
        }
        // Creating missing dependencies
        HadoopUtil.createFolders(sourceFS, prefix, missingDependencies);
        //Adding data to empty folders depending on dataFlag
        if (dataFlag) {
            int tempCount = 1;
            for (String location : missingDependencies) {
                if (tempCount==1) {
                    LOGGER.info("Transferring data to : " + location);
                    HadoopUtil.copyDataToFolder(sourceFS, location, OSUtil.concat(OSUtil.NORMAL_INPUT, "dataFile.xml"));
                    tempCount++;
                }
            }
        }
        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(targetOC, entityName, 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);
        // Adding data for late rerun
        int tempCounter = 1;
        for (String dependency : missingDependencies) {
            if (tempCounter==1) {
                LOGGER.info("Transferring late data to : " + dependency);
                HadoopUtil.copyDataToFolder(sourceFS, dependency,
                    OSUtil.concat(OSUtil.NORMAL_INPUT, "dataFile.properties"));
            }
            tempCounter++;
        }
        return missingDependencies;
    }

    @DataProvider(name = "dataFlagProvider")
    private Object[][] dataFlagProvider() {
        return new Object[][] {
            new Object[] {true, },
            new Object[] {false, },
        };
    }
}

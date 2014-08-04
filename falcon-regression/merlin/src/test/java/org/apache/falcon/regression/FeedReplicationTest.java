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
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.List;

/**
 * feed replication test.
 */
@Test(groups = "embedded")
public class FeedReplicationTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private ColoHelper cluster3 = servers.get(2);
    private FileSystem cluster1FS = serverFS.get(0);
    private FileSystem cluster2FS = serverFS.get(1);
    private FileSystem cluster3FS = serverFS.get(2);
    private OozieClient cluster2OC = serverOC.get(1);
    private OozieClient cluster3OC = serverOC.get(2);
    private String dateTemplate = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String baseTestDir = baseHDFSDir + "/FeedReplicationTest";
    private String sourcePath = baseTestDir + "/source";
    private String feedDataLocation = baseTestDir + "/source" + dateTemplate;
    private String targetPath = baseTestDir + "/target";
    private String targetDataLocation = targetPath + dateTemplate;
    private static final Logger LOGGER = Logger.getLogger(FeedReplicationTest.class);

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws JAXBException, IOException {
        LOGGER.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readLocalDCBundle();

        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);
        bundles[2] = new Bundle(bundle, cluster3);

        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
        bundles[2].generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Test demonstrates replication of stored data from one source cluster to one target cluster.
     * It checks the lifecycle of replication workflow instance including its creation. When
     * replication ends test checks if data was replicated correctly.
     */
    @Test
    public void replicate1Source1Target()
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        OozieClientException {
        Bundle.submitCluster(bundles[0], bundles[1]);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);

        //configure feed
        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, feedDataLocation);
        //erase all clusters from feed definition
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);
        //set cluster1 as source
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)),
            ClusterType.SOURCE, null);
        //set cluster2 as target
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)),
            ClusterType.TARGET, null, targetDataLocation);

        //submit and schedule feed
        LOGGER.info("Feed : " + Util.prettyPrintXml(feed));
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,
                feed));

        //upload necessary data
        DateTime date = new DateTime(startTime, DateTimeZone.UTC);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'/'MM'/'dd'/'HH'/'mm'");
        String timePattern = fmt.print(date);
        String sourceLocation = sourcePath + "/" + timePattern + "/";
        String targetLocation = targetPath + "/" + timePattern + "/";
        HadoopUtil.recreateDir(cluster1FS, sourceLocation);

        Path toSource = new Path(sourceLocation);
        Path toTarget = new Path(targetLocation);
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation,
            OSUtil.RESOURCES + "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation, OSUtil.RESOURCES + "log_01.txt");

        //check if coordinator exists
        InstanceUtil.waitTillInstancesAreCreated(cluster2, feed, 0);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feed),
                "REPLICATION"), 1);

        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);

        //check if data has been replicated correctly
        List<Path> cluster1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, toSource);
        List<Path> cluster2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, toTarget);

        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster2ReplicatedData);
    }

    /**
     * Test demonstrates replication of stored data from one source cluster to two target clusters.
     * It checks the lifecycle of replication workflow instances including their creation on both
     * targets. When replication ends test checks if data was replicated correctly.
     */
    @Test
    public void replicate1Source2Targets() throws Exception {
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);

        //configure feed
        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, feedDataLocation);
        //erase all clusters from feed definition
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);
        //set cluster1 as source
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)),
            ClusterType.SOURCE, null);
        //set cluster2 as target
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)),
            ClusterType.TARGET, null, targetDataLocation);
        //set cluster3 as target
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[2].getClusters().get(0)),
            ClusterType.TARGET, null, targetDataLocation);

        //submit and schedule feed
        LOGGER.info("Feed : " + Util.prettyPrintXml(feed));
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,
                feed));

        //upload necessary data
        DateTime date = new DateTime(startTime, DateTimeZone.UTC);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'/'MM'/'dd'/'HH'/'mm'");
        String timePattern = fmt.print(date);
        String sourceLocation = sourcePath + "/" + timePattern + "/";
        String targetLocation = targetPath + "/" + timePattern + "/";
        HadoopUtil.recreateDir(cluster1FS, sourceLocation);

        Path toSource = new Path(sourceLocation);
        Path toTarget = new Path(targetLocation);
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation,
            OSUtil.RESOURCES + "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation, OSUtil.RESOURCES + "log_01.txt");

        //check if all coordinators exist
        InstanceUtil.waitTillInstancesAreCreated(cluster2, feed, 0);

        InstanceUtil.waitTillInstancesAreCreated(cluster3, feed, 0);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feed),
                "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readEntityName(feed),
                "REPLICATION"), 1);
        //replication on cluster 2 should start, wait till it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);

        //replication on cluster 3 should start, wait till it ends
        InstanceUtil.waitTillInstanceReachState(cluster3OC, Util.readEntityName(feed), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);

        //check if data has been replicated correctly
        List<Path> cluster1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, toSource);
        List<Path> cluster2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, toTarget);
        List<Path> cluster3ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster3FS, toTarget);

        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster2ReplicatedData);
        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster3ReplicatedData);
    }

    /**
     * Test demonstrates how replication depends on availability flag. Scenario includes one
     * source and one target cluster. When feed is submitted and scheduled and data is available,
     * feed still waits for availability flag (file which name is defined as availability flag in
     * feed definition). As soon as mentioned file is got uploaded in data directory,
     * replication starts and when it ends test checks if data was replicated correctly.
     */
    @Test
    public void availabilityFlagTest() throws Exception {
        //replicate1Source1Target scenario + set availability flag but don't upload required file
        Bundle.submitCluster(bundles[0], bundles[1]);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);

        //configure feed
        String availabilityFlagName = "README.md";
        String feedName = Util.readEntityName(bundles[0].getDataSets().get(0));
        Feed feedElement = bundles[0].getFeedElement(feedName);
        feedElement.setAvailabilityFlag(availabilityFlagName);
        bundles[0].writeFeedElement(feedElement, feedName);
        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedFilePath(feed, feedDataLocation);
        //erase all clusters from feed definition
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);
        //set cluster1 as source
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)),
            ClusterType.SOURCE, null);
        //set cluster2 as target
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRtention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)),
            ClusterType.TARGET, null, targetDataLocation);

        //submit and schedule feed
        LOGGER.info("Feed : " + Util.prettyPrintXml(feed));
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,
                feed));

        //upload necessary data
        DateTime date = new DateTime(startTime, DateTimeZone.UTC);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy'/'MM'/'dd'/'HH'/'mm'");
        String timePattern = fmt.print(date);
        String sourceLocation = sourcePath + "/" + timePattern + "/";
        String targetLocation = targetPath + "/" + timePattern + "/";
        HadoopUtil.recreateDir(cluster1FS, sourceLocation);

        Path toSource = new Path(sourceLocation);
        Path toTarget = new Path(targetLocation);
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation,
            OSUtil.RESOURCES + "feed-s4Replication.xml");
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation, OSUtil.RESOURCES + "log_01.txt");

        //check while instance is got created
        InstanceUtil.waitTillInstancesAreCreated(cluster2, feed, 0);

        //check if coordinator exists
        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), feedName, "REPLICATION"), 1);

        //replication should not start even after time
        TimeUtil.sleepSeconds(60);
        InstancesResult r = prism.getFeedHelper().getProcessInstanceStatus(feedName,
            "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 1, 0, 0, 1, 0);
        LOGGER.info("Replication didn't start.");

        //create availability flag on source
        HadoopUtil.copyDataToFolder(cluster1FS, sourceLocation, availabilityFlagName);

        //check if instance become running
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
            CoordinatorAction.Status.RUNNING, EntityType.FEED);

        //wait till instance succeed
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);

        //check if data was replicated correctly
        List<Path> cluster1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster1FS, toSource);
        LOGGER.info("Data on source cluster: " + cluster1ReplicatedData);
        List<Path> cluster2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(cluster2FS, toTarget);
        LOGGER.info("Data on target cluster: " + cluster2ReplicatedData);
        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster2ReplicatedData);
    }

}

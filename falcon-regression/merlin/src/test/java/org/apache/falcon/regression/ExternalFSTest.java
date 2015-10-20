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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Tests for operations with external file systems.
 */
@Test(groups = "embedded")
public class ExternalFSTest extends BaseTestClass{

    public static final String WASB_END_POINT =
            "wasb://" + MerlinConstants.WASB_CONTAINER + "@" + MerlinConstants.WASB_ACCOUNT;
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private FileSystem wasbFS;
    private Bundle externalBundle;

    private String baseTestDir = cleanAndGetTestDir();
    private String sourcePath = baseTestDir + "/source";
    private String baseWasbDir = "/falcon-regression/" + UUID.randomUUID().toString().split("-")[0];
    private String testWasbTargetDir = baseWasbDir + '/'
        + UUID.randomUUID().toString().split("-")[0] + '/';

    private static final Logger LOGGER = Logger.getLogger(ExternalFSTest.class);

    @BeforeClass
    public void setUpClass() throws IOException {
        HadoopUtil.recreateDir(clusterFS, baseTestDir);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", WASB_END_POINT);
        conf.set("fs.azure.account.key." + MerlinConstants.WASB_ACCOUNT,
                MerlinConstants.WASB_SECRET);
        conf.setBoolean("fs.hdfs.impl.disable.cache", false);
        wasbFS = FileSystem.get(conf);
        LOGGER.info("creating base wasb dir" + baseWasbDir);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws JAXBException, IOException {
        Bundle bundle = BundleUtil.readFeedReplicationBundle();

        bundles[0] = new Bundle(bundle, cluster);
        externalBundle = new Bundle(bundle, cluster);

        bundles[0].generateUniqueBundle(this);
        externalBundle.generateUniqueBundle(this);

        LOGGER.info("checking wasb credentials with location: " + testWasbTargetDir);
        wasbFS.create(new Path(testWasbTargetDir));
        wasbFS.delete(new Path(testWasbTargetDir), true);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        removeTestClassEntities();
        wasbFS.delete(new Path(testWasbTargetDir), true);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws IOException {
        wasbFS.delete(new Path(baseWasbDir), true);
    }


    @Test(dataProvider = "getInvalidTargets")
    public void invalidCredentialsExtFS(String endpoint) throws Exception {
        bundles[0].setClusterInterface(Interfacetype.READONLY, endpoint);
        bundles[0].setClusterInterface(Interfacetype.WRITE, endpoint);

        AssertUtil.assertFailed(prism.getClusterHelper()
            .submitEntity(bundles[0].getClusterElement().toString()));

    }

    @Test(dataProvider = "getData")
    public void replicateToExternalFS(final FileSystem externalFS,
        final String separator, final boolean withData) throws Exception {
        final String endpoint = externalFS.getUri().toString();
        Bundle.submitCluster(bundles[0], externalBundle);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        String datePattern = StringUtils .join(
            new String[]{"${YEAR}", "${MONTH}", "${DAY}", "${HOUR}", "${MINUTE}"}, separator);

        //configure feed
        FeedMerlin feed = new FeedMerlin(bundles[0].getDataSets().get(0));
        String targetDataLocation = endpoint + testWasbTargetDir + datePattern;
        feed.setFilePath(sourcePath + '/' + datePattern);
        //erase all clusters from feed definition
        feed.clearFeedClusters();
        //set local cluster as source
        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(bundles[0].getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .withClusterType(ClusterType.SOURCE)
                .build());
        //set externalFS cluster as target
        feed.addFeedCluster(
            new FeedMerlin.FeedClusterBuilder(Util.readEntityName(externalBundle.getClusters().get(0)))
                .withRetention("days(1000000)", ActionType.DELETE)
                .withValidity(startTime, endTime)
                .withClusterType(ClusterType.TARGET)
                .withDataLocation(targetDataLocation)
                .build());

        //submit and schedule feed
        LOGGER.info("Feed : " + Util.prettyPrintXml(feed.toString()));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed.toString()));
        datePattern = StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH", "mm"}, separator);
        //upload necessary data
        DateTime date = new DateTime(startTime, DateTimeZone.UTC);
        DateTimeFormatter fmt = DateTimeFormat.forPattern(datePattern);
        String timePattern = fmt.print(date);
        HadoopUtil.recreateDir(clusterFS, sourcePath + '/' + timePattern);
        if (withData) {
            HadoopUtil.copyDataToFolder(clusterFS, sourcePath + '/' + timePattern, OSUtil.SINGLE_FILE);
        }

        Path srcPath = new Path(sourcePath + '/' + timePattern);
        Path dstPath = new Path(endpoint + testWasbTargetDir + '/' + timePattern);

        //check if coordinator exists
        TimeUtil.sleepSeconds(10);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, feed.toString(), 0);
        Assert.assertEquals(OozieUtil.checkIfFeedCoordExist(clusterOC, feed.getName(), "REPLICATION"), 1);

        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(feed.toString()), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.FEED);

        //check if data has been replicated correctly
        List<Path> cluster1ReplicatedData =
                HadoopUtil.getAllFilesRecursivelyHDFS(clusterFS, srcPath);
        List<Path> cluster2ReplicatedData =
                HadoopUtil.getAllFilesRecursivelyHDFS(externalFS, dstPath);
        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster2ReplicatedData);
        final ContentSummary srcSummary = clusterFS.getContentSummary(srcPath);
        final ContentSummary dstSummary = externalFS.getContentSummary(dstPath);
        Assert.assertEquals(dstSummary.getLength(), srcSummary.getLength());
    }



    @DataProvider
    public Object[][] getData() {
        //"-" for single directory, "/" - for dir with subdirs };
        return MatrixUtil.crossProduct(new FileSystem[]{wasbFS},
            new String[]{"/", "-"},
            new Boolean[]{true, false});
    }

    @DataProvider
    public Object[][] getInvalidTargets() {
        return new Object[][]{{"wasb://invalid@invalid.blob.core.windows.net/"}};
    }

}

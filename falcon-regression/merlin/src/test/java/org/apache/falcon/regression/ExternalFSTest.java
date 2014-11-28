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
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
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
import java.lang.reflect.Method;
import java.util.List;

/**
 * Tests for operations with external file systems.
 */
@Test(groups = "embedded")
public class ExternalFSTest extends BaseTestClass{

    public static final String WASB_END_POINT =
            "wasb://" + MerlinConstants.WASB_CONTAINER + "@" + MerlinConstants.WASB_ACCOUNT;
    private ColoHelper cluster = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private FileSystem wasbFS;


    private String baseTestDir = baseHDFSDir + "/ExternalFSTest";
    private String sourcePath = baseTestDir + "/source";
    private String baseWasbDir = "/falcon-regression/" + Util.getUniqueString().substring(1);
    private String testWasbTargetDir = baseWasbDir + "/"+ Util.getUniqueString().substring(1) + "/";

    private static final Logger LOGGER = Logger.getLogger(ExternalFSTest.class);

    @BeforeClass
    public void setUpClass() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", WASB_END_POINT);
        conf.set("fs.azure.account.key." + MerlinConstants.WASB_ACCOUNT,
                MerlinConstants.WASB_SECRET);
        wasbFS = FileSystem.get(conf);
        LOGGER.info("creating base wasb dir" + baseWasbDir);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws JAXBException, IOException {
        LOGGER.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readFeedReplicaltionBundle();

        bundles[0] = new Bundle(bundle, cluster);
        bundles[1] = new Bundle(bundle, cluster2);

        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();

        LOGGER.info("checking wasb credentials with location: " + testWasbTargetDir);
        wasbFS.create(new Path(testWasbTargetDir));
        wasbFS.delete(new Path(testWasbTargetDir), true);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        removeBundles();
        wasbFS.delete(new Path(testWasbTargetDir), true);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws IOException {
        cleanTestDirs();
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
        Bundle.submitCluster(bundles[0], bundles[1]);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        String datePattern = StringUtils .join(
            new String[]{"${YEAR}", "${MONTH}", "${DAY}", "${HOUR}", "${MINUTE}"}, separator);

        //configure feed
        String feed = bundles[0].getDataSets().get(0);
        String targetDataLocation = endpoint + testWasbTargetDir + datePattern;
        feed = InstanceUtil.setFeedFilePath(feed, sourcePath + '/' + datePattern);
        //erase all clusters from feed definition
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRetention("days(1000000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);
        //set local cluster as source
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRetention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)),
            ClusterType.SOURCE, null);
        //set externalFS cluster as target
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRetention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)),
            ClusterType.TARGET, null, targetDataLocation);

        //submit and schedule feed
        LOGGER.info("Feed : " + Util.prettyPrintXml(feed));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
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
        InstanceUtil.waitTillInstancesAreCreated(cluster2, feed, 0);

        Assert.assertEquals(InstanceUtil
            .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readEntityName(feed),
                "REPLICATION"), 1);

        TimeUtil.sleepSeconds(10);
        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
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

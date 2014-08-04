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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.fs.Path;


@Test(groups = "embedded")
public class FeedRetentionTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster2FS = serverFS.get(1);
    String impressionrcWorkflowDir = baseHDFSDir + "/FeedRetentionTest/impressionrc/";
    String impressionrcWorkflowLibPath = impressionrcWorkflowDir + "lib";
    private static final Logger logger = Logger.getLogger(FeedRetentionTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        for (FileSystem fs : serverFS) {
            fs.copyFromLocalFile(new Path(
                OSUtil.getPath(OSUtil.RESOURCES, "workflows", "impression_rc_workflow.xml")),
                new Path(impressionrcWorkflowDir + "workflow.xml"));
            HadoopUtil.uploadDir(fs, impressionrcWorkflowLibPath, OSUtil.RESOURCES_OOZIE + "lib");
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        //getImpressionRC bundle
        bundles[0] = BundleUtil.readImpressionRCBundle();
        bundles[0].generateUniqueBundle();
        bundles[0] = new Bundle(bundles[0], cluster1);
        bundles[0].setProcessWorkflow(impressionrcWorkflowDir);

        bundles[1] = BundleUtil.readImpressionRCBundle();
        bundles[1].generateUniqueBundle();
        bundles[1] = new Bundle(bundles[1], cluster2);
        bundles[1].setProcessWorkflow(impressionrcWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * submit 2 clusters
     * submit and schedule feed on above 2 clusters, both having different locations
     * submit and schedule process having the above feed as output feed and running on 2
     * clusters
     */
    @Test(enabled = true)
    public void testRetentionClickRC_2Colo() throws Exception {
        String inputPath = baseHDFSDir + "/testInput/";
        String inputData = inputPath + "${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        String outputPathTemplate = baseHDFSDir +
            "/testOutput/op%d/ivoryRetention0%d/%s/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.getTimeWrtSystemTime(-5), TimeUtil.getTimeWrtSystemTime(10), 1);
        HadoopUtil.flattenAndPutDataInFolder(cluster1FS, OSUtil.RESOURCES + "thriftRRMar0602.gz",
            inputPath, dataDates);
        HadoopUtil.flattenAndPutDataInFolder(cluster2FS, OSUtil.RESOURCES + "thriftRRMar0602.gz",
            inputPath, dataDates);

        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));

        String feedOutput01 = bundles[0].getFeed("FETL-RequestRC");

        feedOutput01 = InstanceUtil.setFeedCluster(feedOutput01,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        feedOutput01 = InstanceUtil.setFeedCluster(feedOutput01,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
            "${cluster.colo}",
            String.format(outputPathTemplate, 1, 1, "data"),
            String.format(outputPathTemplate, 1, 1, "stats"),
            String.format(outputPathTemplate, 1, 1, "meta"),
            String.format(outputPathTemplate, 1, 1, "tmp"));

        feedOutput01 = InstanceUtil.setFeedCluster(feedOutput01,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "${cluster.colo}",
            String.format(outputPathTemplate, 1, 2, "data"),
            String.format(outputPathTemplate, 1, 2, "stats"),
            String.format(outputPathTemplate, 1, 2, "meta"),
            String.format(outputPathTemplate, 1, 2, "tmp"));

        //submit the new output feed
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput01));

        String feedOutput02 = bundles[0].getFeed("FETL-ImpressionRC");
        feedOutput02 = InstanceUtil.setFeedCluster(feedOutput02,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        feedOutput02 = InstanceUtil.setFeedCluster(feedOutput02,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
            "${cluster.colo}",
            String.format(outputPathTemplate, 2, 1, "data"),
            String.format(outputPathTemplate, 2, 1, "stats"),
            String.format(outputPathTemplate, 2, 1, "meta"),
            String.format(outputPathTemplate, 2, 1, "tmp"));

        feedOutput02 = InstanceUtil.setFeedCluster(feedOutput02,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "${cluster.colo}",
            String.format(outputPathTemplate, 2, 2, "data"),
            String.format(outputPathTemplate, 2, 2, "stats"),
            String.format(outputPathTemplate, 2, 2, "meta"),
            String.format(outputPathTemplate, 2, 2, "tmp"));

        //submit the new output feed
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput02));

        String feedInput = bundles[0].getFeed("FETL2-RRLog");
        feedInput = InstanceUtil
            .setFeedCluster(feedInput,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);

        feedInput = InstanceUtil.setFeedCluster(feedInput,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
            Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
            "${cluster.colo}", inputData);

        feedInput = InstanceUtil.setFeedCluster(feedInput,
            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
            Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "${cluster.colo}", inputData);

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedInput));

        String process = bundles[0].getProcessData();
        process = InstanceUtil.setProcessCluster(process, null,
            XmlUtil.createProcessValidity("2012-10-01T12:00Z", "2012-10-01T12:10Z"));

        process = InstanceUtil.setProcessCluster(process,
            Util.readEntityName(bundles[0].getClusters().get(0)),
            XmlUtil.createProcessValidity(TimeUtil.getTimeWrtSystemTime(-2),
                TimeUtil.getTimeWrtSystemTime(5)));
        process = InstanceUtil.setProcessCluster(process,
            Util.readEntityName(bundles[1].getClusters().get(0)),
            XmlUtil.createProcessValidity(TimeUtil.getTimeWrtSystemTime(-2),
                TimeUtil.getTimeWrtSystemTime(5)));

        logger.info("process: " + Util.prettyPrintXml(process));

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process));

        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput01));
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput02));
    }

}

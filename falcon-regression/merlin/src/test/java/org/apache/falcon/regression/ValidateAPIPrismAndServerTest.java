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

import org.apache.commons.httpclient.HttpStatus;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Validate API is exposed both via server and prism.
 */
@Test(groups = {"singleCluster"})
public class ValidateAPIPrismAndServerTest extends BaseTestClass {
    private ColoHelper cluster = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0].generateUniqueBundle(this);
        bundles[0] = new Bundle(bundles[0], cluster);
        feed = bundles[0].getInputFeedFromBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     *Validate a valid cluster via prism.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateValidClusterOnPrism() throws Exception {
        ServiceResponse response = prism.getClusterHelper().validateEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);
    }

    /**
     *Validate a valid cluster via server.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateValidClusterOnServer() throws Exception {
        ServiceResponse response = cluster.getClusterHelper().validateEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);
    }

    /**
     *Validate an invalid cluster via prism.
     * Should fail.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateInvalidClusterOnPrism() throws Exception {
        ClusterMerlin clusterObj = new ClusterMerlin(bundles[0].getClusters().get(0));
        clusterObj.setColo(null);
        ServiceResponse response = prism.getClusterHelper().validateEntity(clusterObj.toString());
        AssertUtil.assertFailed(response);
    }

    /**
     *Validate an invalid cluster via server.
     * Should fail.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateInvalidClusterOnServer() throws Exception {
        ClusterMerlin clusterObj = new ClusterMerlin(bundles[0].getClusters().get(0));
        clusterObj.setColo(null);
        ServiceResponse response = cluster.getClusterHelper().validateEntity(clusterObj.toString());
        AssertUtil.assertFailed(response);
    }

    /**
     *Validate a valid feed via prism.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateValidFeed() throws Exception {
        ServiceResponse response =
                prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);
        response = prism.getFeedHelper().validateEntity(feed);
        AssertUtil.assertSucceeded(response);
    }

    /**
     *Validate a valid feed via server.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateValidFeedOnPrism() throws Exception {
        ServiceResponse response =
                prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);
        response = cluster.getFeedHelper().validateEntity(feed);
        AssertUtil.assertSucceeded(response);
    }

    /**
     *Validate an invalid feed via prism.
     * Invalidating feed by setting location type data empty. Should fail..
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateInvalidFeedOnPrism() throws Exception {
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        FeedMerlin feedObj = new FeedMerlin(feed);
        feedObj.setLocation(LocationType.DATA, "");
        ServiceResponse response = prism.getFeedHelper().validateEntity(feedObj.toString());
        AssertUtil.assertFailed(response);
    }

    /**
     *Validate an invalid feed via server.
     * Invalidating feed by setting location type data empty. Should fail..
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateInvalidFeedOnServer() throws Exception {
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        FeedMerlin feedObj = new FeedMerlin(feed);
        feedObj.setLocation(LocationType.DATA, "");
        ServiceResponse response = cluster.getFeedHelper().validateEntity(feedObj.toString());
        AssertUtil.assertFailed(response);
    }

    /**
     *Validate a valid process but without workflow present via prism.
     * Should fail.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateValidProcessNoWorkflowOnPrism() throws Exception {
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(feed);
        ServiceResponse response = prism.getProcessHelper().validateEntity(bundles[0].getProcessData());
        AssertUtil.assertFailedWithStatus(response, HttpStatus.SC_BAD_REQUEST, "Workflow does not exist");
    }

    /**
     *Validate a valid process but without workflow present via server.
     * Should fail.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateValidProcessNoWorkflowOnServer() throws Exception {
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(feed);
        ServiceResponse response = cluster.getProcessHelper().validateEntity(bundles[0].getProcessData());
        AssertUtil.assertFailedWithStatus(response, HttpStatus.SC_BAD_REQUEST, "Workflow does not exist");
    }

    /**Validate a valid process with workflow present via prism.
     * Should pass.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateValidProcessWithWorkflowOnPrism() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(feed);
        prism.getFeedHelper().submitEntity(bundles[0].getOutputFeedFromBundle());
        ServiceResponse response = prism.getProcessHelper().validateEntity(bundles[0].getProcessData());
        AssertUtil.assertSucceeded(response);
    }

    /**Validate a valid process with workflow present via server.
     * Should pass.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateValidProcessWithWorkflowOnServer() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(feed);
        prism.getFeedHelper().submitEntity(bundles[0].getOutputFeedFromBundle());
        ServiceResponse response = cluster.getProcessHelper().validateEntity(bundles[0].getProcessData());
        AssertUtil.assertSucceeded(response);
    }

    /**Validate an invalid process (set workflow null) via prism.
     * Should fail.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void validateInvalidProcessOnPrism() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(feed);
        prism.getFeedHelper().submitEntity(bundles[0].getOutputFeedFromBundle());
        ProcessMerlin processObj = bundles[0].getProcessObject();
        processObj.setWorkflow(null);
        ServiceResponse response = prism.getProcessHelper().validateEntity(processObj.toString());
        AssertUtil.assertFailed(response);
    }

    /**Validate an invalid process (set workflow null) via server.
     * Should fail.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void validateInvalidProcessOnServer() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(feed);
        prism.getFeedHelper().submitEntity(bundles[0].getOutputFeedFromBundle());
        ProcessMerlin processObj = bundles[0].getProcessObject();
        processObj.setWorkflow(null);
        ServiceResponse response = cluster.getProcessHelper().validateEntity(processObj.toString());
        AssertUtil.assertFailed(response);
    }

}

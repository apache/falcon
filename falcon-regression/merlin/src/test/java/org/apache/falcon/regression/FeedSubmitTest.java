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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Feed submission tests.
 */
@Test(groups = "embedded")
public class FeedSubmitTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0].generateUniqueBundle(this);
        bundles[0] = new Bundle(bundles[0], cluster);

        //submit the cluster
        ServiceResponse response =
            prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        AssertUtil.assertSucceeded(response);
        feed = bundles[0].getInputFeedFromBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Submit correctly adjusted feed. Response should reflect success.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void submitValidFeed() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Submit and remove feed. Try to submit it again. Response should reflect success.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void submitValidFeedPostDeletion() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().delete(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Submit feed. Get its definition. Try to submit it again. Should succeed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void submitValidFeedPostGet() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().getEntityDefinition(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Try to submit correctly adjusted feed twice. Should succeed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void submitValidFeedTwice() throws Exception {
        ServiceResponse response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);

        response = prism.getFeedHelper().submitEntity(feed);
        AssertUtil.assertSucceeded(response);
    }
}

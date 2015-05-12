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

package org.apache.falcon.regression.searchUI;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.FeedWizardPage;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** UI tests for Feed Setup Wizard. */
@Test(groups = "search-ui")
public class FeedSetupTest extends BaseUITestClass{
    private static final Logger LOGGER = Logger.getLogger(FeedSetupTest.class);
    private FeedWizardPage feedWizardPage = null;
    private SearchPage searchPage = null;

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private FeedMerlin feed;
    private String[] tags = {"first=yes", "second=yes", "third=yes", "wrong=no"};

    private String getRandomTags() {
        List<String> tagsList = new ArrayList<>();
        Random r = new Random();
        if (r.nextInt(4) == 0) {
            tagsList.add(tags[0]);
        }
        if (r.nextInt(3) == 0) {
            tagsList.add(tags[1]);
        }
        if (r.nextInt(2) == 0) {
            tagsList.add(tags[2]);
        }
        return StringUtils.join(tagsList, ',');
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception{
        openBrowser();
        searchPage = LoginPage.open(getDriver()).doDefaultLogin();
        feedWizardPage = searchPage.getPageHeader().doCreateFeed();
        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundle = new Bundle(bundle, cluster);
        bundle.setInputFeedDataPath(feedInputPath);
        bundle.submitClusters(prism);
        feed = FeedMerlin.fromString(bundle.getInputFeedFromBundle());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

    /*
    Check that buttons (logout, entities, uploadXml, help, Falcon) are present, and
    names are correct. Check the user name on header. "Create an entity"/
    "upload an entity" headers. Check that each button navigates user to correct page
     */
    @Test
    public void testHeader() throws Exception {
        feedWizardPage.getPageHeader().checkHeader();
    }

    /*
    Run full feed creation scenario.
     */
    @Test
    public void testWizardDefaultScenario() throws Exception {
        // Set few values in feed, if they are null (Currently null in the FeedMerlin)
        if (feed.getTags() == null){
            feed.setTags(getRandomTags());
        }
        if (feed.getGroups() == null){
            feed.setGroups("groups");
        }
        if (feed.getAvailabilityFlag() == null){
            feed.setAvailabilityFlag("_SUCCESS");
        }
        feedWizardPage.setFeed(feed);
        //Check the response to validate if the feed creation went successfully
        ServiceResponse response = prism.getFeedHelper().getEntityDefinition(feed.toString());
        AssertUtil.assertSucceeded(response);
    }

}

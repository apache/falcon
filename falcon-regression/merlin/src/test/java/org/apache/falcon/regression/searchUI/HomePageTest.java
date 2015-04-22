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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

/** UI tests for Search UI Homepage. */
@Test(groups = "search-ui")
public class HomePageTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(HomePageTest.class);
    private SearchPage homePage = null;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        openBrowser();
        homePage = LoginPage.open(getDriver()).doDefaultLogin();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

    @Test
    public void testHeader() throws Exception {
        homePage.getPageHeader().checkHeader();
    }

    @Test
    public void testNavigationToEntitiesTable() throws Exception {
        homePage.checkNoResult();
        bundles[0] = BundleUtil.readELBundle();
        ColoHelper cluster = servers.get(0);
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        String pigTestDir = cleanAndGetTestDir();
        String inputPath = pigTestDir + "/input" + MINUTE_DATE_PATTERN;
        bundles[0].setInputFeedDataPath(inputPath);
        bundles[0].submitAndScheduleFeed();
        final List<SearchPage.SearchResult> searchResults = homePage.doSearch("*");
        Assert.assertEquals(searchResults.size(), 1, "Expecting one search results");
    }

    @Test
    public void testSearchBoxInitialConditions() throws Exception {
        homePage.checkNoResult();
        AssertUtil.assertEmpty(homePage.getSearchQuery().getName(), "Expecting blank search box");
        AssertUtil.assertEmpty(homePage.getSearchQuery().getTags(), "Expecting blank search box");
        final List<SearchPage.SearchResult> searchResults = homePage.getSearchResults();
        Assert.assertEquals(searchResults.size(), 0, "Expecting no search results");
    }

}

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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

/** UI tests for entities table with search results. */
@Test(groups = "search-ui")
public class EntitiesTableTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(EntitiesTableTest.class);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    private SearchPage searchPage = null;
    private String[] tags = {"first=correctvalue", "second=one", "third=one", "fourth=one",
        "fifth=one", "sixth=one", "seventh=one", "eighth=one", "ninth=one", "tenth=one", };
    private String baseProcessName;

    /**
     * Submit one cluster, 2 feeds and 10 processes with 1 to 10 tags (1st process has 1 tag,
     * 2nd - two tags.. 10th has 10 tags).
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     * @throws InterruptedException
     * @throws JAXBException
     */
    @BeforeClass(alwaysRun = true)
    public void setup()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException,
        JAXBException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        openBrowser();
        searchPage = LoginPage.open(getDriver()).doDefaultLogin();
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        ProcessMerlin process = bundles[0].getProcessObject();
        baseProcessName = process.getName();
        for (int i = 1; i <= tags.length; i++) {
            process.setName(baseProcessName + '-' + i);
            process.setTags(StringUtils.join(Arrays.copyOfRange(tags, 0, i), ','));
            AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(process.toString()));
        }

    }

    @BeforeMethod(alwaysRun = true)
    public void refresh() {
        searchPage.refresh();
    }


    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

    /**
     * Search entities with invalid params. Zero entities should be shown.
     */
    @Test(dataProvider = "getInvalidFilters")
    public void testSearchBoxInvalidFilter(String invalidSearch) {
        Assert.assertEquals(searchPage.doSearch(invalidSearch).size(), 0,
            "There should be 0 results with query='" + invalidSearch + "'");
    }


    @DataProvider
    public Object[][] getInvalidFilters() {
        return new String[][]{
            {"notnameofentity"},
            {"* othertag=sometag"},
            {"* first=other"},
            {"XX" + bundles[0].getProcessName().substring(2)},
        };
    }

    /**
     * All processes should be found with first tag. Add tags one by one. Only one process
     * should be found with all tags.
     */
    @Test
    public void testSearchBoxManyParams() {
        searchPage.doSearch(baseProcessName);
        for (int i = 0; i < tags.length; i++) {
            Assert.assertEquals(searchPage.appendAndSearch(tags[i]).size(), tags.length - i,
                "There should be " + (tags.length - i) + " results");
        }
    }

    /**
     * Only one process should be found with all tags. Delete tags one by one. All processes
     * should be found with first tag. Zero entities should be shown after cleaning all params.
     */
    @Test(dataProvider = "getBoolean")
    public void testSearchBoxCleanSingleParam(boolean deleteByClick) {
        searchPage.doSearch(this.getClass().getSimpleName() + ' ' + StringUtils.join(tags, ' '));
        for (int i = 1; i <= tags.length; i++) {
            Assert.assertEquals(searchPage.getSearchResults().size(), i,
                "There should be " + i + " results");
            if (deleteByClick) {
                searchPage.getSearchQuery().deleteLast();
            } else {
                searchPage.removeLastParam();
            }
        }
        if (deleteByClick) {
            searchPage.getSearchQuery().deleteLast();
        } else {
            searchPage.removeLastParam();
        }
        Assert.assertEquals(searchPage.getSearchResults().size(), 0,
            "There should be 0 results");
    }

    @DataProvider
    public Object[][] getBoolean() {
        return new Boolean[][]{{Boolean.TRUE}, {Boolean.FALSE}};
    }
}

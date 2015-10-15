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
import org.apache.falcon.regression.core.util.FileUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.ClusterWizardPage;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/** UI tests for Search UI Homepage. */
@Test(groups = "search-ui")
public class HomePageTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(HomePageTest.class);
    private SearchPage homePage = null;
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;

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
        ColoHelper coloHelper = servers.get(0);
        bundles[0] = new Bundle(bundles[0], coloHelper);
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

    /**
     * Upload cluster, feed, process entity through upload entity button.
     * The entity should get created.
     * @throws Exception
     */
    @Test
    public void testUploadEntity() throws Exception{

        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundle = new Bundle(bundle, cluster);
        bundle.setInputFeedDataPath(feedInputPath);
        bundle.setProcessWorkflow(aggregateWorkflowDir);


        final String clusterXml = bundle.getClusterElement().toString();
        homePage.getPageHeader().uploadXml(FileUtil.writeEntityToFile(clusterXml));
        String alert = homePage.getActiveAlertText();
        Assert.assertTrue(alert.contains("Submit successful"), "Not expected alert: '" + alert + "'");
        AssertUtil.assertSucceeded(prism.getClusterHelper().getEntityDefinition(clusterXml));

        final String feedXml = bundle.getInputFeedFromBundle();
        homePage.getPageHeader().uploadXml(FileUtil.writeEntityToFile(feedXml));
        alert = homePage.getActiveAlertText();
        Assert.assertTrue(alert.contains("Submit successful"), "Not expected alert: '" + alert + "'");
        AssertUtil.assertSucceeded(prism.getFeedHelper().getEntityDefinition(feedXml));

        final String outputFeedXml = bundle.getOutputFeedFromBundle();
        homePage.getPageHeader().uploadXml(FileUtil.writeEntityToFile(outputFeedXml));
        alert = homePage.getActiveAlertText();
        Assert.assertTrue(alert.contains("Submit successful"), "Not expected alert: '" + alert + "'");
        AssertUtil.assertSucceeded(prism.getFeedHelper().getEntityDefinition(outputFeedXml));

        final String processXml = bundle.getProcessObject().toString();
        homePage.getPageHeader().uploadXml(FileUtil.writeEntityToFile(processXml));
        alert = homePage.getActiveAlertText();
        Assert.assertTrue(alert.contains("Submit successful"), "Not expected alert: '" + alert + "'");
        AssertUtil.assertSucceeded(prism.getProcessHelper().getEntityDefinition(processXml));

    }

    /**
     * Upload bad cluster, feed, process entity through upload entity button.
     * We should get an error.
     * @throws Exception
     */
    @Test
    public void testUploadBadEntity() throws Exception{

        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundle = new Bundle(bundle, cluster);
        bundle.setInputFeedDataPath(feedInputPath);
        bundle.setProcessWorkflow(aggregateWorkflowDir);

        // Create a text file, with some text
        File textFile = new File("invalid.txt");
        PrintWriter writer = new PrintWriter(textFile, "UTF-8");
        String text = "Some random text";
        writer.println(text);
        writer.close();
        homePage.getPageHeader().uploadXml(textFile.getAbsolutePath());
        String alertText = homePage.getActiveAlertText();
        Assert.assertEquals(alertText, "Invalid xml. File not uploaded",
            "Text file was allowed to be uploaded");

        // Create a xml file with proper name, but invalid text contents
        File xmlFile = new File("cluster.xml");
        writer = new PrintWriter(xmlFile, "UTF-8");
        text = "The first line\nThe second line";
        writer.println(text);
        writer.close();

        homePage.getPageHeader().uploadXml(xmlFile.getAbsolutePath());
        alertText = homePage.getActiveAlertText();
        Assert.assertEquals(alertText, "Invalid xml. File not uploaded",
            "XML file with invalid text was allowed to be uploaded");
        //check the same with notification bar
        homePage.getPageHeader().validateNotificationCountAndCheckLast(2, "Invalid xml. File not uploaded");
    }

    /**
     * Submit cluster with name e.g. myCluster.
     * Select "create cluster". Try to populate name field with the name of previously submitted cluster.
     * Check that "name unavailable" has appeared.
     * @throws Exception
     */
    @Test
    public void clusterSetupNameAvailability() throws Exception{

        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundle = new Bundle(bundle, cluster);

        // Submit Cluster
        final String clusterXml = bundle.getClusterElement().toString();
        homePage.getPageHeader().uploadXml(FileUtil.writeEntityToFile(clusterXml));
        AssertUtil.assertSucceeded(prism.getClusterHelper().getEntityDefinition(clusterXml));

        // Get cluster name and try to set it again
        String clusterName = bundle.getClusterElement().getName();
        ClusterWizardPage clusterWizardPage = homePage.getPageHeader().doCreateCluster();
        clusterWizardPage.setName(clusterName);

        // Assert that name unavailable is displayed
        clusterWizardPage.checkNameUnavailableDisplayed(true);
    }



}

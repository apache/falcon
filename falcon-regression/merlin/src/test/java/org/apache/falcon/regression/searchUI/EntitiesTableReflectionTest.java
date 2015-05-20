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

import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.Page.EntityStatus;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.falcon.regression.ui.search.SearchPage.Button;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** UI tests for entities table with search results. */
@Test(groups = "search-ui")
public class EntitiesTableReflectionTest extends BaseUITestClass {
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    private SearchPage searchPage = null;
    private String twoProcessesNameStart;
    private Map<String, String> processesMap = new TreeMap<>();

    @BeforeClass(alwaysRun = true)
    public void setup() throws IOException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        openBrowser();
        searchPage = LoginPage.open(getDriver()).doDefaultLogin();
    }

    @BeforeMethod(alwaysRun = true)
    public void submitEntities()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException,
        JAXBException {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);

        ProcessMerlin process = bundles[0].getProcessObject();
        twoProcessesNameStart = process.getName() + '-';
        process.setName(twoProcessesNameStart + 1);
        bundles[0].setProcessData(process.toString());
        prism.getProcessHelper().submitEntity(process.toString());
        processesMap.put(process.getName(), process.toString());

        process.setName(twoProcessesNameStart + 2);
        prism.getProcessHelper().submitEntity(process.toString());
        processesMap.put(process.getName(), process.toString());
    }

    @Test
    public void testUIStatusChangedViaAPI() throws URISyntaxException,
        AuthenticationException, InterruptedException, IOException, JAXBException {
        String processName = bundles[0].getProcessName();
        Assert.assertEquals(searchPage.doSearch(processName).size(), 1,
            "One result should be present");
        Assert.assertEquals(searchPage.doSearch(processName).get(0).getStatus(),
            EntityStatus.SUBMITTED, "Status of process should be SUBMITTED");

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().schedule(bundles[0].getProcessData()));
        Assert.assertEquals(searchPage.doSearch(processName).get(0).getStatus(),
            EntityStatus.RUNNING, "Status of process should be RUNNING");

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        Assert.assertEquals(searchPage.doSearch(processName).get(0).getStatus(),
            EntityStatus.SUSPENDED, "Status of process should be SUSPENDED");

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().resume(bundles[0].getProcessData()));
        Assert.assertEquals(searchPage.doSearch(processName).get(0).getStatus(),
            EntityStatus.RUNNING, "Status of process should be RUNNING");

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().delete(bundles[0].getProcessData()));
        Assert.assertEquals(searchPage.doSearch(processName).size(), 0,
            "Zero results should be present after deletion");

        AssertUtil.assertSucceeded(
            prism.getProcessHelper().submitAndSchedule(bundles[0].getProcessData()));
        Assert.assertEquals(searchPage.doSearch(processName).get(0).getStatus(),
            EntityStatus.RUNNING, "Status of rescheduled process should be RUNNING");
    }

    @Test
    public void testActionButtonsNotScheduled() {
        Assert.assertEquals(searchPage.doSearch(twoProcessesNameStart).size(), 2,
            "Two results should be present");

        Assert.assertEquals(searchPage.getButtons(true), EnumSet.noneOf(Button.class),
            "There should be zero active buttons");
        Assert.assertEquals(searchPage.getButtons(false), EnumSet.allOf(Button.class),
            "All buttons should be disabled");

        searchPage.selectRow(1);
        Assert.assertEquals(searchPage.getButtons(false), EnumSet.of(Button.Resume, Button.Suspend),
            "List of disabled buttons is not correct");
        searchPage.selectRow(2);
        Assert.assertEquals(searchPage.getButtons(true), EnumSet.of(Button.Schedule, Button.Delete),
            "List of active buttons is not correct");
        searchPage.deselectRow(2);
        Assert.assertEquals(searchPage.getButtons(false), EnumSet.of(Button.Resume, Button.Suspend),
            "List of disabled buttons is not correct");
    }

    @Test
    public void testActionsPauseResume() throws URISyntaxException,
        AuthenticationException, InterruptedException, IOException {
        Assert.assertEquals(searchPage.doSearch(twoProcessesNameStart).size(), 2,
            "Two results should be present after deletion");

        //select first process
        searchPage.selectRow(1);
        searchPage.clickButton(Button.Schedule);
        List<SearchPage.SearchResult> results = searchPage.getSearchResults();
        String firstProcess = processesMap.get(results.get(0).getEntityName());
        String secondProcess = processesMap.get(results.get(1).getEntityName());

        Assert.assertEquals(results.get(0).getStatus(), EntityStatus.RUNNING,
            "Unexpected status after 'Schedule' was clicked");
        Assert.assertTrue(prism.getProcessHelper().getStatus(firstProcess).getMessage()
            .contains("RUNNING"), "First process should be RUNNING via API");
        //select two processes
        searchPage.clickSelectAll();
        Assert.assertEquals(searchPage.getButtons(true), EnumSet.of(Button.Delete),
            "Only 'Delete' should be available for processes with different  statuses");

        searchPage.deselectRow(1);
        searchPage.clickButton(Button.Schedule);

        Assert.assertEquals(searchPage.getSearchResults().get(1).getStatus(), EntityStatus.RUNNING,
            "Unexpected status after 'Schedule' was clicked");
        Assert.assertTrue(prism.getProcessHelper().getStatus(secondProcess).getMessage()
            .contains("RUNNING"), "Second process should be RUNNING via API");

        searchPage.selectRow(1);
        searchPage.selectRow(2);
        searchPage.clickButton(Button.Suspend);

        Assert.assertEquals(searchPage.getSearchResults().get(0).getStatus(), EntityStatus.SUSPENDED,
            "Unexpected status after 'Suspend' was clicked");
        Assert.assertTrue(prism.getProcessHelper().getStatus(firstProcess).getMessage()
            .contains("SUSPENDED"), "First process should be SUSPENDED via API");

        Assert.assertEquals(searchPage.getSearchResults().get(1).getStatus(), EntityStatus.SUSPENDED,
            "Unexpected status after 'Suspend' was clicked");
        Assert.assertTrue(prism.getProcessHelper().getStatus(secondProcess).getMessage()
            .contains("SUSPENDED"), "Second process should be SUSPENDED via API");

        searchPage.clickSelectAll();
        Assert.assertEquals(searchPage.getButtons(true), EnumSet.of(Button.Resume, Button.Delete),
            "List of active buttons is not correct after selecting two SUSPENDED processes");
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }


    @AfterClass(alwaysRun = true)
    public void tearDownClass() {
        closeBrowser();
    }
}

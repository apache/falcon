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

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.EntityPage;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for Search UI Entity Page.
 */
@Test(groups = "search-ui")
public class EntityPageTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(EntityPageTest.class);
    private final ColoHelper cluster = servers.get(0);
    private final FileSystem clusterFS = serverFS.get(0);
    private final OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output" + MINUTE_DATE_PATTERN;
    private SearchPage searchPage = null;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws IOException {
        cleanAndGetTestDir();
        HadoopUtil.uploadDir(serverFS.get(0), aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessInputStartEnd("now(0, 0)", "now(0, 0)");
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);

        openBrowser();
        final LoginPage loginPage = LoginPage.open(getDriver());
        searchPage = loginPage.doDefaultLogin();
    }

    /**
     * Basic checks on the entity page for feed and process.
     * Run feed and process.
     * Click on each entity from Entities table. Navigate to respective entity page.
     * Check that page is named the same as entity.
     * Check that page has all required blocks and identifying elements (Dependencies,
     * Properties, Instances blocks)
     */
    @Test
    public void entityPage() throws Exception {
        final FeedMerlin inputFeed = FeedMerlin.fromString(bundles[0].getInputFeedFromBundle());
        final FeedMerlin outputFeed = FeedMerlin.fromString(bundles[0].getOutputFeedFromBundle());
        final ProcessMerlin processMerlin = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleProcess();

        final List<SearchPage.SearchResult> results = searchPage.doSearch("*");
        SearchPage.SearchResult.assertEqual(results,
            Arrays.asList(inputFeed, outputFeed, processMerlin), "Unexpected search results.");
        for (SearchPage.SearchResult result : results) {
            EntityPage entityPage = searchPage.click(result);
            Assert.assertEquals(entityPage.getEntityName(), result.getEntityName(),
                "Unexpected entity name displayed on entity page.");
            entityPage.checkPage();
            //navigate to initial search page
            searchPage = entityPage.getPageHeader().gotoHome();
            searchPage.doSearch("*");
        }
    }

    /**
     * Test header of the EntityPage.
     * Check that buttons (logout, entities, uploadXml, help, Falcon) are present, and names are
     * correct.
     * Check the user name on header.
     * "Create an entity"/"upload an entity" headers.
     * Check that each button navigates user to correct page.
     * @throws Exception
     */
    @Test
    public void testHeader() throws Exception {
        bundles[0].submitAndScheduleProcess();
        final SearchPage.SearchResult firstResult = searchPage.doSearch("*").get(0);
        final EntityPage entityPage = searchPage.click(firstResult);
        entityPage.getPageHeader().checkHeader();
    }

    /**
     * Test that details of the feed are displayed correctly.
     * Run feed. Check that properties block shows correct name,
     * tags, workflow etc.
     * @throws IOException
     */
    @Test
    public void testFeedEntityProperties() throws Exception {
        final FeedMerlin inputFeed = FeedMerlin.fromString(bundles[0].getInputFeedFromBundle());
        addLocationsToFeedCluster(inputFeed);
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(inputFeed.toString()));

        final List<SearchPage.SearchResult> results = searchPage.doSearch("*");
        SearchPage.SearchResult.assertEqual(results, Collections.singletonList((Entity) inputFeed),
            "Unexpected search results.");
        final SearchPage.SearchResult result = results.get(0);
        EntityPage entityPage = searchPage.click(result);
        final String entityName = result.getEntityName();
        Assert.assertEquals(entityPage.getEntityName(), entityName,
            "Unexpected entity name displayed on entity page.");
        entityPage.checkPage();
        entityPage.checkFeedProperties(inputFeed);
    }

    private void addLocationsToFeedCluster(FeedMerlin feed) {
        final Locations feedClusterLocations = new Locations();
        final Location firstFeedClusterLocation = new Location();
        firstFeedClusterLocation.setPath(feedInputPath);
        firstFeedClusterLocation.setType(LocationType.DATA);
        feedClusterLocations.getLocations().add(firstFeedClusterLocation);
        final Location secondFeedClusterLocation = new Location();
        secondFeedClusterLocation.setPath(baseTestHDFSDir + "/stats");
        secondFeedClusterLocation.setType(LocationType.STATS);
        feedClusterLocations.getLocations().add(secondFeedClusterLocation);
        feed.getClusters().getClusters().get(0).setLocations(feedClusterLocations);
    }

    /**
     * Test that details of the process are displayed correctly.
     * Run process. Check that properties block shows correct name,
     * input, output etc.
     * @throws IOException
     */
    @Test
    public void testProcessEntityProperties() throws Exception {
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        final List<SearchPage.SearchResult> results = searchPage.doSearch("*");
        final List<Entity> scheduledEntities = new ArrayList<>();
        scheduledEntities.add(process);
        scheduledEntities.addAll(FeedMerlin.fromString(bundles[0].getDataSets()));
        SearchPage.SearchResult.assertEqual(results, scheduledEntities,
            "Unexpected search results.");
        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        Assert.assertEquals(entityPage.getEntityName(), process.getName(),
            "Unexpected entity name displayed on entity page.");
        entityPage.checkPage();
        entityPage.checkProcessProperties(process);
    }

    /**
     * Run a process with 9 instances. Get into it's page.
     * Check that block contains correct number of instances with appropriate names, time ranges,
     * statuses and colors.
     */
    @Test
    public void testInstancesBlockInfo() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:41Z");
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));
        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);
        OozieUtil.createMissingDependencies(
            cluster, EntityType.PROCESS, process.getName(), 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 5);
        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        final List<SearchPage.SearchResult> results = searchPage.doSearch("*");
        final List<Entity> scheduledEntities = new ArrayList<>();
        scheduledEntities.add(process);
        scheduledEntities.addAll(FeedMerlin.fromString(bundles[0].getDataSets()));
        SearchPage.SearchResult.assertEqual(results, scheduledEntities,
            "Unexpected search results.");
        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        Assert.assertEquals(entityPage.getEntityName(), process.getName(),
            "Unexpected entity name displayed on entity page.");
        entityPage.checkPage();
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();
        displayedSummary.check();
        final InstancesResult apiSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=2010-01-02T01:00Z", null);
        displayedSummary.checkSummary(apiSummary.getInstances());
    }

    /**
     * Run a process with 30 instances. Get into it's page.
     * Check that instance block has pagination enabled. Check the number of instances on current
     * page, go to the next one and check that number there too.
     */
    @Test
    public void testInstancesBlockManyInstances() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T03:26Z");
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));
        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);

        final List<SearchPage.SearchResult> results = searchPage.doSearch("*");
        final List<Entity> scheduledEntities = new ArrayList<>();
        scheduledEntities.add(process);
        scheduledEntities.addAll(FeedMerlin.fromString(bundles[0].getDataSets()));
        SearchPage.SearchResult.assertEqual(results, scheduledEntities,
            "Unexpected search results.");
        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        Assert.assertEquals(entityPage.getEntityName(), process.getName(),
            "Unexpected entity name displayed on entity page.");
        entityPage.checkPage();
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();
        final InstancesResult apiSummary = prism.getProcessHelper().listInstances(
            process.getName(),
            "start=2010-01-02T01:00Z&end=2099-01-01T01:00Z&numResults=1000", null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiSummary.getInstances());
    }

    /**
     * Run a process with 3 instances: 1 succeeded, 1 suspended and 1 waiting.
     * Get into it's page.
     * Set filtering by start time. Increase start time and check that number of instances
     * decreases.
     * Set end time filter - only succeeded instance should be shown.
     * Set filtering by status:
     *   running (empty list)
     *   waiting - 1 instance
     *   succeeded - 1 instance
     *   suspended - 1 instance
     * Check that block contains correct number of instances with appropriate names, time ranges,
     * statuses and colors.
     */
    @Test
    public void testInstancesBlockFilter() throws Exception {
        final String startTime = "2010-01-02T01:00Z";
        final String endTime = "2010-01-02T01:11Z";
        String prefix = bundles[0].getFeedDataPathPrefix();
        bundles[0].setProcessValidity(startTime, endTime);
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);
        final List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTime,
            TimeUtil.addMinsToTime(endTime, -5), 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 5);
        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);
        //suspend the second instance
        prism.getProcessHelper().getProcessInstanceSuspend(process.getName(),
            "?start=" + TimeUtil.addMinsToTime(startTime, 5)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 6));

        final List<SearchPage.SearchResult> results = searchPage.doSearch("*");
        final List<Entity> scheduledEntities = new ArrayList<>();
        scheduledEntities.add(process);
        scheduledEntities.addAll(FeedMerlin.fromString(bundles[0].getDataSets()));
        SearchPage.SearchResult.assertEqual(results, scheduledEntities,
            "Unexpected search results.");
        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        Assert.assertEquals(entityPage.getEntityName(), process.getName(),
            "Unexpected entity name displayed on entity page.");
        entityPage.checkPage();
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();
        final InstancesResult apiSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiSummary.getInstances());
        displayedSummary.setInstanceSummaryStartTime("01022010" + "0100");
        final List<EntityPage.OneInstanceSummary> newSummary =
            entityPage.getInstanceSummary().getSummary();
        Assert.assertNull(EntityPage.InstanceSummary.getOneSummary(newSummary, startTime),
            "Not expecting first instance to be displayed");
        displayedSummary.setInstanceSummaryStartTime("01022010" + "0105");
        final List<EntityPage.OneInstanceSummary> newSummary2 =
            entityPage.getInstanceSummary().getSummary();
        Assert.assertNull(EntityPage.InstanceSummary.getOneSummary(newSummary2, startTime),
            "Not expecting first instance to be displayed");
        Assert.assertNull(EntityPage.InstanceSummary.getOneSummary(newSummary2,
                TimeUtil.addMinsToTime(startTime, 5)),
            "Not expecting second instance to be displayed");

        displayedSummary.setInstanceSummaryStartTime("");
        displayedSummary.setInstanceSummaryEndTime("01022010" + "0105");
        final List<EntityPage.OneInstanceSummary> newSummary3 =
            entityPage.getInstanceSummary().getSummary();
        Assert.assertNotNull(
            EntityPage.InstanceSummary.getOneSummary(newSummary3, startTime),
            "Not expecting first instance to be displayed");
        Assert.assertNull(EntityPage.InstanceSummary.getOneSummary(newSummary3,
                TimeUtil.addMinsToTime(startTime, 5)),
            "Not expecting second instance to be displayed");
        Assert.assertNull(EntityPage.InstanceSummary.getOneSummary(newSummary3,
                TimeUtil.addMinsToTime(startTime, 10)),
            "Not expecting second instance to be displayed");

        //checking status
        displayedSummary.setInstanceSummaryStartTime("");
        displayedSummary.setInstanceSummaryEndTime("");
        displayedSummary.selectInstanceSummaryStatus("RUNNING");
        AssertUtil.assertEmpty(entityPage.getInstanceSummary().getSummary(),
            "Unexpected summary displayed.");
        displayedSummary.selectInstanceSummaryStatus("WAITING");
        Assert.assertEquals(entityPage.getInstanceSummary().getSummary().size(), 1,
            "Unexpected summary displayed.");
        displayedSummary.selectInstanceSummaryStatus("SUCCEEDED");
        Assert.assertEquals(entityPage.getInstanceSummary().getSummary().size(), 1,
            "Unexpected summary displayed.");
        displayedSummary.selectInstanceSummaryStatus("SUSPENDED");
        Assert.assertEquals(entityPage.getInstanceSummary().getSummary().size(), 1,
            "Unexpected summary displayed.");
    }

    /**
     * Test whether instance status changes are reflected by UI.
     * Run a process with at 1 running instance.
     * Suspend instance via API. Refresh page. Check that instance status changed on UI.
     * Resume it via API. Refresh page. Check that instance is running on UI.
     * @throws Exception
     */
    @Test
    public void testInstancesBlockStatusRunningSuspended() throws Exception {
        final String startTime = "2010-01-02T01:00Z";
        final String endTime = "2010-01-02T01:01Z";
        String prefix = bundles[0].getFeedDataPathPrefix();
        bundles[0].setProcessValidity(startTime, endTime);
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);
        final List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTime, endTime, 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        //suspend the instance
        prism.getProcessHelper().getProcessInstanceSuspend(process.getName(),
            "?start=" + startTime + "&end=" + endTime);

        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();
        final InstancesResult apiSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiSummary.getInstances());

        //resume the instance
        prism.getProcessHelper().getProcessInstanceResume(process.getName(),
            "?start=" + startTime + "&end=" + endTime);
        EntityPage newEntityPage = entityPage.refreshPage();
        final EntityPage.InstanceSummary newDisplayedSummary = newEntityPage.getInstanceSummary();
        final InstancesResult newApiSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        newDisplayedSummary.check();
        newDisplayedSummary.checkSummary(newApiSummary.getInstances());
    }

    /**
     * Check suspend/resume on multiple instances.
     * Run a process with at least 2 running instances. Check instances status on UI.
     * Select both instances and Pause them. Check paused status on UI & API.
     * Select them again and Resume them. Check running status via both UI and API.
     * @throws Exception
     */
    @Test
    public void testInstancesBlockActionsSuspendResume() throws Exception {
        final String startTime = "2010-01-02T01:00Z";
        final String endTime = "2010-01-02T01:06Z";
        String prefix = bundles[0].getFeedDataPathPrefix();
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(2);
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);
        final List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTime, endTime, 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 2,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();

        //suspend instances through ui
        final List<EntityPage.OneInstanceSummary> runningSummary = displayedSummary.getSummary();
        for (EntityPage.OneInstanceSummary oneInstanceSummary : runningSummary) {
            oneInstanceSummary.clickCheckBox();
        }
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Suspend);
        final List<EntityPage.OneInstanceSummary> suspendedSummary = displayedSummary.getSummary();
        Assert.assertEquals(suspendedSummary.get(0).getStatus(), "SUSPENDED",
            "Expecting first instance to be suspended");
        Assert.assertEquals(suspendedSummary.get(1).getStatus(), "SUSPENDED",
            "Expecting second instance to be suspended");
        final InstancesResult apiSuspendedSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiSuspendedSummary.getInstances());

        //resume instances through ui
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Resume);
        final List<EntityPage.OneInstanceSummary> resumedSummary = displayedSummary.getSummary();
        Assert.assertEquals(resumedSummary.get(0).getStatus(), "RUNNING",
            "Expecting first instance to be running");
        Assert.assertEquals(resumedSummary.get(1).getStatus(), "RUNNING",
            "Expecting second instance to be running");
        final InstancesResult apiResumeSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiResumeSummary.getInstances());
    }

    /**
     * Check kill/rerun on multiple instances.
     * Run a process with at least 2 running instances. Check instances status on UI.
     * Select both instances and Pause them. Check paused status on UI & API.
     * Select them again and Rerun them. Check running status via both UI and API.
     * @throws Exception
     */
    @Test
    public void testInstancesBlockActionsKillRerun() throws Exception {
        final String startTime = "2010-01-02T01:00Z";
        final String endTime = "2010-01-02T01:06Z";
        String prefix = bundles[0].getFeedDataPathPrefix();
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(2);
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);
        final List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTime, endTime, 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 2,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();

        //kill instances through ui
        final List<EntityPage.OneInstanceSummary> runningSummary = displayedSummary.getSummary();
        for (EntityPage.OneInstanceSummary oneInstanceSummary : runningSummary) {
            oneInstanceSummary.clickCheckBox();
        }
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Kill);
        final List<EntityPage.OneInstanceSummary> killedSummary = displayedSummary.getSummary();
        Assert.assertEquals(killedSummary.get(0).getStatus(), "KILLED",
            "Expecting first instance to be killed");
        Assert.assertEquals(killedSummary.get(1).getStatus(), "KILLED",
            "Expecting second instance to be killed");
        final InstancesResult apiKilledSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiKilledSummary.getInstances());

        //rerun instances through ui
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Rerun);
        final List<EntityPage.OneInstanceSummary> rerunSummary = displayedSummary.getSummary();
        Assert.assertEquals(rerunSummary.get(0).getStatus(), "RUNNING",
            "Expecting first instance to be running");
        Assert.assertEquals(rerunSummary.get(1).getStatus(), "RUNNING",
            "Expecting second instance to be running");
        final InstancesResult apiRerunSummary = prism.getProcessHelper().listInstances(
            process.getName(), "start=" + startTime, null);
        displayedSummary.check();
        displayedSummary.checkSummary(apiRerunSummary.getInstances());
    }


    /**
     * Check download log button on single/multiple instances.
     * Run a process and get into it's page.
     * Select two instances, the log button should be disabled.
     * Select one instance, the log button should work.
     * Check that the browser navigates to the correct page.
     * @throws Exception
     */
    @Test
    public void testSpecificInstanceLog() throws Exception {
        final String startTime = "2010-01-02T01:00Z";
        final String endTime = "2010-01-02T01:06Z";
        String prefix = bundles[0].getFeedDataPathPrefix();
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(2);
        final ProcessMerlin process = bundles[0].getProcessObject();
        bundles[0].submitAndScheduleAllFeeds();
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process.toString()));

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 1,
            CoordinatorAction.Status.WAITING, EntityType.PROCESS, 1);
        final List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startTime, endTime, 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        InstanceUtil.waitTillInstanceReachState(clusterOC, process.getName(), 2,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        final EntityPage entityPage = searchPage.openEntityPage(process.getName());
        final EntityPage.InstanceSummary displayedSummary = entityPage.getInstanceSummary();

        //kill instances through ui
        final List<EntityPage.OneInstanceSummary> runningSummary = displayedSummary.getSummary();
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Log);
        Assert.assertFalse(getDriver().getCurrentUrl().contains("oozie"),
            "No instance is selected, so, log button should be disabled. "
                + "Clicking instance log button should not take user to oozie page.");
        for (EntityPage.OneInstanceSummary oneInstanceSummary : runningSummary) {
            oneInstanceSummary.clickCheckBox();
        }
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Log);
        Assert.assertFalse(getDriver().getCurrentUrl().contains("oozie"),
            "Two instances are selected, so, log button should be disabled. "
                + "Clicking instance log button should not take user to oozie page.");
        runningSummary.get(1).clickCheckBox();
        //only first checkbox is ticked
        displayedSummary.performActionOnSelectedInstances(EntityPage.InstanceAction.Log);
        final String nominalTimeOfSelectedInstance = runningSummary.get(0).getNominalTime();
        final InstancesResult processInstanceLogs = prism.getProcessHelper()
            .getProcessInstanceLogs(process.getName(),
                "start=" + nominalTimeOfSelectedInstance
                + "&end=" + TimeUtil.addMinsToTime(nominalTimeOfSelectedInstance, 1));
        Assert.assertEquals(getDriver().getCurrentUrl().replaceFirst("/\\?", "?"),
            processInstanceLogs.getInstances()[0].getLogFile(),
            "Only one instance is selected. "
                + "Clicking instance log button should take user to oozie page.");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

}

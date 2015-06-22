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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.EntityPage;
import org.apache.falcon.regression.ui.search.InstancePage;
import org.apache.falcon.regression.ui.search.InstancePage.Button;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.falcon.resource.InstancesResult;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.EnumSet;

/**
 * Tests for Search UI Instance Page.
 */
@Test(groups = "search-ui")
public class InstancePageTest extends BaseUITestClass {

    private final ColoHelper cluster = servers.get(0);
    private final OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output" + MINUTE_DATE_PATTERN;
    private SearchPage searchPage;
    private InstancePage instancePage;
    private String instance = "2010-01-02T01:00Z";
    private String processName;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        openBrowser();
        searchPage = LoginPage.open(getDriver()).doDefaultLogin();
    }

    @BeforeMethod(alwaysRun = true)
    public void submitEntities() throws Exception {
        cleanAndGetTestDir();
        HadoopUtil.uploadDir(serverFS.get(0), aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:01Z");
        bundles[0].setProcessInputStartEnd("now(0, 0)", "now(0, 0)");
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].submitAndScheduleProcess();
        processName = bundles[0].getProcessName();
        searchPage.refresh();
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.WAITING, EntityType.PROCESS, 5);
    }

    @Test
    public void testInstancePageStatusWaitingRunning() throws Exception {
        instancePage = searchPage.openEntityPage(processName).openInstance(instance);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.WAITING);

        Assert.assertEquals(instancePage.getButtons(false), EnumSet.allOf(Button.class),
            "All buttons should be disabled for WAITING instance");

        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);

        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        instancePage = instancePage.refreshPage();
        checkInstanceStatuses(InstancesResult.WorkflowStatus.RUNNING);
        Assert.assertEquals(instancePage.getButtons(false), EnumSet.of(Button.Resume, Button.Rerun),
                "'Rerun' and 'Resume' buttons should be disabled for RUNNING instance");
    }

    @Test
    public void testInstancePagePauseResume() throws Exception {
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        instancePage = searchPage.openEntityPage(processName).openInstance(instance);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.RUNNING);

        instancePage.clickButton(Button.Suspend);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.SUSPENDED);

        Assert.assertEquals(instancePage.getButtons(false), EnumSet.of(Button.Rerun, Button.Suspend),
                "'Rerun' and 'Suspend' buttons should be disabled for SUSPENDED instance");

        instancePage.clickButton(Button.Resume);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.RUNNING);
    }

    @Test
    public void testInstancePageKillRerun() throws Exception {
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

        instancePage = searchPage.openEntityPage(processName).openInstance(instance);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.RUNNING);

        instancePage.clickButton(Button.Kill);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.KILLED);

        Assert.assertEquals(instancePage.getButtons(true), EnumSet.of(Button.Rerun),
                "Only 'Rerun' button should be active for KILLED instance");

        instancePage.clickButton(Button.Rerun);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.RUNNING);
    }

    @Test
    public void testInstancePageInfo() throws Exception {
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 5);

        EntityPage entityPage = searchPage.openEntityPage(processName);
        EntityPage.OneInstanceSummary summary = EntityPage.InstanceSummary
                .getOneSummary(entityPage.getInstanceSummary().getSummary(), instance);
        Assert.assertNotNull(summary, "Instance not found on entity page");
        Assert.assertEquals(summary.getStatus(), "SUCCEEDED", "Unexpected status on entity page");

        instancePage = entityPage.openInstance(instance);
        checkInstanceStatuses(InstancesResult.WorkflowStatus.SUCCEEDED);

        Assert.assertEquals(instancePage.getEntityName(), processName, "Process name isn't shown correctly");

        Assert.assertTrue(instancePage.isLineagePresent(), "Lineage graph should be present");
    }

    @Test
    public void testHeader() {
        instancePage = searchPage.openEntityPage(processName).openInstance(instance);
        instancePage.getPageHeader().checkHeader();

    }


    private void checkInstanceStatuses(InstancesResult.WorkflowStatus status) throws Exception {
        Assert.assertEquals(instancePage.getStatus(), status.toString(), "Unexpected status on UI");

        InstancesResult.Instance[] instances = prism.getProcessHelper()
                .listInstances(processName, "start=" + instance, null).getInstances();
        Assert.assertNotNull(instance, "Instances not found via API");
        Assert.assertEquals(instances.length, 1, "Only one instance expected via API");
        Assert.assertEquals(instances[0].getStatus(), status, "Unexpected status via API");
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() {
        closeBrowser();
    }
}

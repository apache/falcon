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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.Page.EntityStatus;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;

/** UI tests for entities table with search results. */
@Test(groups = "search-ui")
public class EntitiesTableReflectionTest extends BaseUITestClass {
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    private SearchPage searchPage = null;

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
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitBundle(prism);

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
    public void testActionsPauseResume() throws URISyntaxException,
        AuthenticationException, InterruptedException, IOException, JAXBException {
        String processName = Util.readEntityName(bundles[0].getProcessData());
        Assert.assertEquals(searchPage.doSearch(processName).size(), 1,
            "One result should be present");
        Assert.assertEquals(searchPage.doSearch(processName).get(0).getStatus(),
            EntityStatus.SUBMITTED, "Status of process should be SUBMITTED");
    }


    @AfterClass(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
        closeBrowser();
    }

}

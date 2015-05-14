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
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.ProcessWizardPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/** UI tests for process creation. */
@Test(groups = "search-ui")
public class ProcessSetupTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(ProcessSetupTest.class);
    private final ColoHelper cluster = servers.get(0);
    private final FileSystem clusterFS = serverFS.get(0);
    private final OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output" + MINUTE_DATE_PATTERN;
    private ProcessWizardPage processWizardPage = null;

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
        SearchPage searchPage = loginPage.doDefaultLogin();
        processWizardPage = searchPage.getPageHeader().doCreateProcess();
        processWizardPage.checkPage();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
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
        processWizardPage.getPageHeader().checkHeader();
    }

    /**
     * Populate fields with valid values (name, tag, workflow, engine, version, wf path)
     * and check that user can go to the next step.
     */
    @Test
    public void testGeneralStepDefaultScenario() throws Exception {
        final ProcessMerlin process = bundles[0].getProcessObject();
        final List<String> tags = Arrays.asList("first=yes", "second=yes", "third=yes", "wrong=no");
        process.setTags(StringUtils.join(tags, ","));
        processWizardPage.doStep1(process);
    }

}

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

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.*;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.ProcessWizardPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/** UI tests for process creation. */
@Test(groups = "search-ui")
public class ProcessSetupTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(ProcessSetupTest.class);
    private final ColoHelper cluster = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output" + MINUTE_DATE_PATTERN;
    private ProcessWizardPage processWizardPage = null;
    private final List<String> timeZones = new ArrayList<>(Arrays.asList(
        "-Select timezone-", "UTC", "(GMT -12:00) Eniwetok, Kwajalein",
        "(GMT -11:00) Midway Island, Samoa", "(GMT -10:00) Hawaii", "(GMT -9:00) Alaska",
        "(GMT -8:00) Pacific Time (US & Canada)", "(GMT -7:00) Mountain Time (US & Canada)",
        "(GMT -6:00) Central Time (US & Canada), Mexico City",
        "(GMT -5:00) Eastern Time (US & Canada), Bogota, Lima",
        "(GMT -4:00) Atlantic Time (Canada), Caracas, La Paz", "(GMT -3:30) Newfoundland",
        "(GMT -3:00) Brazil, Buenos Aires, Georgetown", "(GMT -2:00) Mid-Atlantic",
        "(GMT -1:00 hour) Azores, Cape Verde Islands",
        "(GMT) Western Europe Time, London, Lisbon, Casablanca",
        "(GMT +1:00 hour) Brussels, Copenhagen, Madrid, Paris",
        "(GMT +2:00) Kaliningrad, South Africa",
        "(GMT +3:00) Baghdad, Riyadh, Moscow, St. Petersburg", "(GMT +3:30) Tehran",
        "(GMT +4:00) Abu Dhabi, Muscat, Baku, Tbilisi", "(GMT +4:30) Kabul",
        "(GMT +5:00) Ekaterinburg, Islamabad, Karachi, Tashkent",
        "(GMT +5:30) Bombay, Calcutta, Madras, New Delhi", "(GMT +5:45) Kathmandu",
        "(GMT +6:00) Almaty, Dhaka, Colombo", "(GMT +7:00) Bangkok, Hanoi, Jakarta",
        "(GMT +8:00) Beijing, Perth, Singapore, Hong Kong",
        "(GMT +9:00) Tokyo, Seoul, Osaka, Sapporo, Yakutsk", "(GMT +9:30) Adelaide, Darwin",
        "(GMT +10:00) Eastern Australia, Guam, Vladivostok",
        "(GMT +11:00) Magadan, Solomon Islands, New Caledonia",
        "(GMT +12:00) Auckland, Wellington, Fiji, Kamchatka"
    ));

    private final List<String> timeUnits = new ArrayList<>(Arrays.asList("minutes", "hours", "days", "months"));
    private final List<String> delayTimeUnits = new ArrayList<>(Arrays.asList("-Select delay-", "minutes",
        "hours", "days", "months"));
    private final List<String> parallel = new ArrayList<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7",
        "8", "9", "10", "11", "12"));
    private final List<String> order = new ArrayList<>(Arrays.asList("-Select order-", "FIFO", "LIFO", "LAST_ONLY"));
    private final List<String> policy =new ArrayList<>(Arrays.asList("-Select policy-", "periodic", "exp-backoff",
        "final"));

    private ProcessMerlin process;

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
        process = bundles[0].getProcessObject();
        //we need to reduce name size to 39 symbols
        process.setName(process.getName().substring(0, 39));
        process.setTags("first=yes,second=yes,third=no");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

    /* Step 1 tests */

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
     * @throws Exception
     */
    @Test
    public void testGeneralStepDefaultScenario() throws Exception {
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Assert on the click of next, the Page moves to the next page
        processWizardPage.isFrequencyQuantityDisplayed(true);
    }

    /**
     * Populate fields with valid values (name, tag, workflow, engine, version, wf path)
     * Check that they are reflected on XML preview.
     * @throws Exception
     */
    @Test
    public void testGeneralStepXmlPreview() throws Exception{

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);

        // Get process from XML Preview
        ProcessMerlin processFromXML = processWizardPage.getProcessMerlinFromProcessXml();

        // Assert all the values entered on the General Info Page
        LOGGER.info(String.format("Comparing source process: %n%s%n and preview: %n%s%n.", process, processFromXML));
        process.assertGeneralProperties(processFromXML);
    }

    /**
     * Add few tags to the process. Click edit XML. Remove both tags from XML.
     * Check that properties were removed from matching fields.
     * Now click Edit XML again. Add new tag, Pig engine (instead of Oozie) with one of existing versions to the XML.
     * Check that changes have been reflected on wizard page.
     * @throws Exception
     */
    @Test
    public void testGeneralStepEditXml() throws Exception{

        // Set tag in process
        process.setTags("first=yes,second=yes");

        // Set tag and group on the Wizard
        processWizardPage.setTags(process.getTags());

        // Get XML, and set tag and group back to null
        ProcessMerlin processFromXML = processWizardPage.getProcessMerlinFromProcessXml();
        processFromXML.setTags(null);

        // Now click EditXML and set the updated XML here
        processWizardPage.clickEditXml();
        String xmlToString = processFromXML.toString();
        processWizardPage.setProcessXml(xmlToString);
        processWizardPage.clickEditXml();

        Thread.sleep(1000);
        // Assert that there is only one Tag on the Wizard window
        processWizardPage.isTagsDisplayed(0, true);
        processWizardPage.isTagsDisplayed(1, false);

        // Assert that the Tag value is empty on the Wizard window
        Assert.assertEquals(processWizardPage.getTagKeyText(0), "",
            "Tag Key Should be empty on the Wizard window");
        Assert.assertEquals(processWizardPage.getTagValueText(0), "",
            "Tag Value Should be empty on the Wizard window");

        // Set Tag and Engine values
        processFromXML.setTags("third=yes,fourth=no");
        processFromXML.getWorkflow().setEngine(EngineType.PIG);
        processFromXML.getWorkflow().setVersion("pig-0.13.0");

        // Now click EditXML and set the updated XML here
        processWizardPage.clickEditXml();
        xmlToString = processFromXML.toString();
        processWizardPage.setProcessXml(xmlToString);
        processWizardPage.clickEditXml();

        // Assert that there are two Tags on the Wizard window
        processWizardPage.isTagsDisplayed(0, true);
        processWizardPage.isTagsDisplayed(1, true);

        // Assert that the Tag values are correct on the Wizard window
        Assert.assertEquals(processWizardPage.getTagKeyText(0), "third",
            "Unexpected Tag1 Key on the Wizard window");
        Assert.assertEquals(processWizardPage.getTagValueText(0), "yes",
            "Unexpected Tag1 Value on the Wizard window");
        Assert.assertEquals(processWizardPage.getTagKeyText(1), "fourth",
            "Unexpected Tag2 Key on the Wizard window");
        Assert.assertEquals(processWizardPage.getTagValueText(1), "no",
            "Unexpected Tag2 Value on the Wizard window");
        Assert.assertEquals(processWizardPage.isPigRadioSelected(), true,
            "Unexpected Engine on the Wizard window");
        Assert.assertTrue(processWizardPage.getEngineVersionText().contains("pig-0.13.0"),
            "Unexpected Engine Version on the Wizard window");
    }

    /**
     * Add two tags to the process. Check that it is present.
     * Delete the tag. Check that it has been removed.
     * @throws Exception
     */
    @Test
    public void testGeneralStepAddRemoveTag() throws Exception{

        // Set tag in process
        process.setTags("first=yes,second=yes");

        // Set tag and group on the Wizard
        processWizardPage.setTags(process.getTags());

        // Assert that there are two Tags on the Wizard window
        processWizardPage.isTagsDisplayed(0, true);
        processWizardPage.isTagsDisplayed(1, true);

        // Delete the tags
        processWizardPage.deleteTags();

        // Assert that there is only one Tag on the Wizard window
        processWizardPage.isTagsDisplayed(0, true);
        processWizardPage.isTagsDisplayed(1, false);

        // Assert that the Tag value is empty on the Wizard window
        Assert.assertEquals(processWizardPage.getTagKeyText(0), "",
            "Tag Key Should be empty on the Wizard window");
        Assert.assertEquals(processWizardPage.getTagValueText(0), "",
            "Tag Value Should be empty on the Wizard window");
    }

    /* Step 2 tests */

    /**
     * Populate all fields with valid values (frequency, parallel, retry).
     * Check that user can go to the next step.
     * @throws Exception
     */
    @Test
    public void testTimingStepDefaultScenario() throws Exception{

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);

        processWizardPage.clickNext();
        // Assert that user is able to go to next page
        processWizardPage.isValidityStartDateDisplayed(true);
    }

    /**
     * Populate fields with valid values (frequency, late arrival, availability flag, so on)
     * Check that they are reflected on XML preview.
     * @throws Exception
     */
    @Test
    public void testTimingStepXmlPreview() throws Exception{

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);

        // Get process from XML Preview
        ProcessMerlin processFromXML = processWizardPage.getProcessMerlinFromProcessXml();

        // Assert all the values entered on the Properties Page
        LOGGER.info(String.format("Comparing source process: %n%s%n and preview: %n%s%n.", process, processFromXML));
        process.assertPropertiesInfo(processFromXML);
    }

    /**
     * Add some properties to the feed (frequency, late parallel). Click edit XML.
     * Remove both properties from XML. Check that properties were removed from matching fields.
     * Now click Edit XML again. Add timezone and order properties to the XML.
     * Check that Timezone and order were enabled and set to values what we've populated in XML.
     * @throws Exception
     */
    @Test
    public void testTimingStepEditXml() throws Exception{

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set Frequency and Parallel values on the Properties Page
        processWizardPage.setFrequencyQuantity("10");
        processWizardPage.setFrequencyUnit("minutes");
        processWizardPage.setMaxParallelInstances(5);

        // Get process from XML Preview
        ProcessMerlin processFromXML = processWizardPage.getProcessMerlinFromProcessXml();
        processFromXML.setFrequency(null);
        processFromXML.setParallel(1);

        // Now click EditXML and set the updated XML here
        processWizardPage.clickEditXml();
        String xmlToString = processFromXML.toString();
        processWizardPage.setProcessXml(xmlToString);
        processWizardPage.clickEditXml();

        // Assert Frequency and Parallel values
        Assert.assertEquals(processWizardPage.getFrequencyQuantityText(), "",
            "Frequency Quantity Should be empty on the Wizard window");
        Assert.assertEquals(processWizardPage.getMaxParallelInstancesText(), "1",
            "Unexpected Parallel on the Wizard window");

        // Get process from XML Preview
        processFromXML = processWizardPage.getProcessMerlinFromProcessXml();
        // Set TimeZone and Order
        TimeZone tz = TimeZone.getTimeZone("GMT-08:00");
        processFromXML.setTimezone(tz);
        processFromXML.setOrder(ExecutionType.LIFO);

        // Now click EditXML and set the updated XML here
        processWizardPage.clickEditXml();
        xmlToString = processFromXML.toString();
        processWizardPage.setProcessXml(xmlToString);
        processWizardPage.clickEditXml();

        // Assert TimeZone and Order
        Assert.assertEquals(processWizardPage.getOrderText(), "LIFO",
            "Unexpected Order on the Wizard window");
        Assert.assertEquals(processWizardPage.getTimezoneText(), "GMT-08:00",
            "Unexpected TimeZone on the Wizard window");
    }

    /**
     * Check that timezone, frequency, parallel, order, retry policy,
     * retry delay drop down lists contain correct items (time units etc.).
     * @throws Exception
     */
    @Test
    public void testTimingStepDropDownLists() throws Exception{

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Assert dropdown values
        List<String> dropdownValues = processWizardPage.getTimezoneValues();
        Assert.assertEquals(timeZones, dropdownValues, "TimeZone Values Are Not Equal");

        dropdownValues = processWizardPage.getFrequencyUnitValues();
        Assert.assertEquals(timeUnits, dropdownValues, "Frequency Unit Values Are Not Equal");

        dropdownValues = processWizardPage.getMaxParallelInstancesValues();
        Assert.assertEquals(parallel, dropdownValues, "Max Parallel Values Are Not Equal");

        dropdownValues = processWizardPage.getOrderValues();
        Assert.assertEquals(order, dropdownValues, "Order Unit Values Are Not Equal");

        dropdownValues = processWizardPage.getRetryPolicyValues();
        Assert.assertEquals(policy, dropdownValues, "Retry Policy Values Are Not Equal");

        dropdownValues = processWizardPage.getRetryDelayUnitValues();
        Assert.assertEquals(delayTimeUnits, dropdownValues, "Retry Delay Unit Values Are Not Equal");
    }

    /* Step 3 tests */

    /**
     * testClustersStepDefaultScenario
     * Populate each field with correct values (name, validity ...). Check that
     * user can go to the next step.
     */
    @Test
    public void testClustersStepDefaultScenario()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException, JAXBException {
        bundles[0].submitClusters(cluster);
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();
        processWizardPage.isAddInputButtonDisplayed(true);
    }

    /**
     * Check that cluster drop down list contains correct list of items.
     */
    @Test
    public void testClustersStepDropDownList()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException, JAXBException {
        //submit all clusters
        List<String> clusters = new ArrayList<>();
        ClusterMerlin clusterMerlin = bundles[0].getClusterElement();
        String clusterName = clusterMerlin.getName();
        for(int i = 1; i < 6; i++) {
            clusterMerlin.setName(clusterName + i);
            AssertUtil.assertSucceeded(cluster.getClusterHelper().submitEntity(clusterMerlin.toString()));
            clusters.add(clusterMerlin.getName());
        }
        //go to clusters page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();
        List<String> dropdownValues = new ArrayList<>(processWizardPage.getClustersFromDropDown());

        //clean all clusters which belong to anything else then current test class
        String testClassName = ProcessSetupTest.class.getSimpleName();
        for(int i = 0; i < dropdownValues.size(); i++) {
            if (!dropdownValues.get(i).contains(testClassName)) {
                dropdownValues.remove(i);
            }
        }
        Collections.sort(clusters);
        Collections.sort(dropdownValues);
        Assert.assertEquals(clusters, dropdownValues, "Clusters Drop Down Values Are Not Equal");
    }

    /**
     * Click on validity start/end, check that pop up calendars have been shown.
     */
    @Test
    public void testClustersStepPopupCalendars() {
        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        //click on respective fields
        processWizardPage.clickOnValidityStart();
        processWizardPage.clickOnValidityEnd();
    }

    /**
     * Click on add cluster. Check that new cluster block appears. Populate it with values
     * and check that XML preview shows both clusters. Remove the cluster and check that XML
     * has the only cluster.
     */
    @Test
    public void testClustersStepAddDeleteCluster() throws Exception {
        bundles[0].submitClusters(cluster);
        //submit one extra cluster
        ClusterMerlin clusterMerlin = bundles[0].getClusterElement();
        clusterMerlin.setName(clusterMerlin.getName() + 1);
        AssertUtil.assertSucceeded(cluster.getClusterHelper().submitEntity(clusterMerlin.toString()));

        Cluster processCluster = new Cluster();
        processCluster.setName(clusterMerlin.getName());
        processCluster.setValidity(process.getClusters().getClusters().get(0).getValidity());
        process.addProcessCluster(processCluster);

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Add clusters
        processWizardPage.setClusters(process.getClusters());
        ProcessMerlin xmlPreview = processWizardPage.getProcessMerlinFromProcessXml();

        //compare clusters
        LOGGER.info(String.format("Comparing clusters of process: %n%s%n and preview: %n%s%n.", process, xmlPreview));
        ProcessMerlin.assertClustersEqual(process.getClusters().getClusters(), xmlPreview.getClusters().getClusters());

        //delete one cluster and repeat the check
        processWizardPage.deleteLastCluster();
        xmlPreview = processWizardPage.getProcessMerlinFromProcessXml();
        process.getClusters().getClusters().remove(1);

        //compare clusters
        LOGGER.info(String.format("Comparing clusters of process: %n%s%n and preview: %n%s%n.", process, xmlPreview));
        ProcessMerlin.assertClustersEqual(process.getClusters().getClusters(), xmlPreview.getClusters().getClusters());
    }

    /**
     * Populate all fields with valid values and check that they are reflected on XML
     * preview. Click edit XML. Change validity and cluster name. Check that changes
     * are reflected on XML as well as on wizard page. Edit xml again. Add new cluster
     * and check that new cluster block has been added on wizard page.
     */
    @Test
    public void testClusterStepEditXml() throws Exception {
        bundles[0].submitClusters(cluster);
        ClusterMerlin clusterMerlin = bundles[0].getClusterElement();
        String firstClusterName = clusterMerlin.getName();
        clusterMerlin.setName(firstClusterName + 2);
        AssertUtil.assertSucceeded(cluster.getClusterHelper().submitEntity(clusterMerlin.toString()));

        //set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        //set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        //set cluster
        processWizardPage.setClusters(process.getClusters());

        //compare preview and source data
        ProcessMerlin xmlPreview = processWizardPage.getProcessMerlinFromProcessXml();
        LOGGER.info(String.format("Comparing clusters of process: %n%s%n and preview: %n%s%n.", process, xmlPreview));
        ProcessMerlin.assertClustersEqual(process.getClusters().getClusters(), xmlPreview.getClusters().getClusters());

        //change validity and name and push it to xmlPreview
        Date date = new Date();
        xmlPreview.getClusters().getClusters().get(0).getValidity().setEnd(date);
        xmlPreview.getClusters().getClusters().get(0).setName(clusterMerlin.getName());
        processWizardPage.clickEditXml();
        processWizardPage.setProcessXml(xmlPreview.toString());
        processWizardPage.clickEditXml();

        //check that validity end is changed on wizard
        String endUI = processWizardPage.getValidityEnd();
        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        String endSource = format.format(date);
        Assert.assertEquals(endUI, endSource, "Validity end should be updated on wizard.");
        Assert.assertEquals(processWizardPage.getClusterName(0), clusterMerlin.getName(),
            "Cluster name should be updated on wizard.");

        //add cluster to process preview
        int initCount = processWizardPage.getWizardClusterCount();
        Cluster processCluster = new Cluster();
        process = new ProcessMerlin(xmlPreview);
        processCluster.setName(firstClusterName);
        processCluster.setValidity(xmlPreview.getClusters().getClusters().get(0).getValidity());
        process.addProcessCluster(processCluster);
        processWizardPage.clickEditXml();
        processWizardPage.setProcessXml(xmlPreview.toString());
        processWizardPage.clickEditXml();

        //check that changes are reflected on wizard
        int finalCount = processWizardPage.getWizardClusterCount();
        Assert.assertEquals(finalCount - initCount, 1, "Cluster should have been added to wizard.");
        Assert.assertEquals(processWizardPage.getClusterName(1), firstClusterName,
            "Cluster name should be updated on wizard.");
    }

    /* Step 4 tests */

    /**
     * Add input and output. Populate each field with correct values(name, feed, instance ...).
     * Check that user can go to the next step.
     * @throws Exception
     */
    @Test
    public void testInOutStepDefaultScenario() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set values on the Input Output Page
        processWizardPage.setInputOutputInfo(process);
        processWizardPage.clickNext();

        // Assert that user is able to go on the next page
        processWizardPage.isSaveButtonDisplayed(true);


    }

    /**
     * Check that user is allowed to go to the next page without adding inputs/outputs.
     * @throws Exception
     */
    @Test
    public void testInOutStepWithoutInOuts() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Do not set values on the Input Output Page
        processWizardPage.clickNext();

        // Assert that user is able to go on the next page
        processWizardPage.isSaveButtonDisplayed(true);

    }

    /**
     * Add input. Set instance with start time after end.
     * Check that user is not allowed to go to the next step and has been notified with an alert.
     * Check the same for invalid EL expression.
     * @throws Exception
     */
    @Test
    public void testInOutInvalidInstance() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set start date after end date in Input
        process.getInputs().getInputs().get(0).setStart("now(0, 0)");
        process.getInputs().getInputs().get(0).setEnd("now(0, -5)");

        // Set Input Values on the Input Output Page
        processWizardPage.setInputInfo(process.getInputs());
        processWizardPage.clickNext();

        // Assert User should not be allowed to go on the next page
        processWizardPage.isSaveButtonDisplayed(false);

        // Delete the current Input
        processWizardPage.clickDeleteInput();

        // Set invalid EL expression
        process.getInputs().getInputs().get(0).setStart("bad(0, 0)");
        process.getInputs().getInputs().get(0).setEnd("bad(0, 0)");

        // Set new Input Values on the Input Output Page
        processWizardPage.setInputInfo(process.getInputs());
        processWizardPage.clickNext();

        // Assert User should not be allowed to go on the next page
        processWizardPage.isSaveButtonDisplayed(false);
    }

    /**
     * Check that input/output feed drop down list contains correct list of feeds.
     * @throws Exception
     */
    @Test
    public void testInOutStepDropDownFeeds() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Click Add Input and Output Buttons
        processWizardPage.clickAddInput();
        processWizardPage.clickAddOutput();

        List<String> expectedDropdownValues = new ArrayList<>();
        expectedDropdownValues.add("-Select feed-");
        expectedDropdownValues.add(process.getInputs().getInputs().get(0).getFeed());
        expectedDropdownValues.add(process.getOutputs().getOutputs().get(0).getFeed());
        Collections.sort(expectedDropdownValues);

        // Assert Input and Output Feed Dropdown values
        List<String> actualDropdownValues = processWizardPage.getInputValues(0);
        Collections.sort(actualDropdownValues);
        Assert.assertEquals(expectedDropdownValues, actualDropdownValues,
            "Input Feed Dropdown Values Are Not Equal");

        actualDropdownValues = processWizardPage.getOutputValues(0);
        Collections.sort(actualDropdownValues);
        Assert.assertEquals(expectedDropdownValues, actualDropdownValues,
            "Output Feed Dropdown Values Are Not Equal");
    }

    /**
     * Add input. Check that it has been added to wizard as well as to XML preview.
     * Click edit xml. Change input name and add output in xml.
     * Check that output has been added on wizard page and input name has been changed as well.
     * @throws Exception
     */
    @Test
    public void testInOutStepPreviewEditXml() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set Input Values on the Input Output Page
        processWizardPage.setInputInfo(process.getInputs());

        // Assert Input values on Wizard
        Assert.assertEquals(processWizardPage.getInputNameText(0), process.getInputs().getInputs().get(0).getName(),
            "Unexpected Input Name on the Wizard window");
        Assert.assertEquals(processWizardPage.getInputFeedText(0), process.getInputs().getInputs().get(0).getFeed(),
            "Unexpected Input Feed on the Wizard window");
        Assert.assertEquals(processWizardPage.getInputStartText(0), process.getInputs().getInputs().get(0).getStart(),
            "Unexpected Input Start on the Wizard window");
        Assert.assertEquals(processWizardPage.getInputEndText(0), process.getInputs().getInputs().get(0).getEnd(),
            "Unexpected Input End on the Wizard window");

        // Get process from XML Preview
        ProcessMerlin processFromXML = processWizardPage.getProcessMerlinFromProcessXml();

        // Assert Input values on the XML Preview
        LOGGER.info(String.format("Comparing source process: %n%s%n and preview: %n%s%n.", process, processFromXML));
        process.assertInputValues(processFromXML);

        // Change Input Name and Set Output in the XML
        processFromXML.getInputs().getInputs().get(0).setName("newInputData");
        processFromXML.setOutputs(process.getOutputs());

        // Now click EditXML and set the updated XML here
        processWizardPage.clickEditXml();
        String xmlToString = processFromXML.toString();
        processWizardPage.setProcessXml(xmlToString);
        processWizardPage.clickEditXml();

        // Assert Input Name and Output values on Wizard
        Assert.assertEquals(processWizardPage.getInputNameText(0), "newInputData",
            "Unexpected Input Name on the Wizard window");
        Assert.assertEquals(processWizardPage.getOutputNameText(0),
            process.getOutputs().getOutputs().get(0).getName(),
            "Unexpected Output Name on the Wizard window");
        Assert.assertEquals(processWizardPage.getOutputFeedText(0),
            process.getOutputs().getOutputs().get(0).getFeed(),
            "Unexpected Output Feed on the Wizard window");
        Assert.assertEquals(processWizardPage.getOutputInstanceText(0),
            process.getOutputs().getOutputs().get(0).getInstance(),
            "Unexpected Output Instance on the Wizard window");
    }

    /**
     * Add input. Check that it has been added to wizard as well to XML.
     * Delete it. Check that it has been removed from wizard as well as from XML.
     * Repeat the same for the output.
     * @throws Exception
     */
    @Test
    public void testInOutStepAddDeleteInOut() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set Input Values on the Input Output Page
        processWizardPage.setInputInfo(process.getInputs());

        // Assert Input values on Wizard
        Assert.assertEquals(processWizardPage.getInputNameText(0), process.getInputs().getInputs().get(0).getName(),
            "Unexpected Input Name on the Wizard window");
        Assert.assertEquals(processWizardPage.getInputFeedText(0), process.getInputs().getInputs().get(0).getFeed(),
            "Unexpected Input Feed on the Wizard window");
        Assert.assertEquals(processWizardPage.getInputStartText(0), process.getInputs().getInputs().get(0).getStart(),
            "Unexpected Input Start on the Wizard window");
        Assert.assertEquals(processWizardPage.getInputEndText(0), process.getInputs().getInputs().get(0).getEnd(),
            "Unexpected Input End on the Wizard window");

        // Get process from XML Preview
        ProcessMerlin processFromXML = processWizardPage.getProcessMerlinFromProcessXml();

        // Assert Input values on the XML Preview
        LOGGER.info(String.format("Comparing source process: %n%s%n and preview: %n%s%n.", process, processFromXML));
        process.assertInputValues(processFromXML);

        // Delete the input
        processWizardPage.clickDeleteInput();

        // Assert on the click of delete Input the Input is deleted
        processWizardPage.isInputNameDisplayed(0, false);

        // Set Output Values on the Input Output Page
        processWizardPage.setOutputInfo(process.getOutputs());

        // Assert Output values on Wizard
        Assert.assertEquals(processWizardPage.getOutputNameText(0),
            process.getOutputs().getOutputs().get(0).getName(),
            "Unexpected Output Name on the Wizard window");
        Assert.assertEquals(processWizardPage.getOutputFeedText(0),
            process.getOutputs().getOutputs().get(0).getFeed(),
            "Unexpected Output Feed on the Wizard window");
        Assert.assertEquals(processWizardPage.getOutputInstanceText(0),
            process.getOutputs().getOutputs().get(0).getInstance(),
            "Unexpected Output Instance on the Wizard window");

        // Get process from XML Preview
        processFromXML = processWizardPage.getProcessMerlinFromProcessXml();

        // Assert Output values on the XML Preview
        LOGGER.info(String.format("Comparing source process : %n%s%n and preview: %n%s%n.", process, processFromXML));
        process.assertOutputValues(processFromXML);

        // Delete the output
        processWizardPage.clickDeleteOutput();

        // Assert on the click of delete Input the Input is deleted
        processWizardPage.isOutputNameDisplayed(0, false);
    }

    /* Step 5 tests */

    /**
     * Create process. Using API check that process was created.
     * @throws Exception
     */
    @Test
    public void testSummaryStepDefaultScenario() throws Exception{

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set values on the Input Output Page
        processWizardPage.setInputOutputInfo(process);
        processWizardPage.clickNext();

        // Save the Process
        processWizardPage.clickSave();

        // Assert the response using API to validate if the feed creation went successfully
        ServiceResponse response = prism.getProcessHelper().getEntityDefinition(process.toString());
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Go through all properties which are shown on page. Check that they are equal to
     * those which were populated in previous steps.
     */
    @Test
    public void testSummaryStepAllProperties()
        throws URISyntaxException, IOException, AuthenticationException, InterruptedException, JAXBException {
        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        process.setTags("first=yes,second=yes,third=no");
        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set values on the Input Output Page
        processWizardPage.setInputOutputInfo(process);
        processWizardPage.clickNext();

        //get process from summary and compare it with the source
        ProcessMerlin summaryProcess = ProcessMerlin.getEmptyProcess(process);
        summaryProcess = processWizardPage.getProcessFromSummaryBox(summaryProcess);
        summaryProcess.assertEquals(process);
    }

    /**
     * Check that all properties which are shown on page are equal to those which are
     * shown on XML Preview. Click Edit XML. Add new input. Check that it has been
     * added on wizard.
     */
    @Test
    public void testSummaryStepEditXml() throws Exception {

        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        process.setTags("first=yes,second=yes,third=no");
        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set values on the Input Output Page
        processWizardPage.setInputOutputInfo(process);
        processWizardPage.clickNext();

        //get process from summary and from xml and compare them
        ProcessMerlin summaryProcess = ProcessMerlin.getEmptyProcess(process);
        summaryProcess = processWizardPage.getProcessFromSummaryBox(summaryProcess);
        ProcessMerlin previewProcess = processWizardPage.getProcessMerlinFromProcessXml();
        summaryProcess.assertEquals(previewProcess);

        //add input to preview cluster
        Input oldInput = previewProcess.getInputs().getInputs().get(0);
        Input newInput = new Input();
        newInput.setFeed(oldInput.getFeed());
        newInput.setName("newInput");
        newInput.setStart("now(-40, 0)");
        newInput.setEnd("now(20, 0)");
        previewProcess.getInputs().getInputs().add(newInput);

        //push new process to xml preview
        processWizardPage.clickEditXml();
        processWizardPage.setProcessXml(previewProcess.toString());
        processWizardPage.clickEditXml();

        //get process from summary and check that new input is available
        summaryProcess = processWizardPage.getProcessFromSummaryBox(ProcessMerlin.getEmptyProcess(summaryProcess));
        LOGGER.info(String.format("Comparing summary : %n%s%n and preview: %n%s%n.", summaryProcess, previewProcess));
        summaryProcess.assertInputValues(previewProcess);
    }

    /**
     * Click on EditXML. Break xml. Check that entity is
     * not accepted and preview xml is being reverted to previous state.
     */
    @Test
    public void testSummaryStepEditXmlInvalidChanges()
        throws Exception {
        bundles[0].submitClusters(cluster);
        bundles[0].submitFeeds(prism);

        bundles[0].getInputFeedNameFromBundle();

        process.setTags("first=yes,second=yes,third=no");
        // Set values on the General Info Page
        processWizardPage.setProcessGeneralInfo(process);
        processWizardPage.clickNext();

        // Set values on the Properties Page
        processWizardPage.setProcessPropertiesInfo(process);
        processWizardPage.clickNext();

        // Set values on the Cluster Info Page
        processWizardPage.setProcessClustersInfo(process);
        processWizardPage.clickNext();

        // Set values on the Input Output Page
        processWizardPage.setInputOutputInfo(process);
        processWizardPage.clickNext();

        //get process from xml preview
        ProcessMerlin previewProcess1 = processWizardPage.getProcessMerlinFromProcessXml();
        String processString = previewProcess1.toString();

        //damage the xml and populate it back to preview
        processString = processString.substring(0, processString.length() - 3);
        processWizardPage.clickEditXml();
        processWizardPage.setProcessXml(processString);
        processWizardPage.clickEditXml();

        //get xml preview and compare with initial state
        ProcessMerlin previewProcess2 = processWizardPage.getProcessMerlinFromProcessXml();
        previewProcess2.assertEquals(previewProcess1);
    }
}

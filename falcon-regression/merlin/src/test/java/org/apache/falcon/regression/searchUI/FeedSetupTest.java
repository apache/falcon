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
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.apache.falcon.regression.Entities.ClusterMerlin;
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
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    private ClusterMerlin clusterFromBundle;
    private final String[] tags = {"first=yes", "second=yes", "third=yes", "wrong=no"};
    private final List<String> timeUnits = new ArrayList<>(Arrays.asList("minutes", "hours", "days", "months"));
    private final List<String> jobPriorities = new ArrayList<>(Arrays.asList("-Select job-",
        "Very high", "High", "Normal", "Low", "Very Low"));
    private final String catalogUri = "catalog:default:test_input_table#dt=${YEAR}-${MONTH}-${DAY}-${HOUR}-${MINUTE}";

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
        if (tagsList.isEmpty()){
            return null;
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
        clusterFromBundle = bundle.getClusterElement();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }

    /**
     * Check that buttons (logout, entities, uploadXml, help, Falcon) are present, and names are
     * correct.
     * Check the user name on header.
     * Create an entity / upload an entity headers. Check that each button navigates user to
     * correct page
     * @throws Exception
     */
    @Test
    public void testHeader() throws Exception {
        feedWizardPage.getPageHeader().checkHeader();
    }

    /**
     * Run full feed creation scenario.
     * @throws Exception
     */
    @Test
    public void testWizardDefaultScenario() throws Exception {
        feed.setTags(getRandomTags());
        feed.setGroups("groups");
        feed.setAvailabilityFlag("_SUCCESS");
        feedWizardPage.setFeed(feed);
        //Check the response to validate if the feed creation went successfully
        ServiceResponse response = prism.getFeedHelper().getEntityDefinition(feed.toString());
        AssertUtil.assertSucceeded(response);
    }

    /**
     * Click Cancel on each page. Check that user was navigated to Home page.
     * @throws Exception
     */
    @Test
    public void testWizardCancel() throws Exception {
        // Step 1 - Check cancel on the first page - General Info Page
        feedWizardPage.clickCancel();
        searchPage.checkPage();

        // Step 2 - Check cancel on the second page - Properties Info Page
        feedWizardPage = searchPage.getPageHeader().doCreateFeed();
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.clickCancel();
        searchPage.checkPage();

        // Step 3 - Check cancel on the third page - Location Info Page
        feedWizardPage = searchPage.getPageHeader().doCreateFeed();
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.clickCancel();
        searchPage.checkPage();

        // Step 4 - Check cancel on the fourth page - Cluster Info Page
        feedWizardPage = searchPage.getPageHeader().doCreateFeed();
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.clickCancel();
        searchPage.checkPage();

        // Step 5 - Check cancel on the fifth page - Summary Page
        feedWizardPage = searchPage.getPageHeader().doCreateFeed();
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.setFeedClustersInfo(feed);
        feedWizardPage.clickNext();
        feedWizardPage.clickCancel();
        searchPage.checkPage();

    }

    /**
     * Check that XML Preview reflects changes correctly.
     * @throws Exception
     */
    @Test
    public void testWizardXmlPreview() throws Exception{

        feed.setTags(getRandomTags());
        feed.setGroups("groups");
        feed.setAvailabilityFlag("_SUCCESS");

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the General Info Page
        feed.assertGeneralProperties(feedFromXML);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the Properties Info Page
        feed.assertPropertiesInfo(feedFromXML);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the Location Info Page
        feed.assertLocationInfo(feedFromXML);


        // Set values on the Cluster Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedClustersInfo(feed);
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();


        // Assert all the values entered on the Cluster Info Page
        feed.assertClusterInfo(feedFromXML);

    }

    /**
     * Add few properties to the feed (tag, group).
     * Click edit XML. Remove both properties from XML.
     * Check that properties were removed from matching fields.
     * Now click Edit XML again. Add new tag, group to the XML.
     * Check that changes have been reflected on wizard page.
     * @throws Exception
     */
    @Test
    public void testGeneralStepEditXml() throws Exception{

        feed.setTags("first=yes,second=yes");
        feed.setGroups("groups");

        // Set tag and group on the Wizard
        feedWizardPage.setFeedTags(feed.getTags());
        feedWizardPage.setFeedGroups(feed.getGroups());

        // Get XML, and set tag and group back to null
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();
        feedFromXML.setTags(null);
        feedFromXML.setGroups(null);

        // Now click EditXML and set the updated XML here
        feedWizardPage.clickEditXml();
        String xmlToString = feedFromXML.toString();
        feedWizardPage.setFeedXml(xmlToString);
        feedWizardPage.clickEditXml();

        // Assert that there is only one Tag on the Wizard window
        feedWizardPage.isTagsDisplayed(0, true);
        feedWizardPage.isTagsDisplayed(1, false);

        // Assert that the Tag value is empty on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedTagKeyText(0), "",
            "Tag Key Should be empty on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedTagValueText(0), "",
            "Tag Value Should be empty on the Wizard window");

        // Assert that the Group value is empty on the Wizard window now
        Assert.assertEquals(feedWizardPage.getFeedGroupsText(), "",
            "Group Should be empty on the Wizard window");

        // Set Tag and Group values
        feedFromXML.setTags("third=yes,fourth=no");
        feedFromXML.setGroups("groups_new");

        // Now click EditXML and set the updated XML here
        feedWizardPage.clickEditXml();
        xmlToString = feedFromXML.toString();
        feedWizardPage.setFeedXml(xmlToString);
        feedWizardPage.clickEditXml();

        // Assert that there are two Tags on the Wizard window
        feedWizardPage.isTagsDisplayed(0, true);
        feedWizardPage.isTagsDisplayed(1, true);

        // Assert that the Tag values are correct on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedTagKeyText(0), "third",
            "Unexpected Tag1 Key on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedTagValueText(0), "yes",
            "Unexpected Tag1 Value on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedTagKeyText(1), "fourth",
            "Unexpected Tag2 Key on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedTagValueText(1), "no",
            "Unexpected Tag2 Value on the Wizard window");

        // Assert that the Group value is correct on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedGroupsText(), "groups_new",
            "Unexpected Group on the Wizard window");

    }

    /**
     * Populate fields with valid values (name, description, tag, groups, ACL, schema).
     * Check that user can go to the next step (click next)"
     * @throws Exception
     */
    @Test
    public void testGeneralStepDefaultScenario() throws Exception{

        feed.setTags(getRandomTags());
        feed.setGroups("groups");

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();

        // Assert on the click of next, the Page moves to the next page
        feedWizardPage.isFeedFrequencyDisplayed(true);

    }

    /**
     * Populate fields with valid values (name, description, tag, groups, ACL, schema)\
     * Check that they are reflected on XML preview.
     * @throws Exception
     */
    @Test
    public void testGeneralStepXmlPreview() throws Exception{

        feed.setTags(getRandomTags());
        feed.setGroups("groups");

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the General Info Page
        feed.assertGeneralProperties(feedFromXML);

    }

    /**
     * Add two tags to the feed. Check that they are present.
     * Check XML preview has it.
     * Delete one tag. Check that it has been removed from wizard window as well as from XML preview.
     * @throws Exception
     */
    @Test
    public void testGeneralStepAddRemoveTag() throws Exception{

        // Set Tags in feed
        feed.setTags("first=yes,second=yes");

        // Set Tag on the General Info Page
        feedWizardPage.setFeedTags(feed.getTags());

        // Assert two tags are present on the Wizard window
        feedWizardPage.isTagsDisplayed(0, true);
        feedWizardPage.isTagsDisplayed(1, true);

        // Get feed from XML Preview
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert Tag values in the XML Preview
        Assert.assertEquals(feedFromXML.getTags(), feed.getTags());

        // Delete the Tag
        feedWizardPage.deleteTagOrProperty();

        // Assert that there is only one Tag on the Wizard window
        feedWizardPage.isTagsDisplayed(0, true);
        feedWizardPage.isTagsDisplayed(1, false);

        // Get feed from XML Preview
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();
        // Assert that there are is only one Tag in the XML Preview
        Assert.assertEquals(feedFromXML.getTags(), "first=yes",
            "Unexpected Tags on the XML preview");

    }


    /**
     * Make sure that optional fields are actually optional i.e. keep blank for the
     * optional fields for this test user should be able to go to next step.
     * @throws Exception
     */
    @Test
    public void testGeneralStepBlankOptionalFields() throws Exception{

        // Only setting the required fields
        feedWizardPage.setFeedName(feed.getName());
        feedWizardPage.setFeedACLOwner(feed.getACL().getOwner());
        feedWizardPage.setFeedACLGroup(feed.getACL().getGroup());
        feedWizardPage.setFeedACLPermissions(feed.getACL().getPermission());
        feedWizardPage.setFeedSchemaLocation(feed.getSchema().getLocation());
        feedWizardPage.setFeedSchemaProvider(feed.getSchema().getProvider());
        feedWizardPage.clickNext();

        // Assert that user is able to go to next page using only required fields
        feedWizardPage.isFeedFrequencyDisplayed(true);
    }

    /**
     * Populate all fields with valid values (frequency, parallel...).
     * Check that user can go to the next step.
     * @throws Exception
     */
    @Test
    public void testTimingStepDefaultScenario() throws Exception{

        feed.setAvailabilityFlag("_SUCCESS");

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedFrequencyQuantity(feed.getFrequency().getFrequency());
        feedWizardPage.setFeedFrequencyUnit(feed.getFrequency().getTimeUnit().toString());
        feedWizardPage.setFeedLateArrivalCheckBox();
        feedWizardPage.setFeedLateArrivalCutOffQuantity(
            feed.getLateArrival().getCutOff().getFrequencyAsInt());
        feedWizardPage.setFeedLateArrivalCutOffUnit(
            feed.getLateArrival().getCutOff().getTimeUnit().toString());
        feedWizardPage.setFeedAvailabilityFlag(feed.getAvailabilityFlag());
        feedWizardPage.setFeedTimeZone();
        feedWizardPage.setQueueName("Default");
        feedWizardPage.setJobPriority("High");
        feedWizardPage.setTimeoutQuantity("30");
        feedWizardPage.setTimeoutUnit("minutes");
        feedWizardPage.setParallel("4");
        feedWizardPage.setMaxMaps("7");
        feedWizardPage.setMapBandwidthKB("2048");
        feedWizardPage.setFeedPropertyKey(0, feed.getProperties().getProperties().get(0).getName());
        feedWizardPage.setFeedPropertyValue(0,
            feed.getProperties().getProperties().get(0).getValue());
        feedWizardPage.addProperty();
        feedWizardPage.setFeedPropertyKey(1, feed.getProperties().getProperties().get(1).getName());
        feedWizardPage.setFeedPropertyValue(1,
            feed.getProperties().getProperties().get(1).getValue());

        feedWizardPage.clickNext();

        // Assert user is able to go on the next page
        feedWizardPage.isFeedDataPathDisplayed(true);

    }

    /**
     * Populate fields with valid values (frequency, late arrival, availability flag, so on)
     * Check that they are reflected on XML preview.
     * @throws Exception
     */
    @Test
    public void testTimingStepXmlPreview() throws Exception{

        feed.setAvailabilityFlag("_SUCCESS");

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);
        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the Properties Info Page
        feed.assertPropertiesInfo(feedFromXML);

    }

    /**
     * Add some properties to the feed (frequency, late arrival).
     * Click edit XML. Remove both properties from XML.
     * Check that properties were removed from matching fields.
     * Now click Edit XML again. Add new properties to the XML.
     * Check that changes have been reflected on wizard page.
     * @throws Exception
     */
    @Test
    public void testTimingStepEditXml() throws Exception{

        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();

        // Set Frequency and Late Arrival on the Wizard
        feedWizardPage.setFeedFrequencyQuantity(feed.getFrequency().getFrequency());
        feedWizardPage.setFeedFrequencyUnit(feed.getFrequency().getTimeUnit().toString());
        feedWizardPage.setFeedLateArrivalCheckBox();
        feedWizardPage.setFeedLateArrivalCutOffQuantity(feed.getLateArrival().getCutOff().getFrequencyAsInt());
        feedWizardPage.setFeedLateArrivalCutOffUnit(feed.getLateArrival().getCutOff().getTimeUnit().toString());

        // Get XML, and set Frequency and Late Arrival back to null
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();
        feedFromXML.setFrequency(null);
        feedFromXML.setLateArrival(null);

        // Now click EditXML and set the updated XML here
        feedWizardPage.clickEditXml();
        String xmlToString = feedFromXML.toString();
        feedWizardPage.setFeedXml(xmlToString);
        feedWizardPage.clickEditXml();

        // Assert that the Frequency value is empty on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedFrequencyQuantityText(), "",
            "Frequency Quantity Should be empty on the Wizard window");

        // Assert that the Late Arrival value is empty on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedLateArrivalCutOffQuantityText(), "",
            "CutOff Quantity Should be empty on the Wizard window");

        // Set Frequency and Late Arrival values
        feedFromXML.setFrequency(new Frequency("5", Frequency.TimeUnit.minutes));
        feedFromXML.setLateArrival(new LateArrival());
        feedFromXML.getLateArrival().setCutOff(new Frequency("1", Frequency.TimeUnit.days));

        // Now click EditXML and set the updated XML here
        feedWizardPage.clickEditXml();
        xmlToString = feedFromXML.toString();
        feedWizardPage.setFeedXml(xmlToString);
        feedWizardPage.clickEditXml();

        // Assert that the Frequency values are correct on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedFrequencyQuantityText(), "5",
            "Unexpected Frequency on the XML preview");

        // Assert that the Late Arrival value is correct on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedLateArrivalCutOffQuantityText(), "1",
            "Unexpected CutOff on the XML preview");

    }

    /**
     * Check that frequency, late arrival and timeout drop downs contain correct items (timeunits).
     * Check that jobPriority dropDown contains valid priorities.
     * @throws Exception
     */
    @Test
    public void testTimingStepDropDownLists() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();

        List<String> dropdownValues = feedWizardPage.getFeedFrequencyUnitValues();
        Assert.assertEquals(timeUnits, dropdownValues, "Frequency Unit Values Are Not Equal");

        dropdownValues = feedWizardPage.getFeedLateArrivalCutOffUnitValues();
        Assert.assertEquals(timeUnits, dropdownValues, "Late Arrival Unit Values Are Not Equal");

        dropdownValues = feedWizardPage.getJobPriorityValues();
        Assert.assertEquals(jobPriorities, dropdownValues, "Job Priority Unit Values Are Not Equal");

        dropdownValues = feedWizardPage.getTimeoutUnitValues();
        ArrayList<String> timeUnitsWithSelect = new ArrayList<String>(timeUnits);
        timeUnitsWithSelect.add(0, "-Select timeout-");
        Assert.assertEquals(timeUnitsWithSelect, dropdownValues, "Timeout Unit Values Are Not Equal");
    }


    /**
     * Add two properties to the feed. Check that they are present (as separate element) on the
     * page.
     * Check XML preview has it. Delete one property. Check that it has been removed from wizard
     * window as well as from XML preview.
     * @throws Exception
     */
    @Test
    public void testTimingStepAddDeleteProperties() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);
        feedWizardPage.clickNext();
        // Set first property
        feedWizardPage.setFeedPropertyKey(0, feed.getProperties().getProperties().get(0).getName());
        feedWizardPage.setFeedPropertyValue(0,
            feed.getProperties().getProperties().get(0).getValue());
        // Click add property button
        feedWizardPage.addProperty();
        // Set second property
        feedWizardPage.setFeedPropertyKey(1, feed.getProperties().getProperties().get(1).getName());
        feedWizardPage.setFeedPropertyValue(1,
            feed.getProperties().getProperties().get(1).getValue());

        // Assert two Properties are present on the Wizard window
        feedWizardPage.isPropertyDisplayed(0, true);
        feedWizardPage.isPropertyDisplayed(1, true);

        // Get feed from XML Preview
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert Property values in the XML Preview
        Assert.assertEquals(feedFromXML.getProperties().getProperties().get(0).getName(),
            feed.getProperties().getProperties().get(0).getName(),
            "Unexpected Property1 Name on the XML preview");
        Assert.assertEquals(feedFromXML.getProperties().getProperties().get(0).getValue(),
            feed.getProperties().getProperties().get(0).getValue(),
            "Unexpected Property1 Value on the XML preview");
        Assert.assertEquals(feedFromXML.getProperties().getProperties().get(1).getName(),
            feed.getProperties().getProperties().get(1).getName(),
            "Unexpected Property2 Name on the XML preview");
        Assert.assertEquals(feedFromXML.getProperties().getProperties().get(1).getValue(),
            feed.getProperties().getProperties().get(1).getValue(),
            "Unexpected Property2 Value on the XML preview");


        // Delete one Property
        feedWizardPage.deleteTagOrProperty();

        // Assert only one Property is present on the Wizard window
        feedWizardPage.isPropertyDisplayed(0, true);
        feedWizardPage.isPropertyDisplayed(1, false);


        // Get feed from XML Preview
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert Property value in the XML Preview
        Assert.assertEquals(feedFromXML.getProperties().getProperties().get(0).getName(),
            feed.getProperties().getProperties().get(0).getName(),
            "Unexpected Property1 Name on the XML preview");
        Assert.assertEquals(feedFromXML.getProperties().getProperties().get(0).getValue(),
            feed.getProperties().getProperties().get(0).getValue(),
            "Unexpected Property1 Value on the XML preview");
        try{
            feedFromXML.getProperties().getProperties().get(1);
            Assert.fail("Second Property found in the XML Preview");
        } catch (Exception ex){
            LOGGER.info("Second Property not found in the XML Preview");
        }

    }


    /**
     * Populate locations with valid values.
     * Check that user can go to the next step.
     * @throws Exception
     */
    @Test
    public void testLocationStepValidValuesFS() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        feedWizardPage.clickNext();
        // Assert user is able to go on the next page
        feedWizardPage.isFeedClusterRetentionDisplayed(true);

    }

    /**
     * Select Catalog Storage and populate it with valid value.
     * Check that user is allowed to go to the next step.
     * @throws Exception
     */
    @Test
    public void testLocationStepValidCatalogStorage() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Select Catalog Storage and Set Table Uri on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.clickCatalogStorageButton();
        feedWizardPage.setFeedCatalogTableUri(catalogUri);

        feedWizardPage.clickNext();

        // Assert user is able to go to the next Page
        feedWizardPage.isFeedClusterRetentionDisplayed(true);
    }

    /**
     * Populate locations fields with values as well as Catalog table uri.
     * Check that user is not allowed to go to the next step and is notified with an appropriate alert.
     * @throws Exception
     */
    @Test
    public void testLocationStepBothLocationsAndTableUri() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        // Select Catalog Storage and Set Table Uri on the Location Info Page
        feedWizardPage.clickCatalogStorageButton();
        feedWizardPage.setFeedCatalogTableUri(catalogUri);

        feedWizardPage.clickNext();

        // Assert user should not be able to go to the next Page
        feedWizardPage.isFeedClusterRetentionDisplayed(false);

    }

    /**
     * Populate locations with valid values and check that they are reflected on XML preview.
     * Click edit XML. Change locations. Check that changes are reflected on XML as well as on wizard page.
     * @throws Exception
     */
    @Test
    public void testLocationStepEditXml() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        // Get feed from XML Preview
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the Location Info Page
        feed.assertLocationInfo(feedFromXML);
        // Set new Location Paths
        feedFromXML.getLocations().getLocations().get(0).setPath(
            baseTestHDFSDir + "/newInput/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        feedFromXML.getLocations().getLocations().get(1).setPath(baseTestHDFSDir + "/newFalcon/clicksStats");
        feedFromXML.getLocations().getLocations().get(2).setPath(baseTestHDFSDir + "/newFalcon/clicksMetaData");

        // Now click EditXML and set the updated XML here
        feedWizardPage.clickEditXml();
        String xmlToString = feedFromXML.toString();
        feedWizardPage.setFeedXml(xmlToString);
        feedWizardPage.clickEditXml();

        // Get feed from XML Preview
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();
        // Assert all the values on the Location Info Page
        Assert.assertEquals(feedFromXML.getLocations().getLocations().get(0).getPath(),
            baseTestHDFSDir + "/newInput/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        Assert.assertEquals(feedFromXML.getLocations().getLocations().get(1).getPath(),
            baseTestHDFSDir + "/newFalcon/clicksStats");
        Assert.assertEquals(feedFromXML.getLocations().getLocations().get(2).getPath(),
            baseTestHDFSDir + "/newFalcon/clicksMetaData");

        // Assert that the Location Path values are correct on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedPathText(0),
            baseTestHDFSDir + "/newInput/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
            "Unexpected Cluster Data Location on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedPathText(1),
            baseTestHDFSDir + "/newFalcon/clicksStats",
            "Unexpected Cluster Stats Location on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedPathText(2),
            baseTestHDFSDir +"/newFalcon/clicksMetaData",
            "Unexpected Cluster Meta Location on the Wizard window");


    }

    /**
     * Populate each field with correct values (cluster, validity ...).
     * Check that user can go to the next step.
     * @throws Exception
     */
    @Test
    public void testClustersStepDefaultScenario() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        // Set values on the Cluster Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedClustersInfo(feed);

        feedWizardPage.clickNext();

        // Assert user is able to go to the next Page
        feedWizardPage.isSaveFeedButtonDisplayed(true);
    }

    /**
     * Submit few clusters. Check that cluster list contains all clusters.
     * Check that retention drop down lists contains valid time units.
     * @throws Exception
     */
    @Test
    public void testClustersStepDropDownLists() throws Exception{

        List<String> allClusters = new ArrayList<>();
        // Add four more clusters
        for (int i=0; i< 4; i++){
            ClusterMerlin newCluster = new ClusterMerlin(clusterFromBundle.toString());
            newCluster.setName(clusterFromBundle.getName() + "-" + i);
            AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(newCluster.toString()));
            allClusters.add(newCluster.getName());
        }
        // Also add base cluster and -Select cluster- to allCluster array
        allClusters.add(feed.getClusters().getClusters().get(0).getName());
        allClusters.add("-Select cluster-");
        Collections.sort(allClusters);

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        feedWizardPage.clickNext();
        // Assert cluster source drop down contains all the clusters
        List<String> dropdownValues = feedWizardPage.getFeedClusterSourceValues();
        Collections.sort(dropdownValues);
        Assert.assertEquals(allClusters, dropdownValues,
            "Cluster Source Values Are Not Equal");

        // Assert retention drop down time units
        dropdownValues = feedWizardPage.getFeedClusterRetentionUnitValues();
        Assert.assertEquals(timeUnits, dropdownValues, "Retention Unit Values Are Not Equal");

    }

    /**
     * Populate FS locations on previous step. Don't populate any values on the current step.
     * Go to the next one. Check that XML preview has locations provided on Location step.
     * Repeat the same for Catalog URI.
     * @throws Exception
     */
    @Test
    public void testClustersStepDefaultLocations() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        feedWizardPage.clickNext();
        // Get feed from XML Preview
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values entered on the Location Info Page
        feed.assertLocationInfo(feedFromXML);

        feedWizardPage.clickPrevious();
        // Clear all the Location Info
        feedWizardPage.setFeedPath(0, "");
        feedWizardPage.setFeedPath(1, "");
        feedWizardPage.setFeedPath(2, "");

        // Select Catalog Storage and Set Table Uri on the Location Info Page
        feedWizardPage.clickCatalogStorageButton();
        feedWizardPage.setFeedCatalogTableUri(catalogUri);

        feedWizardPage.clickNext();

        // Get feed from XML Preview
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert the Table Uri value entered on the Location Info Page
        Assert.assertEquals(feedFromXML.getTable().getUri(), catalogUri,
            "Unexpected Cluster URI on the XML preview");

    }

    /**
     * Populate all fields with valid values and check that they are reflected on XML preview.
     * Click edit XML. Change retention and locations.
     * Check that changes are reflected on XML as well as on wizard page.
     * @throws Exception
     */
    @Test
    public void testClusterStepEditXml() throws Exception{

        // Set values on the General Info Page
        feedWizardPage.setFeedGeneralInfo(feed);

        // Set values on the Properties Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedPropertiesInfo(feed);

        // Set values on the Location Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedLocationInfo(feed);

        // Set values on the Cluster Info Page
        feedWizardPage.clickNext();
        feedWizardPage.setFeedClustersInfo(feed);

        // Get feed from XML Preview
        FeedMerlin feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();

        // Assert all the values on the Cluster Info Page
        feed.assertClusterInfo(feedFromXML);


        // Set new Location and Retention
        feedFromXML.getClusters().getClusters().get(0).getLocations().getLocations().get(0).setPath(
            baseTestHDFSDir + "/newInput/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        feedFromXML.getClusters().getClusters().get(0).getLocations().getLocations().get(1).setPath(
            baseTestHDFSDir + "/newFalcon/clicksStats");
        feedFromXML.getClusters().getClusters().get(0).getLocations().getLocations().get(2).setPath(
            baseTestHDFSDir + "/newFalcon/clicksMetaData");
        feedFromXML.getClusters().getClusters().get(0).getRetention().
            setLimit(new Frequency("60", Frequency.TimeUnit.minutes));


        // Now click EditXML and set the updated XML here
        feedWizardPage.clickEditXml();
        String xmlToString = feedFromXML.toString();
        feedWizardPage.setFeedXml(xmlToString);
        feedWizardPage.clickEditXml();

        // Get feed from XML Preview
        feedFromXML = feedWizardPage.getFeedMerlinFromFeedXml();
        // Assert all the values on the Location Info Page
        Assert.assertEquals(feedFromXML.getClusters().getClusters().get(0)
                .getLocations().getLocations().get(0).getPath(),
            baseTestHDFSDir + "/newInput/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
            "Unexpected Cluster Data Path on the XML preview");
        Assert.assertEquals(feedFromXML.getClusters().getClusters().get(0)
                .getLocations().getLocations().get(1).getPath(),
            baseTestHDFSDir + "/newFalcon/clicksStats",
            "Unexpected Cluster Stats Path on the XML preview");
        Assert.assertEquals(feedFromXML.getClusters().getClusters().get(0)
                .getLocations().getLocations().get(2).getPath(),
            baseTestHDFSDir + "/newFalcon/clicksMetaData",
            "Unexpected Cluster Meta Path on the XML preview");
        Assert.assertEquals(feedFromXML.getClusters().getClusters().get(0)
            .getRetention().getLimit().getFrequency(), "60",
            "Unexpected Retention on the XML preview");
        Assert.assertEquals(feedFromXML.getClusters().getClusters().get(0)
                .getRetention().getLimit().getTimeUnit().name(), "minutes",
            "Unexpected Retention Unit on the XML preview");


        // Assert that the Location and Retention values are correct on the Wizard window
        Assert.assertEquals(feedWizardPage.getFeedPathText(0),
            baseTestHDFSDir + "/newInput/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}",
            "Unexpected Cluster Data Location on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedPathText(1),
            baseTestHDFSDir + "/newFalcon/clicksStats",
            "Unexpected Cluster Stats Location on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedPathText(2),
            baseTestHDFSDir + "/newFalcon/clicksMetaData",
            "Unexpected Cluster Meta Location on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedClusterRetentionLimitText(), "60",
            "Unexpected Retention Limit on the Wizard window");
        Assert.assertEquals(feedWizardPage.getFeedClusterRetentionUnitText(), "minutes",
            "Unexpected Retention Unit on the Wizard window");


    }

    /**
     * Create feed. Using API check that feed was created.
     * @throws Exception
     */
    @Test
    public void testSummaryStepDefaultScenario() throws Exception{

        feedWizardPage.setFeed(feed);
        //Check the response using API to validate if the feed creation went successfully
        ServiceResponse response = prism.getFeedHelper().getEntityDefinition(feed.toString());
        AssertUtil.assertSucceeded(response);

    }


}

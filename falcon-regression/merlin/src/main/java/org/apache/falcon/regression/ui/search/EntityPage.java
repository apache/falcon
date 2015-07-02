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

package org.apache.falcon.regression.ui.search;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.falcon.resource.InstancesResult;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.Select;
import org.testng.asserts.SoftAssert;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Class representation of Search UI entity page.
 */
public class EntityPage extends AbstractSearchPage {
    private static final Logger LOGGER = Logger.getLogger(EntityPage.class);

    /**
     * Possible instance actions available on entity page.
     */
    public enum InstanceAction {
        Log,
        Resume,
        Rerun,
        Suspend,
        Kill
    }

    public EntityPage(WebDriver driver) {
        super(driver);
    }

    private WebElement getEntityTitle() {
        final WebElement title = driver.findElement(By.id("entity-title"));
        UIAssert.assertDisplayed(title, "entity title");
        return title;
    }

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "dependencies-graph")
    })
    private WebElement dependencyBox;


    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(xpath = "(.//*[contains(@class, 'detailsBox')])[2]")
    })
    private WebElement instanceListBox;

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "summaryBox")
    })
    private WebElement propertiesBlock;

    public String getEntityName() {
        UIAssert.assertDisplayed(getEntityTitle(), "Entity title");
        return getEntityTitle().getText().split(" ")[0];
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(dependencyBox, "Dependency box");
        UIAssert.assertDisplayed(instanceListBox, "Instance list box");
        UIAssert.assertDisplayed(propertiesBlock, "Summary box");
    }

    public EntityPage refreshPage() {
        final String entityName = getEntityName();
        SearchPage searchPage = getPageHeader().gotoHome();
        return searchPage.openEntityPage(entityName);
    }

    public void checkFeedProperties(FeedMerlin feed) {
        openProperties();

        final WebElement propertiesBox =
            propertiesBlock.findElement(By.xpath("//div[@ui-view='feedSummary']"));
        UIAssert.assertDisplayed(propertiesBox, "Properties box");

        //all the parts of the entity properties
        final List<WebElement> propertyParts = propertiesBox.findElements(By.xpath("./div"));
        //First set of properties
        final WebElement generalBox = propertyParts.get(0);
        final List<WebElement> generalParts = generalBox.findElements(By.xpath("./div"));
        SoftAssert softAssert = new SoftAssert();
        //General
        softAssert.assertEquals(generalParts.get(0).getText(), "General", "Unexpected heading");
        final List<WebElement> nameAndDesc = generalParts.get(1).findElements(By.xpath("./div"));
        softAssert.assertEquals(nameAndDesc.get(0).getText(), "Name: " + feed.getName(),
            "Unexpected feed name in properties.");
        softAssert.assertEquals(nameAndDesc.get(1).getText(), "Description: " + feed.getDescription(),
            "Unexpected description in properties.");
        //Tags
        softAssert.assertEquals(generalParts.get(2).getText(), "Tags", "Unexpected heading");
        softAssert.assertEquals(generalParts.get(3).getText(),
            StringUtils.trimToEmpty(feed.getTags()),
            "Unexpected tags");
        //Groups
        softAssert.assertEquals(generalParts.get(4).getText(), "Groups", "Unexpected heading");
        softAssert.assertEquals(generalParts.get(5).getText(),
            StringUtils.trimToEmpty(feed.getGroups()),
            "Unexpected groups");
        //Access Control list
        softAssert.assertEquals(generalParts.get(6).getText(), "Access Control List",
            "Unexpected heading");
        final List<WebElement> ownerGrpPerm = generalParts.get(7).findElements(By.xpath("./div"));
        softAssert.assertEquals(ownerGrpPerm.get(0).getText(),
            "Owner: " + feed.getACL().getOwner(), "Unexpected owner");
        softAssert.assertEquals(ownerGrpPerm.get(1).getText(),
            "Group: " + feed.getACL().getGroup(), "Unexpected group");
        softAssert.assertEquals(ownerGrpPerm.get(2).getText(),
            "Permissions: " + feed.getACL().getPermission(), "Unexpected permission");
        //Schema
        softAssert.assertEquals(generalParts.get(8).getText(), "Schema",
            "Unexpected heading for general properties");
        final List<WebElement> locAndProvider = generalParts.get(9).findElements(By.xpath("./div"));
        softAssert.assertEquals(locAndProvider.get(0).getText(),
            "Location: " + feed.getSchema().getLocation(), "Unexpected schema locations");
        softAssert.assertEquals(locAndProvider.get(1).getText(),
            "Provider: " + feed.getSchema().getProvider(), "Unexpected schema provider");
        //Properties
        softAssert.assertEquals(generalParts.get(10).getText(), "Properties",
            "Unexpected heading for general properties");
        final List<WebElement> freqLateAvail = generalParts.get(11).findElements(By.xpath("./div"));
        final Frequency feedFrequency = feed.getFrequency();
        softAssert.assertEquals(freqLateAvail.get(0).getText(),
            String.format("Frequency: Every %s %s",
                feedFrequency.getFrequency(), feedFrequency.getTimeUnit()),
            "Unexpected frequency");
        final Frequency feedLateCutoff = feed.getLateArrival().getCutOff();
        softAssert.assertEquals(freqLateAvail.get(1).getText(),
            String.format("Late Arrival: Up to %s %s",
                feedLateCutoff.getFrequency(), feedLateCutoff.getTimeUnit()),
            "Unexpected late arrival");
        softAssert.assertEquals(freqLateAvail.get(2).getText(),
            String.format("Availability Flag:%s",
                StringUtils.trimToEmpty(feed.getAvailabilityFlag())),
            "Unexpected availability flag");
        final List<WebElement> propertyElements =
            generalParts.get(12).findElements(By.xpath("./div"));
        List<String> displayedPropStr = new ArrayList<>();
        for (WebElement webElement : propertyElements) {
            displayedPropStr.add(webElement.getText());
        }
        Collections.sort(displayedPropStr);
        final List<String> expectedPropStr = getFeedPropString(feed);
        softAssert.assertEquals(displayedPropStr, expectedPropStr,
            "Feed properties & displayed properties don't match. Expected: " + expectedPropStr
                + " Actual: " + displayedPropStr);
        //Storage type
        softAssert.assertEquals(generalParts.get(13).getText(), "Default Storage Type:",
            "Unexpected label for storage type.");
        if (feed.getLocations() != null
            && feed.getLocations().getLocations() != null
            && feed.getLocations().getLocations().size() > 0) {
            softAssert.assertEquals(generalParts.get(13).getText(), "File System",
                "Unexpected storage type for feed.");
        } else {
            softAssert.fail("Need to add handler for other feed types.");
        }
        //Feed locations - Data followed by Stats followed by Meta
        softAssert.assertEquals(generalParts.get(14).getText(), "Default Location:",
            "Unexpected label for default location.");
        softAssert.assertEquals(generalParts.get(15).getText(),
            "Data\n" + feed.getFeedPath(LocationType.DATA),
            "Unexpected label for feed data label");
        softAssert.assertEquals(generalParts.get(16).getText(),
            "Stats\n" + feed.getFeedPath(LocationType.STATS),
            "Unexpected label for feed stats label");
        softAssert.assertEquals(generalParts.get(17).getText(),
            "Meta\n" + feed.getFeedPath(LocationType.META),
            "Unexpected label for feed mata label");

        //Second set of properties details with Source Cluster Properties
        final WebElement clustersBox = propertyParts.get(1);
        final List<WebElement> displayedClusters = clustersBox.findElements(By.xpath("./div"));
        final List<Cluster> feedClusters = feed.getClusters().getClusters();
        //test needs to be fixed when we have support for more than one feed cluster
        softAssert.assertEquals(feedClusters.size(), 1,
            "Current UI has support for only one feed cluster.");
        checkFeedCluster(displayedClusters.get(0), feedClusters.get(0), softAssert);
        softAssert.assertAll();
    }

    private void openProperties() {
        final WebElement heading = propertiesBlock.findElement(By.tagName("h4"));
        assertEquals(heading.getText(), "Properties",
            "Unexpected heading of properties box.");
        final WebElement upButton = propertiesBlock.findElement(By.className("pointer"));
        upButton.click();
    }

    private void checkFeedCluster(WebElement cluster, Cluster feedCluster, SoftAssert softAssert) {
        final List<WebElement> clusterElements = cluster.findElements(By.xpath("./div"));
        final String vClusterName = clusterElements.get(1).getText();
        softAssert.assertNotNull(feedCluster,
            "Unexpected feed cluster is displayed: " + vClusterName);
        final String clusterType = clusterElements.get(0).getText();
        softAssert.assertEquals(clusterType,
            WordUtils.capitalize(feedCluster.getType().toString().toLowerCase() + " Cluster"),
            "Unexpected cluster type for cluster: " + vClusterName);
        softAssert.assertEquals(clusterElements.get(2).getText(),
            "Start: " + feedCluster.getValidity().getStart()
                + "\nEnd: " + feedCluster.getValidity().getEnd(),
            "Unexpected validity of the cluster: " + vClusterName);
        softAssert.assertEquals(clusterElements.get(3).getText(), "Timezone: UTC",
            "Unexpected timezone for validity of the cluster: " + vClusterName);
        softAssert.assertEquals(clusterElements.get(4).getText(),
            "Retention: Archive in " + feedCluster.getRetention().getLimit().getFrequency()
                + " " + feedCluster.getRetention().getLimit().getTimeUnit(),
            "Unexpected retention associated with cluster: " + vClusterName);
    }

    private List<String> getFeedPropString(FeedMerlin feed) {
        List<String> retVals = new ArrayList<>();
        for (Property property : feed.getProperties().getProperties()) {
            retVals.add(property.getName() + ": " + property.getValue());
        }
        Collections.sort(retVals);
        return retVals;
    }

    public void checkProcessProperties(ProcessMerlin process) {
        openProperties();

        final WebElement propertiesBox =
            propertiesBlock.findElement(By.xpath("//div[@ui-view='processSummary']"));
        UIAssert.assertDisplayed(propertiesBox, "Properties box");
        final List<WebElement> propertiesParts = propertiesBox.findElements(By.xpath("./div"));
        final WebElement generalPropBlock = propertiesParts.get(0);
        final WebElement clusterPropBlock = propertiesParts.get(1);
        final WebElement inputPropBlock = propertiesParts.get(2);
        final WebElement outputPropBlock = propertiesParts.get(3);

        //checking general properties
        final List<WebElement> generalPropParts =
            generalPropBlock.findElement(By.xpath("./*")).findElements(By.xpath("./*"));
        SoftAssert softAssert = new SoftAssert();
        softAssert.assertEquals(generalPropParts.get(0).getText(), "Process",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(1).getText(), "Name",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(2).getText(), process.getName(),
            "Unexpected process name in general properties.");
        softAssert.assertEquals(generalPropParts.get(3).getText(), "Tags",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(4).getText(),
            StringUtils.defaultIfBlank(process.getTags(), "No tags selected"),
            "Unexpected tags in general properties.");
        softAssert.assertEquals(generalPropParts.get(5).getText(), "Workflow",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(6).getText(), "Name\nEngine\nVersion",
            "Unexpected workflow properties in general properties.");
        softAssert.assertEquals(generalPropParts.get(7).getText(),
            String.format("%s%n%s%n%s",
                StringUtils.defaultIfBlank(process.getWorkflow().getName(), ""),
                process.getWorkflow().getEngine(), process.getWorkflow().getVersion()),
            "Unexpected workflow properties in general properties.");
        softAssert.assertEquals(generalPropParts.get(7).getText(), "Path",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(8).getText(), process.getWorkflow().getPath(),
            "Unexpected workflow path in general properties.");
        softAssert.assertEquals(generalPropParts.get(9).getText(), "Timing",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(10).getText(), "Timezone",
            "Unexpected label in general properties.");
        softAssert.assertEquals(generalPropParts.get(12).getText(),
            String.format("Frequency%nEvery %s %s%n", process.getFrequency().getFrequency(),
                process.getFrequency().getTimeUnit())
                + "Max. parallel instances\n" + process.getParallel()
                + "\nOrder\n" + process.getOrder().toString(),
            "Unexpected frequency/parallel/order info in general properties.");
        softAssert.assertEquals(generalPropParts.get(13).getText(), "Retry",
            "Unexpected label in general properties.");
        final Retry processRetry = process.getRetry();
        softAssert.assertEquals(generalPropParts.get(14).getText(),
            "Policy\n" + processRetry.getPolicy().toString().toLowerCase()
                + "\nAttempts\n" + processRetry.getAttempts()
                + "\nDelay\nUp to " + processRetry.getDelay().getFrequency()
                + " " + processRetry.getDelay().getTimeUnit(),
            "Unexpected policy/attempt/delay in general properties.");

        //checking cluster properties
        final List<WebElement> allClusterProps =
            clusterPropBlock.findElements(By.xpath("./div/div/div"));
        final WebElement clustersHeading = clusterPropBlock.findElement(By.xpath(".//h5"));
        softAssert.assertEquals(clustersHeading.getText(), "Clusters",
            "Unexpected label in clusters heading");
        for (WebElement oneClusterProp : allClusterProps) {
            final List<WebElement> clusterPropParts = oneClusterProp.findElements(By.xpath("./*"));
            softAssert.assertEquals(clusterPropParts.get(0).getText(), "Name",
                "Unexpected label in clusters properties");
            final String clusterName = clusterPropParts.get(1).getText();
            final org.apache.falcon.entity.v0.process.Cluster processCluster =
                process.getClusterByName(clusterName);
            softAssert.assertNotNull(processCluster,
                "cluster with name " + clusterName + " was not present in process.");
            softAssert.assertEquals(clusterName, processCluster.getName(),
                "Unexpected cluster name in clusters properties");
            softAssert.assertEquals(clusterPropParts.get(2).getText(), "Validity",
                "Unexpected label in clusters properties");
            softAssert.assertEquals(clusterPropParts.get(3).getText(),
                "Start\n" + processCluster.getValidity().getStart()
                + "\nEnd\n" + processCluster.getValidity().getEnd(),
                "Unexpected start/end time in clusters properties");
        }
        //checking  inputs properties
        final WebElement inputHeading = inputPropBlock.findElement(By.xpath(".//h5"));
        softAssert.assertEquals(inputHeading.getText(), "Inputs",
            "Unexpected heading for input properties.");
        final List<WebElement> allInputsProps =
            inputPropBlock.findElements(By.xpath("./div/div/*"));
        for (WebElement oneInputProps : allInputsProps) {
            final List<WebElement> inputPropParts = oneInputProps.findElements(By.xpath("./*"));
            softAssert.assertEquals(inputPropParts.get(0).getText(), "Name",
                "Unexpected label in input properties");
            final String inputName = inputPropParts.get(1).getText();
            final Input processInput = process.getInputByName(inputName);
            softAssert.assertEquals(inputName, processInput.getName(),
                "Unexpected input name in input properties");
            softAssert.assertEquals(inputPropParts.get(2).getText(), "Feed",
                "Unexpected label in input properties");
            softAssert.assertEquals(inputPropParts.get(3).getText(), processInput.getFeed(),
                "Unexpected label in input properties");
            softAssert.assertEquals(inputPropParts.get(4).getText(), "Instance",
                "Unexpected label in input properties");
            softAssert.assertEquals(inputPropParts.get(5).getText(),
                "Start\n" + processInput.getStart() + "\nEnd\n" + processInput.getEnd(),
                "Unexpected start/end in input properties");
        }
        final WebElement outputHeading = outputPropBlock.findElement(By.tagName("h5"));
        softAssert.assertEquals(outputHeading.getText(), "Outputs",
            "Unexpected label for output properties.");
        final List<WebElement> allOutputsProps =
            outputPropBlock.findElements(By.xpath("./div/div/*"));
        for (WebElement oneOutputProps : allOutputsProps) {
            final List<WebElement> outputPropParts = oneOutputProps.findElements(By.xpath("./*"));
            softAssert.assertEquals(outputPropParts.get(0).getText(), "Name",
                "Unexpected label in output properties");
            final String outputName = outputPropParts.get(1).getText();
            final Output processOutput = process.getOutputByName(outputName);
            softAssert.assertEquals(outputName, processOutput.getName(),
                "Unexpected output name in output properties");
            softAssert.assertEquals(outputPropParts.get(2).getText(), "Feed",
                "Unexpected label in output properties");
            softAssert.assertEquals(outputPropParts.get(3).getText(), processOutput.getFeed(),
                "Unexpected feed name in output properties");
            softAssert.assertEquals(outputPropParts.get(4).getText(), "Instance",
                "Unexpected label in output properties");
            softAssert.assertEquals(outputPropParts.get(5).getText(), processOutput.getInstance(),
                "Unexpected instance in output properties");
            softAssert.assertAll();
        }
    }
    public void performActionOnSelectedInstances(InstanceAction instanceAction) {
        driver.findElement(By.xpath(String.format("//td/div[%d]", instanceAction.ordinal() + 1))).click();
        waitForAngularToFinish();
    }

    public InstanceSummary getInstanceSummary() {
        return new InstanceSummary(this);
    }

    /**
     * Class representing all the displayed instance.
     */
    public static class InstanceSummary {
        private final WebElement instanceListBox;
        private final WebElement summaryTableHeading;

        public InstanceSummary(EntityPage entityPage) {
            instanceListBox = entityPage.instanceListBox;
            UIAssert.assertDisplayed(instanceListBox, "instance list box");
            assertEquals(instanceListBox.findElement(By.tagName("h4")).getText(),
                "Instances",
                "Unexpected heading in instances box.");

            summaryTableHeading = instanceListBox.findElement(By.xpath(".//thead/tr"));
        }

        private List<WebElement> getTableRows() {
            return instanceListBox.findElements(By.xpath(".//tbody/tr"));
        }

        /**
         * Get instance summary starting for all the pages.
         * @return instance summary
         */
        public List<OneInstanceSummary> getSummary() {
            List<OneInstanceSummary> summary = new ArrayList<>();
            final List<WebElement> tableBody = getTableRows();
            //last line has page number
            final WebElement pageNumberRow = tableBody.remove(tableBody.size() - 1);
            final List<WebElement> pages = pageNumberRow.findElement(By.className("pagination"))
                .findElements(By.className("ng-scope"));
            final int numberOfPages = pages.size();
            for (int pageNumber = 1; pageNumber <= numberOfPages; ++pageNumber) {
                //We want to use new web elements to avoid stale element issues
                final List<WebElement> newTableBody = getTableRows();
                //last line has page number
                final WebElement newPageNumberRow = newTableBody.remove(newTableBody.size() - 1);
                final List<WebElement> newPages =
                    newPageNumberRow.findElement(By.className("pagination"))
                        .findElements(By.className("ng-scope"));
                newPages.get(pageNumber-1).findElement(By.tagName("a")).click();
                summary.addAll(getSummaryInner());
            }
            return summary;
        }

        /**
         * Get instance summary starting for the current page.
         * @return instance summary
         */
        private List<OneInstanceSummary> getSummaryInner() {
            List<OneInstanceSummary> summary = new ArrayList<>();
            final List<WebElement> tableBody = getTableRows();
            //first line in body has buttons
            tableBody.remove(0);
            //last line has page number
            tableBody.remove(tableBody.size() - 1);
            //second last line is horizontal line
            tableBody.remove(tableBody.size() - 1);
            if (tableBody.size() == 1
                && tableBody.get(0).getText().equals("There are no results")) {
                return summary;
            }
            for (WebElement oneSummaryRow : tableBody) {
                summary.add(new OneInstanceSummary(oneSummaryRow));
            }
            return summary;
        }

        public void check() {
            final List<WebElement> summaryHeadParts = getSummaryHeadParts();
            getSelectAllCheckBox(summaryHeadParts);
            final WebElement instanceHeadLabel = summaryHeadParts.get(1);
            assertEquals(instanceHeadLabel.getText(), "Instance",
                "Unexpected label in instance summary heading");
            getSummaryStartedButton();
            getSummaryEndedButton();
            getStatusDropDown();
        }

        public void setInstanceSummaryStartTime(String timeStr) {
            final WebElement startTimeButton = getSummaryStartedButton();
            startTimeButton.clear();
            sendKeysSlowly(startTimeButton, timeStr);
            startTimeButton.sendKeys(Keys.ENTER);
        }

        public void setInstanceSummaryEndTime(String timeStr) {
            final WebElement endTimeButton = getSummaryEndedButton();
            endTimeButton.clear();
            sendKeysSlowly(endTimeButton, timeStr);
            endTimeButton.sendKeys(Keys.ENTER);
        }

        public void selectInstanceSummaryStatus(String labelText) {
            getStatusDropDown().selectByVisibleText(labelText);
        }

        public static OneInstanceSummary getOneSummary(final List<OneInstanceSummary> summaries,
                                                       final String nominalTime) {
            for (OneInstanceSummary oneSummary : summaries) {
                if (oneSummary.getNominalTime().equals(nominalTime)) {
                    return oneSummary;
                }
            }
            return null;
        }

        public void checkSummary(InstancesResult.Instance[] apiSummary) {
            final List<OneInstanceSummary> summary = getSummary();
            assertEquals(apiSummary.length, summary.size(),
                String.format("Length of the displayed instance summary is not same: %s %s",
                    Arrays.toString(apiSummary), summary));
            for (InstancesResult.Instance oneApiSummary : apiSummary) {
                final OneInstanceSummary oneSummary =
                    getOneSummary(summary, oneApiSummary.instance);
                assertEquals(oneApiSummary.instance, oneSummary.getNominalTime(),
                    "Nominal time of instance summary doesn't match.");
                final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                final Date apiStartTime = oneApiSummary.getStartTime();
                if (apiStartTime == null) {
                    assertTrue(StringUtils.isEmpty(oneSummary.getStartTime()),
                        "Displayed start time : " + oneSummary + " is not "
                            + "consistent with start time of api which is null");
                } else {
                    assertEquals(oneSummary.getStartTime(), dateFormat.format(apiStartTime),
                        "Displayed start time : " + oneSummary + " is not "
                            + "consistent with start time of api: " + apiStartTime);
                }
                final Date apiEndTime = oneApiSummary.getEndTime();
                if (apiEndTime == null) {
                    assertTrue(StringUtils.isEmpty(oneSummary.getEndTime()),
                        "Displayed end time : " + oneSummary + " is not "
                            + "consistent end start time of api which is null");
                } else {
                    assertEquals(oneSummary.getEndTime(), dateFormat.format(apiEndTime),
                        "Displayed end time : " + oneSummary + " is not "
                            + "consistent with end time of api: " + apiEndTime);
                }
                assertEquals(oneApiSummary.status.toString(), oneSummary.getStatus(),
                    "Status of instance summary doesn't match.");
            }
        }

        public WebElement getSummaryStartedButton() {
            final WebElement startedBox = getSummaryHeadParts().get(2);
            assertEquals(startedBox.getText(), "Started ",
                "Unexpected label in instance summary heading");
            return startedBox.findElement(By.tagName("input"));
        }

        public WebElement getSummaryEndedButton() {
            final WebElement endedBox = getSummaryHeadParts().get(3);
            assertEquals(endedBox.getText(), "Ended ",
                "Unexpected label in instance summary heading");
            return endedBox.findElement(By.tagName("input"));
        }

        public Select getStatusDropDown() {
            final WebElement statusBox = getSummaryHeadParts().get(4);
            assertEquals(statusBox.getText(),
                "Status \nALL\nRUNNING\nSUCCEEDED\nSUSPENDED\nWAITING\nKILLED",
                "Unexpected label in instance summary heading");
            return new Select(statusBox.findElement(By.tagName("select")));
        }

        public List<WebElement> getSummaryHeadParts() {
            return summaryTableHeading.findElements(By.xpath("./th/div"));
        }

        public WebElement getSelectAllCheckBox(List<WebElement> summaryHeadParts) {
            return summaryHeadParts.get(0).findElement(By.tagName("input"));
        }
    }


    public InstancePage openInstance(String nominalTime) {
        instanceListBox.findElement(By.xpath("//button[contains(.,'" + nominalTime + "')]")).click();
        return PageFactory.initElements(driver, InstancePage.class);
    }

    /**
     * Class representing summary of one instance.
     */
    public static final class OneInstanceSummary {
        private final WebElement oneInstanceSummary;
        private final String startTime;
        private final String endTime;
        private final String status;
        private final String nominalTime;

        private final Map<Object, Object> statusColorMap = ImmutableMap.builder()
            .put("WAITING", "rgba(51, 51, 51, 1)")
            .put("RUNNING", "")
            .put("KILLED", "")
            .put("SUCCEEDED", "")
            .put("SUSPENDED", "")
            .put("FAILED", "").build();
        private boolean isCheckBoxTicked;

        private OneInstanceSummary(WebElement oneInstanceSummary) {
            this.oneInstanceSummary = oneInstanceSummary;
            nominalTime = getNominalTimeButton().getText();
            startTime = getSummaryCols().get(2).getText();
            endTime = getSummaryCols().get(3).getText();

            final WebElement statusElement = getSummaryCols().get(4);
            assertTrue(statusElement.isDisplayed(), "Status should be displayed");
            final String statusText = statusElement.getText();
            final Object expectedColor = statusColorMap.get(statusText.trim());
            assertNotNull(expectedColor,
                "Unexpected status: " + statusText + " not found in: " + statusColorMap);
            //status color not checked
            //final String actualColor = statusElement.getCssValue("color");
            //assertEquals(actualColor, expectedColor,
            //    "Unexpected color for status in process instances block: " + statusText);
            status = statusText;
            isCheckBoxTicked = getCheckBox().isSelected();
        }

        private List<WebElement> getSummaryCols() {
            return oneInstanceSummary.findElements(By.tagName("td"));
        }

        private WebElement getCheckBox() {
            return getSummaryCols().get(0).findElement(By.tagName("input"));
        }

        private WebElement getNominalTimeButton() {
            return getSummaryCols().get(1);
        }

        public String getStartTime() {
            return startTime;
        }

        public String getEndTime() {
            return endTime;
        }

        public String getStatus() {
            return status;
        }
        public String getNominalTime() {
            return nominalTime;
        }

        public boolean isCheckBoxSelected() {
            return isCheckBoxTicked;
        }

        /**
         * Click the checkbox corresponding to this result. It is the responsibility of the
         * client to make sure that the web element for the instance is displayed and valid.
         */
        public void clickCheckBox() {
            getCheckBox().click();
            // Toggling of checkbox should change its internal state
            // Note that we can't expect the web element to be displayed & valid at the point this
            // object is used
            isCheckBoxTicked = !isCheckBoxTicked;
        }

        @Override
        public String toString() {
            return "OneInstanceSummary{"
                + "checkBox=" + isCheckBoxSelected()
                + ", nominalTime=" + getNominalTime()
                + ", startTime=" + getStartTime()
                + ", endTime=" + getEndTime()
                + ", status=" + getStatus()
                + "}";
        }
    }
}

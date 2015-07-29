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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.RecipeMerlin;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Page object of the Mirror creation page. */
public class MirrorWizardPage extends AbstractSearchPage {
    private static final Logger LOGGER = Logger.getLogger(MirrorWizardPage.class);
    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "formPage")
    })
    private WebElement mirrorBox;

    public MirrorWizardPage(WebDriver driver) {
        super(driver);
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(mirrorBox, "Mirror box");
    }


    public void setName(String name) {
        clearAndSetByNgModel("UIModel.name", name);
    }

    public void setTags(List<String> tags) {
        //TODO add code here
    }

    public void setMirrorType(FalconCLI.RecipeOperation recipeOperation) {
        switch (recipeOperation) {
        case HDFS_REPLICATION:
            driver.findElement(By.xpath("//button[contains(.,'File System')]")).click();
            break;
        case HIVE_DISASTER_RECOVERY:
            driver.findElement(By.xpath("//button[contains(.,'HIVE')]")).click();
            break;
        default:
            break;
        }
    }


    public void setHiveReplication(RecipeMerlin recipeMerlin) {
        if (StringUtils.isNotEmpty(recipeMerlin.getSourceTable())) {
            clickById("targetHIVETablesRadio");
            clearAndSetByNgModel("UIModel.source.hiveDatabase", recipeMerlin.getSourceDb());
            clearAndSetByNgModel("UIModel.source.hiveTables", recipeMerlin.getSourceTable());
        } else {
            clickById("targetHIVEDatabaseRadio");
            clearAndSetByNgModel("UIModel.source.hiveDatabases", recipeMerlin.getSourceDb());
        }
    }


    public void setStartTime(String validityStartStr) {
        final DateTime startDate = TimeUtil.oozieDateToDate(validityStartStr);

        clearAndSetByNgModel("UIModel.validity.start", DateTimeFormat.forPattern("MM/dd/yyyy").print(startDate));
        final WebElement startTimeBox = driver.findElement(By.className("startTimeBox"));
        final List<WebElement> startHourAndMinute = startTimeBox.findElements(By.tagName("input"));
        final WebElement hourText = startHourAndMinute.get(0);
        final WebElement minuteText = startHourAndMinute.get(1);
        clearAndSet(hourText, DateTimeFormat.forPattern("hh").print(startDate));
        clearAndSet(minuteText, DateTimeFormat.forPattern("mm").print(startDate));
        final WebElement amPmButton = startTimeBox.findElement(By.tagName("button"));
        if (!amPmButton.getText().equals(DateTimeFormat.forPattern("a").print(startDate))) {
            amPmButton.click();
        }
    }

    public void setEndTime(String validityEndStr) {
        final DateTime validityEnd = TimeUtil.oozieDateToDate(validityEndStr);

        clearAndSetByNgModel("UIModel.validity.end", DateTimeFormat.forPattern("MM/dd/yyyy").print(validityEnd));
        final WebElement startTimeBox = driver.findElement(By.className("endTimeBox"));
        final List<WebElement> startHourAndMinute = startTimeBox.findElements(By.tagName("input"));
        final WebElement hourText = startHourAndMinute.get(0);
        final WebElement minuteText = startHourAndMinute.get(1);
        clearAndSet(hourText, DateTimeFormat.forPattern("hh").print(validityEnd));
        clearAndSet(minuteText, DateTimeFormat.forPattern("mm").print(validityEnd));
        final WebElement amPmButton = startTimeBox.findElement(By.tagName("button"));
        if (!amPmButton.getText().equals(DateTimeFormat.forPattern("a").print(validityEnd))) {
            amPmButton.click();
        }
    }

    public void toggleAdvancedOptions() {
        final WebElement advanceOption = driver.findElement(By.xpath("//h4[contains(.,'Advanced options')]"));
        advanceOption.click();
    }

    public void setFrequency(Frequency frequency) {
        clearAndSetByNgModel("UIModel.frequency.number", frequency.getFrequency());
        selectNgModelByVisibleText("UIModel.frequency.unit", frequency.getTimeUnit().name().toLowerCase());
    }

    public void setHdfsDistCpMaxMaps(String distCpMaxMaps) {
        clearAndSetByNgModel("UIModel.allocation.hdfs.maxMaps", distCpMaxMaps);
    }


    public void setHdfsMaxBandwidth(String replicationMaxMaps) {
        clearAndSetByNgModel("UIModel.allocation.hdfs.maxBandwidth", replicationMaxMaps);
    }

    public void setHiveDistCpMaxMaps(String distCpMaxMaps) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxMapsDistcp", distCpMaxMaps);
    }


    public void setHiveReplicationMaxMaps(String replicationMaxMaps) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxMapsMirror", replicationMaxMaps);
    }

    public void setMaxEvents(String maxEvents) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxMapsEvents", maxEvents);
    }

    public void setHiveMaxBandwidth(String maxBandWidth) {
        clearAndSetByNgModel("UIModel.allocation.hive.maxBandwidth", maxBandWidth);
    }


    public void setSourceInfo(ClusterMerlin srcCluster) {
        setSourceStaging(srcCluster.getLocation("staging"));
        setSourceHiveEndpoint(srcCluster.getInterfaceEndpoint(Interfacetype.REGISTRY));
    }

    public void setSourceHiveEndpoint(String hiveEndpoint) {
        clearAndSetByNgModel("UIModel.hiveOptions.source.hiveServerToEndpoint", hiveEndpoint);
    }

    public void setSourceStaging(String stagingLocation) {
        clearAndSetByNgModel("UIModel.hiveOptions.source.stagingPath", stagingLocation);
    }

    public void setTargetInfo(ClusterMerlin tgtCluster) {
        setTargetStaging(tgtCluster.getLocation("staging"));
        setTargetHiveEndpoint(tgtCluster.getInterfaceEndpoint(Interfacetype.REGISTRY));
    }

    public void setTargetHiveEndpoint(String hiveEndPoint) {
        clearAndSetByNgModel("UIModel.hiveOptions.target.hiveServerToEndpoint", hiveEndPoint);
    }

    public void setTargetStaging(String hiveEndpoint) {
        clearAndSetByNgModel("UIModel.hiveOptions.target.stagingPath", hiveEndpoint);
    }

    public void setRetry(Retry retry) {
        selectNgModelByVisibleText("UIModel.retry.policy", retry.getPolicy().toString().toUpperCase());
        clearAndSetByNgModel("UIModel.retry.delay.number", retry.getDelay().getFrequency());
        selectNgModelByVisibleText("UIModel.retry.delay.unit", retry.getDelay().getTimeUnit().name().toLowerCase());
        clearAndSetByNgModel("UIModel.retry.attempts", String.valueOf(retry.getAttempts()));
    }


    public void setAcl(ACL acl) {
        setAclOwner(acl.getOwner());
        setAclGroup(acl.getGroup());
        setAclPermission(acl.getPermission());
    }

    public void setAclOwner(String aclOwner) {
        clearAndSetSlowlyByNgModel("UIModel.acl.owner", aclOwner);
    }

    public boolean isAclOwnerWarningDisplayed() {
        final WebElement warning =
            findElementByNgModel("UIModel.acl.owner").findElement(By.xpath("./following-sibling::*"));
        waitForAngularToFinish();
        return warning.isDisplayed();
    }

    public void setAclGroup(String aclGroup) {
        clearAndSetSlowlyByNgModel("UIModel.acl.group", aclGroup);
    }

    public boolean isAclGroupWarningDisplayed() {
        final WebElement warning =
            findElementByNgModel("UIModel.acl.group").findElement(By.xpath("./following-sibling::*"));
        waitForAngularToFinish();
        return warning.isDisplayed();
    }

    public void setAclPermission(String aclPermission) {
        clearAndSetSlowlyByNgModel("UIModel.acl.permissions", aclPermission);
    }

    public boolean isAclPermissionWarningDisplayed() {
        final WebElement warning =
            findElementByNgModel("UIModel.acl.permissions").findElement(By.xpath("./following-sibling::*"));
        waitForAngularToFinish();
        return warning.isDisplayed();
    }

    public void next() {
        final WebElement nextButton = driver.findElement(By.xpath("//button[contains(.,'Next')]"));
        nextButton.click();
    }

    public void previous() {
        final WebElement prevButton = driver.findElement(By.xpath("//button[contains(.,'Previous')]"));
        prevButton.click();
    }

    public void silentPrevious() {
        try {
            previous();
        } catch (Exception ignore) {
            //ignore
        }
    }

    public void cancel() {
        driver.findElement(By.xpath("//a[contains(.,'Cancel')]"));
    }

    public void save() {
        final WebElement saveButton = driver.findElement(By.xpath("//button[contains(.,'Save')]"));
        UIAssert.assertDisplayed(saveButton, "Save button in not displayed.");
        saveButton.click();
        waitForAlert();
    }

    public ClusterBlock getSourceBlock() {
        return new ClusterBlock("Source");
    }

    public ClusterBlock getTargetBlock() {
        return new ClusterBlock("Target");
    }

    public void applyRecipe(RecipeMerlin recipe) {
        final ClusterMerlin srcCluster = recipe.getSrcCluster();
        final ClusterMerlin tgtCluster = recipe.getTgtCluster();
        setName(recipe.getName());
        setTags(recipe.getTags());
        setMirrorType(recipe.getRecipeOperation());
        getSourceBlock().selectCluster(srcCluster.getName());
        getTargetBlock().selectCluster(tgtCluster.getName());
        getSourceBlock().selectRunHere();
        setStartTime(recipe.getValidityStart());
        setEndTime(recipe.getValidityEnd());
        toggleAdvancedOptions();
        switch (recipe.getRecipeOperation()) {
        case HDFS_REPLICATION:
            getSourceBlock().setPath(recipe.getSourceDir());
            getTargetBlock().setPath(recipe.getTargetDir());
            setHdfsDistCpMaxMaps(recipe.getDistCpMaxMaps());
            setHdfsMaxBandwidth(recipe.getDistCpMaxMaps());
            break;
        case HIVE_DISASTER_RECOVERY:
            setHiveReplication(recipe);
            setHiveDistCpMaxMaps(recipe.getDistCpMaxMaps());
            setHiveReplicationMaxMaps(recipe.getReplicationMaxMaps());
            setMaxEvents(recipe.getMaxEvents());
            setHiveMaxBandwidth(recipe.getMapBandwidth());
            setSourceInfo(recipe.getSrcCluster());
            setTargetInfo(recipe.getTgtCluster());
            break;
        default:
            break;
        }
        setFrequency(recipe.getFrequency());
        setRetry(recipe.getRetry());
        setAcl(recipe.getAcl());
    }

    public int getStepNumber() {
        try {
            driver.findElement(By.xpath("//button[contains(.,'Previous')]"));
            return 2;
        } catch (Exception ignore) {
            //ignore
        }
        return 1;
    }

    public Map<Summary, String> getSummaryProperties() {
        String formText = driver.findElement(By.id("formSummaryBox")).getText();
        Map<Summary, String> props = new EnumMap<>(Summary.class);
        props.put(Summary.NAME, getBetween(formText, "Name", "Type"));
        props.put(Summary.TYPE, getBetween(formText, "Type", "Tags"));
        props.put(Summary.TAGS, getBetween(formText, "Tags", "Source"));
        props.put(Summary.RUN_ON, getBetween(formText, "Run On", "Schedule"));
        props.put(Summary.START, getBetween(formText, "Start on:", "End on:"));
        props.put(Summary.END, getBetween(formText, "End on:", "Max Maps"));
        props.put(Summary.MAX_MAPS, getBetween(formText, "Max Maps", "Max Bandwidth"));
        props.put(Summary.MAX_BANDWIDTH, getBetween(formText, "Max Bandwidth", "ACL"));

        props.put(Summary.ACL_OWNER, getBetween(formText, "Owner:", "Group:"));
        props.put(Summary.ACL_GROUP, getBetween(formText, "Group:", "Permissions:"));
        props.put(Summary.ACL_PERMISSIONS, getBetween(formText, "Permissions:", "Retry"));

        props.put(Summary.RETRY_POLICY, getBetween(formText, "Policy:", "delay:"));
        props.put(Summary.RETRY_DELAY, getBetween(formText, "delay:", "Attempts:"));
        props.put(Summary.RETRY_ATTEMPTS, getBetween(formText, "Attempts:", "Frequency"));

        props.put(Summary.FREQUENCY, getBetween(formText, "Frequency", "Previous"));

        String source = getBetween(formText, "Source", "Target");
        String target = getBetween(formText, "Target", "Run On");
        if ("HDFS".equals(props.get(Summary.TYPE))) {
            props.put(Summary.SOURCE_LOCATION, getBetween(source, "Location", "Path"));
            props.put(Summary.TARGET_LOCATION, getBetween(target, "Location", "Path"));
            if ("HDFS".equals(props.get(Summary.SOURCE_LOCATION))) {
                props.put(Summary.SOURCE_CLUSTER, getBetween(source, "^", "Location"));
                props.put(Summary.SOURCE_PATH, getBetween(source, "Path:", "$"));

            } else {
                props.put(Summary.SOURCE_PATH, getBetween(source, "Path:", "URL"));
                props.put(Summary.SOURCE_URL, getBetween(source, "URL:", "$"));

            }
            if ("HDFS".equals(props.get(Summary.TARGET_LOCATION))) {
                props.put(Summary.TARGET_CLUSTER, getBetween(target, "^", "Location"));
                props.put(Summary.TARGET_PATH, getBetween(target, "Path:", "$"));

            } else {
                props.put(Summary.TARGET_PATH, getBetween(target, "Path:", "URL"));
                props.put(Summary.TARGET_URL, getBetween(target, "URL:", "$"));

            }

        } else {
            LOGGER.error("TODO Read info for HIVE replication.");
        }


        return props;
    }

    /** Parts of the mirror summary. */
    public enum Summary {
        NAME,
        TYPE,
        TAGS,
        RUN_ON,
        START,
        END,
        MAX_MAPS,
        MAX_BANDWIDTH,
        ACL_OWNER,
        ACL_GROUP,
        ACL_PERMISSIONS,
        RETRY_POLICY,
        RETRY_DELAY,
        RETRY_ATTEMPTS,
        FREQUENCY,
        SOURCE_LOCATION,
        SOURCE_PATH,
        SOURCE_CLUSTER,
        SOURCE_URL,
        TARGET_LOCATION,
        TARGET_PATH,
        TARGET_CLUSTER,
        TARGET_URL,

    }

    private static String getBetween(String text, String first, String second) {
        Pattern pattern = Pattern.compile(".*" + first + "(.+)" + second + ".*", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return matcher.group(1).trim();
        } else {
            return null;
        }
    }

    /**
     * Block of source or target cluster with parameters.
     */
    public final class ClusterBlock {
        private final WebElement mainBlock;
        private final WebElement runHereButton;
        private final String blockType;

        private ClusterBlock(String type) {
            this.blockType = type;
            mainBlock = driver.findElement(By.xpath("//h3[contains(.,'" + type + "')]/.."));
            runHereButton = mainBlock.findElement(By.id("runJobOn" + type + "Radio"));
        }

        public Set<Location> getAvailableLocationTypes() {
            List<WebElement> inputs = getLocationBox().findElements(By.xpath(".//input"));
            Set<Location> result = EnumSet.noneOf(Location.class);
            for (WebElement input : inputs) {
                result.add(Location.getByInput(input));
            }
            return result;
        }

        public Location getSelectedLocationType() {
            WebElement selected = getLocationBox()
                .findElement(By.xpath("//input[contains(@class,'ng-valid-parse')]"));
            return Location.getByInput(selected);
        }

        public void setLocationType(Location type) {
            getLocationBox().findElement(By.xpath(
                String.format(".//input[translate(@value,'azures','AZURES')='%s']", type.toString()))).click();
        }

        public void selectRunHere() {
            runHereButton.click();
        }

        public Set<String> getAvailableClusters() {
            List<WebElement> options = mainBlock.findElements(By.xpath(".//option[not(@disabled)]"));
            Set<String> clusters = new TreeSet<>();
            for (WebElement option : options) {
                clusters.add(option.getText());
            }
            return clusters;
        }

        public void selectCluster(String clusterName) {
            selectNgModelByVisibleText("UIModel." + blockType.toLowerCase() + ".cluster", clusterName);
        }

        public void setPath(String path) {
            final WebElement srcPathElement = getPath();
            clearAndSet(srcPathElement, path);
        }

        public boolean isRunHereSelected() {
            return runHereButton.getAttribute("class").contains("ng-valid-parse");
        }

        public boolean isRunHereAvailable() {
            return runHereButton.getAttribute("disabled") == null;
        }


        private WebElement getLocationBox() {
            return mainBlock.findElement(By.className("locationBox"));
        }

        private WebElement getPath() {
            return mainBlock.findElement(By.name(blockType.toLowerCase() + "ClusterPathInput"));
        }



    }

    /**
     * Types of source/target location.
     */
    public enum Location {
        HDFS,
        AZURE,
        S3;

        private static Location getByInput(WebElement input) {
            return Location.valueOf(input.getAttribute("value").trim().toUpperCase());
        }

    }

}

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

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.util.UIAssert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.ui.Select;

import java.text.SimpleDateFormat;
import java.util.Date;

/** Page object of the Feed creation page. */
public class FeedWizardPage extends AbstractSearchPage {

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "feedForm")
    })
    private WebElement feedBox;

    @FindBys({
            @FindBy(className = "mainUIView"),
            @FindBy(className = "feedForm"),
            @FindBy(className = "nextBtn")
    })
    private WebElement nextButton;

    @FindBys({
            @FindBy(xpath = "//button[contains(.,'add tag')]")
    })
    private WebElement addTagButton;

    @FindBys({
            @FindBy(xpath = "//button[contains(.,'add property')]")
    })
    private WebElement addPropertyButton;

    @FindBys({
            @FindBy(id = "feed.step5")
    })
    private WebElement saveFeedButton;

    public FeedWizardPage(WebDriver driver) {
        super(driver);
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(feedBox, "Feed box");
    }

    private WebElement getFeedName() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.name']"));
    }
    private WebElement getFeedDescription() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.description']"));
    }
    private WebElement getFeedTagKey(int index) {
        return feedBox.findElements(By.xpath("//input[@ng-model='tag.key']")).get(index);
    }
    private WebElement getFeedTagValue(int index) {
        return feedBox.findElements(By.xpath("//input[@ng-model='tag.value']")).get(index);
    }
    private WebElement getFeedGroups() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.groups']"));
    }
    private WebElement getFeedACLOwner() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.ACL.owner']"));
    }
    private WebElement getFeedACLGroup() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.ACL.group']"));
    }
    private WebElement getFeedACLPermissions() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.ACL.permission']"));
    }
    private WebElement getFeedSchemaLocation() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.schema.location']"));
    }
    private WebElement getFeedSchemaProvider() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.schema.provider']"));
    }

    private WebElement getFeedFrequencyQuantity() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.frequency.quantity']"));
    }
    private WebElement getFeedFrequencyUnit() {
        return feedBox.findElement(By.xpath("//select[@ng-model='feed.frequency.unit']"));
    }
    private WebElement getFeedLateArrivalCheckBox() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.lateArrival.active']"));
    }
    private WebElement getFeedLateArrivalCutOffQuantity() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.lateArrival.cutOff.quantity']"));
    }
    private WebElement getFeedLateArrivalCutOffUnit() {
        return feedBox.findElement(By.xpath("//select[@ng-model='feed.lateArrival.cutOff.unit']"));
    }
    private WebElement getFeedAvailabilityFlag() {
        return feedBox.findElement(By.xpath("//input[@ng-model='feed.availabilityFlag']"));
    }
    private Select getFeedTimeZone() {
        return new Select(feedBox.findElement(By.xpath("//time-zone-select[@ng-model='feed.timezone']/select")));
    }
    private WebElement getFeedPropertyKey(int index) {
        return feedBox.findElements(By.xpath("//input[@ng-model='property.key']")).get(index);
    }
    private WebElement getFeedPropertyValue(int index) {
        return feedBox.findElements(By.xpath(
            "//div[@ng-repeat='property in feed.customProperties']/*/input[@ng-model='property.value']")).get(index);
    }

    private WebElement getFeedPath(int index) {
        return feedBox.findElements(By.xpath("//input[@ng-model='location.path']")).get(index);
    }

    private Select getFeedClusterSource() {
        return new Select(feedBox.findElement(By.id("clusterNameSelect")));
    }

    private WebElement getFeedClusterRetentionLimit() {
        return feedBox.findElement(By.xpath("//input[@ng-model='cluster.retention.quantity']"));
    }

    private WebElement getFeedClusterRetentionUnit() {
        return feedBox.findElement(By.xpath("//select[@ng-model='cluster.retention.unit']"));
    }

    private WebElement getFeedClusterValidityStartDate() {
        return feedBox.findElement(By.xpath("//input[@ng-model='cluster.validity.start.date']"));
    }

    private WebElement getFeedClusterValidityHour(int index) {
        return feedBox.findElements(By.xpath("//input[@ng-model='hours']")).get(index);
    }

    private WebElement getFeedClusterValidityMinutes(int index) {
        return feedBox.findElements(By.xpath("//input[@ng-model='minutes']")).get(index);
    }

    private WebElement getFeedClusterValidityMeridian(int index) {
        return feedBox.findElements(By.xpath("//td[@ng-show='showMeridian']/button")).get(index);
    }

    private WebElement getFeedClusterValidityEndDate() {
        return feedBox.findElement(By.xpath("//input[@ng-model='cluster.validity.end.date']"));
    }

    public void setFeedName(String name){
        sendKeysSlowly(getFeedName(), name);
    }
    public void setFeedDescription(String description){
        getFeedDescription().sendKeys(description);
    }
    public void setFeedTagKey(int index, String tagKey){
        getFeedTagKey(index).sendKeys(tagKey);
    }
    public void setFeedTagValue(int index, String tagValue){
        getFeedTagValue(index).sendKeys(tagValue);
    }
    // Tags are in the format, "first=yes","second=yes","third=yes". Need a separate method to handle this
    public void setFeedTags(String tagsStr){
        if (tagsStr == null){
            return;
        }
        String[] tags = tagsStr.split(",");
        for (int i=0; i < tags.length; i++){
            String[] keyValue = tags[i].split("=");
            setFeedTagKey(i, keyValue[0]);
            setFeedTagValue(i, keyValue[1]);
            if (tags.length > i+1){
                addTagButton.click();
            }
        }
    }

    public void setFeedGroups(String feedGroups){
        getFeedGroups().sendKeys(feedGroups);
    }
    public void setFeedACLOwner(String feedACLOwner){
        getFeedACLOwner().clear();
        getFeedACLOwner().sendKeys(feedACLOwner);
    }
    public void setFeedACLGroup(String feedACLGroup){
        getFeedACLGroup().clear();
        getFeedACLGroup().sendKeys(feedACLGroup);
    }
    public void setFeedACLPermissions(String feedACLPermissions){
        getFeedACLPermissions().clear();
        getFeedACLPermissions().sendKeys(feedACLPermissions);
    }
    public void setFeedSchemaLocation(String feedSchemaLocation){
        sendKeysSlowly(getFeedSchemaLocation(), feedSchemaLocation);
    }
    public void setFeedSchemaProvider(String feedSchemaProvider){
        sendKeysSlowly(getFeedSchemaProvider(), feedSchemaProvider);
    }

    public void setFeedFrequencyQuantity(String frequencyQuantity){
        getFeedFrequencyQuantity().sendKeys(frequencyQuantity);
    }
    public void setFeedFrequencyUnit(String frequencyUnit){
        getFeedFrequencyUnit().sendKeys(frequencyUnit);
    }
    public void setFeedLateArrivalCheckBox(){
        getFeedLateArrivalCheckBox().click();
    }
    public void setFeedLateArrivalCutOffQuantity(int lateArrivalCutOffQuantity){
        getFeedLateArrivalCutOffQuantity().sendKeys(Integer.toString(lateArrivalCutOffQuantity));
    }
    public void setFeedLateArrivalCutOffUnit(String lateArrivalCutOffUnit){
        getFeedLateArrivalCutOffUnit().sendKeys(lateArrivalCutOffUnit);
    }
    public void setFeedAvailabilityFlag(String availabilityFlag){
        getFeedAvailabilityFlag().sendKeys(availabilityFlag);
    }
    public void setFeedTimeZone(){
        String timeZone = "GMT+00:00";
        getFeedTimeZone().selectByValue(timeZone);
    }
    public void setFeedPropertyKey(int index, String propertyKey){
        getFeedPropertyKey(index).sendKeys(propertyKey);
    }
    public void setFeedPropertyValue(int index, String propertyValue){
        getFeedPropertyValue(index).sendKeys(propertyValue);
    }

    public void setFeedPath(int index, String path){
        getFeedPath(index).sendKeys(path);
    }

    public void setFeedClusterSource(String clusterSource){
        getFeedClusterSource().selectByVisibleText(clusterSource);
    }

    public void setFeedClusterRetentionLimit(String clusterRetentionLimit){
        getFeedClusterRetentionLimit().clear();
        sendKeysSlowly(getFeedClusterRetentionLimit(), clusterRetentionLimit);
    }

    public void setFeedClusterRetentionUnit(String clusterRetentionUnit){
        getFeedClusterRetentionUnit().sendKeys(clusterRetentionUnit);
    }

    public void setFeedClusterValidityStartDate(String clusterValidityStartDate){
        getFeedClusterValidityStartDate().clear();
        sendKeysSlowly(getFeedClusterValidityStartDate(), clusterValidityStartDate);
    }
    public void setFeedClusterValidityHour(int index, String clusterValidityHour){
        getFeedClusterValidityHour(index).clear();
        getFeedClusterValidityHour(index).sendKeys(clusterValidityHour);
    }
    public void setFeedClusterValidityMinutes(int index, String clusterValidityMinutes){
        getFeedClusterValidityMinutes(index).clear();
        getFeedClusterValidityMinutes(index).sendKeys(clusterValidityMinutes);
    }
    public void setFeedClusterValidityMeridian(int index, String clusterValidityMeridian){
        // Toggle AM PM, if clusterValidityMeridian value is not equal to AM PM Button text
        if (!clusterValidityMeridian.equalsIgnoreCase(getFeedClusterValidityMeridian(index).getText())){
            getFeedClusterValidityMeridian(index).click();
        }
    }
    public void setFeedClusterValidityEndDate(String clusterValidityEndDate){
        getFeedClusterValidityEndDate().clear();
        sendKeysSlowly(getFeedClusterValidityEndDate(), clusterValidityEndDate);
    }

    // Enter feed info on Page 1 of FeedSetup Wizard
    public void setFeedGeneralInfo(FeedMerlin feed) {
        setFeedName(feed.getName());
        setFeedDescription(feed.getDescription());
        setFeedTags(feed.getTags());
        setFeedGroups(feed.getGroups());
        setFeedACLOwner(feed.getACL().getOwner());
        setFeedACLGroup(feed.getACL().getGroup());
        setFeedACLPermissions(feed.getACL().getPermission());
        setFeedSchemaLocation(feed.getSchema().getLocation());
        setFeedSchemaProvider(feed.getSchema().getProvider());
    }

    // Enter feed info on Page 2 of FeedSetup Wizard
    public void setFeedPropertiesInfo(FeedMerlin feed){
        setFeedFrequencyQuantity(feed.getFrequency().getFrequency());
        setFeedFrequencyUnit(feed.getFrequency().getTimeUnit().toString());
        setFeedLateArrivalCheckBox();
        setFeedLateArrivalCutOffQuantity(feed.getLateArrival().getCutOff().getFrequencyAsInt());
        setFeedLateArrivalCutOffUnit(feed.getLateArrival().getCutOff().getTimeUnit().toString());
        setFeedAvailabilityFlag(feed.getAvailabilityFlag());
        setFeedTimeZone();
        setFeedPropertyKey(0, feed.getProperties().getProperties().get(0).getName());
        setFeedPropertyValue(0, feed.getProperties().getProperties().get(0).getValue());
        addPropertyButton.click();
        setFeedPropertyKey(1, feed.getProperties().getProperties().get(1).getName());
        setFeedPropertyValue(1, feed.getProperties().getProperties().get(1).getValue());
    }

    // Enter feed info on Page 3 of FeedSetup Wizard
    public void setFeedLocationInfo(FeedMerlin feed){
        setFeedPath(0, feed.getLocations().getLocations().get(0).getPath());
        setFeedPath(1, feed.getLocations().getLocations().get(1).getPath());
        setFeedPath(2, feed.getLocations().getLocations().get(2).getPath());

    }

    // Enter feed info on Page 4 of FeedSetup Wizard
    public void setFeedClustersInfo(FeedMerlin feed){
        setFeedClusterSource(feed.getClusters().getClusters().get(0).getName());
        setFeedLocationInfo(feed);
        Date startDate = feed.getClusters().getClusters().get(0).getValidity().getStart();
        Date endDate = feed.getClusters().getClusters().get(0).getValidity().getEnd();
        setFeedClusterValidityStartDate(new SimpleDateFormat("MM/dd/yyyy").format(startDate));
        setFeedClusterValidityHour(0, new SimpleDateFormat("h").format(startDate));
        setFeedClusterValidityMinutes(0, new SimpleDateFormat("m").format(startDate));
        setFeedClusterValidityMeridian(0, new SimpleDateFormat("a").format(startDate));
        setFeedClusterValidityEndDate(new SimpleDateFormat("MM/dd/yyyy").format(endDate));
        setFeedClusterValidityHour(1, new SimpleDateFormat("h").format(endDate));
        setFeedClusterValidityMinutes(1, new SimpleDateFormat("m").format(endDate));
        setFeedClusterValidityMeridian(1, new SimpleDateFormat("a").format(endDate));
        /*
        The merlin feed has 9000 months.
        The UI only support till two digits.
        Need to send hardcoded value of 99,
        instead of feed.getClusters().getClusters().get(0).getRetention().getLimit().getFrequency()
        */
        setFeedClusterRetentionLimit("99");
        setFeedClusterRetentionUnit(feed.getClusters().getClusters().get(0)
            .getRetention().getLimit().getTimeUnit().name());
    }

    // setFeed method runs the default feed setup wizard, entering data on each page
    public void setFeed(FeedMerlin feed){
        setFeedGeneralInfo(feed);
        nextButton.click();
        setFeedPropertiesInfo(feed);
        nextButton.click();
        setFeedLocationInfo(feed);
        nextButton.click();
        setFeedClustersInfo(feed);
        nextButton.click();
        saveFeedButton.click();
    }

}

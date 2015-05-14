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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.util.UIAssert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/** Page object of the Process creation page. */
public class ProcessWizardPage extends AbstractSearchPage {

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "entityForm")
    })
    private WebElement processBox;

    public ProcessWizardPage(WebDriver driver) {
        super(driver);
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(processBox, "Process box");
    }

    private WebElement getNextButton() {
        return driver.findElement(By.id("nextButton"));
    }

    public void pressNext() {
        getNextButton().click();
    }

    /*----- Step1 elements & operations ----*/
    private WebElement getName() {
        return driver.findElement(By.id("entityNameField"));
    }

    public void setName(String name) {
        final WebElement nameElement = getName();
        nameElement.clear();
        for (String s : name.split("")) {
            nameElement.sendKeys(s);
        }
    }
    private WebElement getTagsSection() {
        return driver.findElement(By.id("tagsSection"));
    }

    private WebElement getAddTagButton() {
        return driver.findElement(By.className("formViewContainer"))
            .findElement(By.xpath("./form/div[4]/button"));
    }

    private List<WebElement> getDeleteTagButtons() {
        return getTagsSection().findElements(By.tagName("button"));
    }

    private List<WebElement> getTagTextFields() {
        return getTagsSection().findElements(By.tagName("input"));
    }

    public void deleteTags() {
        //delete all tags
        final List<WebElement> deleteTagButtons = getDeleteTagButtons();
        for (WebElement deleteTagButton : Lists.reverse(deleteTagButtons)) {
            deleteTagButton.click();
        }
        for (WebElement textField : getTagTextFields()) {
            textField.clear();
        }
    }

    public void setTags(List<String> tags) {
        deleteTags();
        //create enough number of tag fields
        final int numTags = tags.size();
        for (int i = 0; i < numTags - 1; i++) {
            getAddTagButton().click();
        }
        final List<WebElement> tagTextFields = getTagTextFields();
        Assert.assertEquals(tagTextFields.size() % 2, 0,
            "Number of text fields for tags should be even, found: " + tagTextFields.size());
        for (int i = 0; i < (tagTextFields.size() / 2); i++) {
            final String oneTag = tags.get(i);
            final String[] tagParts = oneTag.split("=");
            Assert.assertEquals(tagParts.length, 2,
                "Each tag is expected to be of form key=value, found: " + oneTag);
            String key = tagParts[0];
            String val = tagParts[1];
            tagTextFields.get(2 * i).sendKeys(key);
            tagTextFields.get(2 * i + 1).sendKeys(val);
        }
    }

    private WebElement getWfName() {
        return driver.findElement(By.id("workflowNameField"));
    }

    private WebElement getOozieRadio() {
        return driver.findElement(By.id("oozieEngineRadio"));
    }

    private WebElement getPigRadio() {
        return driver.findElement(By.id("pigEngineRadio"));
    }

    private WebElement getHiveRadio() {
        return driver.findElement(By.id("hiveEngineRadio"));
    }

    private Select getEngineVersion() {
        return new Select(driver.findElement(By.id("engineVersionField")));
    }

    private WebElement getPath() {
        return driver.findElement(By.id("pathField"));
    }

    public void setWorkflow(Workflow processWf) {
        final WebElement wfName = getWfName();
        wfName.clear();
        wfName.sendKeys(processWf.getName());
        switch (processWf.getEngine()) {
        case OOZIE:
            getOozieRadio().click();
            break;
        case PIG:
            getPigRadio().click();
            break;
        case HIVE:
            getHiveRadio().click();
            break;
        default:
            Assert.fail("Unexpected workflow engine: " + processWf.getEngine());
        }
        final String version = processWf.getVersion();
        if (StringUtils.isNotEmpty(version) && !version.equals("1.0")) {
            getEngineVersion().selectByVisibleText(version);
        }
        final WebElement path = getPath();
        path.clear();
        path.sendKeys(processWf.getPath());
    }

    private WebElement getAclOwner() {
        return driver.findElement(By.name("aclOwnerInput"));
    }

    private WebElement getAclGroup() {
        return driver.findElement(By.name("aclGroupInput"));
    }

    private WebElement getAclPerm() {
        return driver.findElement(By.name("aclPermissionsInput"));
    }

    public void setAcl(ACL acl) {
        final WebElement aclOwner = getAclOwner();
        aclOwner.clear();
        aclOwner.sendKeys(acl.getOwner());
        final WebElement aclGroup = getAclGroup();
        aclGroup.clear();
        aclGroup.sendKeys(acl.getGroup());
        final WebElement aclPerm = getAclPerm();
        aclPerm.clear();
        aclPerm.sendKeys(acl.getPermission());
    }

    public void doStep1(ProcessMerlin process) {
        setName(process.getName());
        final String tags = StringUtils.trimToEmpty(process.getTags());
        setTags(Arrays.asList(tags.split(",")));
        setWorkflow(process.getWorkflow());
        setAcl(process.getACL());
        final WebElement step1Element = getName();
        pressNext();
        new WebDriverWait(driver, AbstractSearchPage.PAGELOAD_TIMEOUT_THRESHOLD).until(
            ExpectedConditions.stalenessOf(step1Element));
    }

    /*----- Step2 elements & operations ----*/
    private Select getTimezone() {
        return new Select(driver.findElement(By.id("timeZoneSelect")));
    }

    public void setTimezone(TimeZone timezone) {
        getTimezone().selectByValue(timezone.getDisplayName());
    }
}

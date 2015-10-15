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

import org.apache.falcon.entity.v0.Entity;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.testng.Assert;

/**
 * https://issues.apache.org/jira/browse/FALCON-1546.
 * Parent class for cluster, feed and process wizard pages.
 */
public abstract class EntityWizardPage extends AbstractSearchPage {
    @FindBy(xpath = "//i[contains(@class, 'pointer')]")
    protected WebElement xmlPreviewPointer;
    protected WebElement xmlPreview = null;

    public EntityWizardPage(WebDriver driver) {
        super(driver);
    }

    /**
     * Expand/collapse xml preview.
     * @param shouldBeExpanded should preview be expanded or collapsed.
     */
    public void clickXMLPreview(boolean shouldBeExpanded) {
        if (isXmlPreviewExpanded() != shouldBeExpanded) {
            xmlPreviewPointer.click();
        }
        Assert.assertEquals(isXmlPreviewExpanded(), shouldBeExpanded,
            "Xml preview should be " + (shouldBeExpanded ? " expanded." : " collapsed."));
    }

    /**
     * @return true if xml preview exists and is displayed, false otherwise.
     */
    public boolean isXmlPreviewExpanded() {
        xmlPreview = getElementOrNull("//textarea[@ng-model='prettyXml']");
        return xmlPreview != null && xmlPreview.isDisplayed();
    }

    public String getXMLPreview() {
        //preview block fetches changes slower then they appear on the form
        waitForAngularToFinish();
        clickXMLPreview(true);
        return xmlPreview.getAttribute("value");
    }

    public abstract Entity getEntityFromXMLPreview();

    /**
     * Pushes xml into xml preview block.
     * @param xml entity definition
     */
    public void setXmlPreview(String xml) {
        clickEditXml(true);
        xmlPreview.clear();
        xmlPreview.sendKeys(xml);
        waitForAngularToFinish();
        clickEditXml(false);
    }

    /**
     * Clicks on editXml button.
     */
    public void clickEditXml(boolean shouldBeEnabled) {
        waitForAngularToFinish();
        clickXMLPreview(true);
        getEditXMLButton().click();
        String disabled = xmlPreview.getAttribute("disabled");
        Assert.assertEquals(disabled == null, shouldBeEnabled,
            "Xml preview should be " + (shouldBeEnabled ? "enabled" : "disabled"));
    }

    public abstract WebElement getEditXMLButton();
}

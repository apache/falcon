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

import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.ui.pages.Page;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

/** Parent page object for all the search ui pages. */
public abstract class AbstractSearchPage extends Page {

    public static final String UI_URL = MerlinConstants.PRISM_URL;
    public static final long PAGELOAD_TIMEOUT_THRESHOLD = 10;

    public AbstractSearchPage(WebDriver driver) {
        super(driver);
        pageHeader = PageFactory.initElements(driver, PageHeader.class);
    }

    private PageHeader pageHeader;

    @FindBy(className = "mainUIView")
    private WebElement mainUI;

    public PageHeader getPageHeader() {
        return pageHeader;
    }

    protected WebElement getParentElement(WebElement element) {
        return element.findElement(By.xpath(".."));
    }

    /**
     * A rough check to make sure that we are indeed on the correct page.
     */
    public abstract void checkPage();

    // Utility method to enter the data slowly on an element
    public void sendKeysSlowly(WebElement webElement, String data){
        for (String str : data.split("")) {
            webElement.sendKeys(str);
        }

    }
}

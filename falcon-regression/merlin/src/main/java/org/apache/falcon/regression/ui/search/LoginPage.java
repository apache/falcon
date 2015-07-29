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
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;
import org.testng.Assert;

/** Page object for the Login Page. */
public class LoginPage extends AbstractSearchPage {
    private static final Logger LOGGER = Logger.getLogger(LoginPage.class);
    public static final String UI_DEFAULT_USER = MerlinConstants.CURRENT_USER_NAME;

    public LoginPage(WebDriver driver) {
        super(driver);
    }

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "login")
    })
    protected WebElement loginElem;

    public static LoginPage open(WebDriver driver) {
        driver.get(UI_URL);
        return PageFactory.initElements(driver, LoginPage.class);
    }

    private WebElement getUserTextBox() {
        return loginElem.findElement(By.xpath("//input[@name='user']"));
    }

    public void appendToUserName(String text) {
        getUserTextBox().sendKeys(text);
    }

    public String getUserVisibleWarning() {
        final WebElement userTextBox = getUserTextBox();

        final WebElement userWarnLabel = getParentElement(userTextBox).findElement(
            By.xpath("//label[contains(@class, 'custom-danger') and contains(@class, 'validationMessageGral')]"));
        if (userWarnLabel.isDisplayed()) {
            return userWarnLabel.getText();
        }
        return "";
    }

    /** Try to login by pressing the login button. */
    public void tryLogin() {
        LOGGER.info("Trying to login.");
        final WebElement loginButton = loginElem.findElement(By.id("login.submit"));
        UIAssert.assertDisplayed(loginButton, "Login button");
        loginButton.click();
    }

    /** Login successfully and take to the next page i.e. search page. */
    public SearchPage doDefaultLogin() {
        if (!MerlinConstants.IS_SECURE) {
            getUserTextBox().clear();
            appendToUserName(UI_DEFAULT_USER);
            tryLogin();
        }
        LOGGER.info("Search page should have opened.");
        final SearchPage searchPage = PageFactory.initElements(driver, SearchPage.class);
        searchPage.checkPage();
        final PageHeader searchHeader = searchPage.getPageHeader();
        if (!MerlinConstants.IS_SECURE) {
            searchHeader.checkLoggedIn();
            Assert.assertEquals(searchHeader.getLoggedInUser(), LoginPage.UI_DEFAULT_USER,
                "Unexpected user is displayed");
        }
        return searchPage;
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(loginElem, "Cluster box");
    }

}

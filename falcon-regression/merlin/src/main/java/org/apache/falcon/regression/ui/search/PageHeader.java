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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.List;

/** Page object for header of the search ui pages. */
public class PageHeader {
    private static final Logger LOGGER = Logger.getLogger(PageHeader.class);

    protected WebDriver driver;

    public PageHeader(WebDriver driver) {
        this.driver = driver;
    }

    @FindBy(className = "navbar")
    private WebElement header;

    @FindBys({
        @FindBy(className = "navbar"),
        @FindBy(className = "logoTitle")
    })
    private WebElement homeButton;

    @FindBys({
        @FindBy(className = "navbar"),
        @FindBy(className = "logoTitle"),
        @FindBy(className = "falconLogo")
    })
    private WebElement falconLogo;

    @FindBys({
        @FindBy(className = "navbar"),
        @FindBy(className = "loginHeaderBox")
    })
    private WebElement loginHeaderBox;

    @FindBys({
        @FindBy(className = "navbar"),
        @FindBy(className = "createNavWrapper")
    })
    private WebElement createEntityBox;

    @FindBy(id = "cluster.create")
    private WebElement clusterCreateButton;

    @FindBy(id = "feed.create")
    private WebElement feedCreateButton;

    @FindBy(id = "process.create")
    private WebElement processCreateButton;

    @FindBy(id = "dataset.create")
    private WebElement mirrorCreateButton;

    @FindBys({
        @FindBy(className = "navbar"),
        @FindBy(className = "uploadNavWrapper")
    })
    private WebElement uploadEntityBox;

    @FindBys({
        @FindBy(className = "navbar"),
        @FindBy(className = "uploadNavWrapper"),
        @FindBy(className = "btn-file")
    })
    private WebElement uploadEntityButton;


    public WebElement getHomeButton() {
        return homeButton;
    }

    public void checkLoggedIn() {
        Assert.assertEquals(getLogoutButton().getText(), "Logout",
            "Unexpected text on logout button");
    }

    public SearchPage gotoHome() {
        homeButton.click();
        final SearchPage searchPage = PageFactory.initElements(driver, SearchPage.class);
        searchPage.checkPage();
        final PageHeader searchHeader = searchPage.getPageHeader();
        searchHeader.checkLoggedIn();
        Assert.assertEquals(searchHeader.getLoggedInUser(), LoginPage.UI_DEFAULT_USER,
            "Unexpected user is displayed");
        return searchPage;
    }

    public void checkLoggedOut() {
        UIAssert.assertNotDisplayed(getLogoutButton(), "logout button");
    }

    /**
     * Check header and make sure all the buttons/links are working correctly. Handles both
     * logged in and logged out scenarios.
     */
    public void checkHeader() {
        //home button is always displayed
        UIAssert.assertDisplayed(homeButton, "falcon logo");
        Assert.assertEquals(homeButton.getText(), "Falcon", "Unexpected home button text");
        UIAssert.assertDisplayed(falconLogo, "falcon logo");
        final WebElement helpLink = loginHeaderBox.findElement(By.tagName("a"));
        UIAssert.assertDisplayed(helpLink, "help link");

        final String oldUrl = driver.getCurrentUrl();
        //displayed if user is logged in: create entity buttons, upload entity button, username
        if (getLogoutButton().isDisplayed()) {
            //checking create entity box
            UIAssert.assertDisplayed(createEntityBox, "Create entity box");
            final WebElement createEntityLabel = createEntityBox.findElement(By.tagName("h4"));
            Assert.assertEquals(createEntityLabel.getText(), "Create an entity",
                "Unexpected create entity text");
            //checking upload entity part
            UIAssert.assertDisplayed(uploadEntityBox, "Create entity box");
            final WebElement uploadEntityLabel = uploadEntityBox.findElement(By.tagName("h4"));
            Assert.assertEquals(uploadEntityLabel.getText(), "Upload an entity",
                "Unexpected upload entity text");
            UIAssert.assertDisplayed(uploadEntityButton, "Create entity box");
            Assert.assertEquals(uploadEntityButton.getText(), "Browse for the XML file",
                "Unexpected text on upload entity button");
            //checking if logged-in username is displayed
            AssertUtil.assertNotEmpty(getLoggedInUser(), "Expecting logged-in username.");

            //create button navigation
            doCreateCluster();
            driver.get(oldUrl);
            doCreateFeed();
            driver.get(oldUrl);
            doCreateProcess();
            driver.get(oldUrl);
            doCreateMirror();
            driver.get(oldUrl);
        }
        //home button navigation
        homeButton.click();
        Assert.assertTrue(getHomeUrls().contains(driver.getCurrentUrl()),
            "home button navigate to: " + driver.getCurrentUrl() + " instead of: " + getHomeUrls());
        driver.get(oldUrl);

        //help link navigation
        Assert.assertEquals(helpLink.getText(), "Help", "Help link expected to have text 'Help'");
        helpLink.click();
        new WebDriverWait(driver, AbstractSearchPage.PAGELOAD_TIMEOUT_THRESHOLD).until(
            ExpectedConditions.stalenessOf(helpLink));
        Assert.assertEquals(driver.getCurrentUrl(), MerlinConstants.HELP_URL,
            "Unexpected help url");
        driver.get(oldUrl);
    }

    public ClusterWizardPage doCreateCluster() {
        UIAssert.assertDisplayed(clusterCreateButton, "Cluster create button");
        Assert.assertEquals(clusterCreateButton.getText(), "Cluster",
            "Unexpected text on create cluster button");
        clusterCreateButton.click();
        final ClusterWizardPage clusterPage = PageFactory.initElements(driver, ClusterWizardPage.class);
        clusterPage.checkPage();
        return clusterPage;
    }

    public FeedWizardPage doCreateFeed() {
        UIAssert.assertDisplayed(feedCreateButton, "Feed create button");
        Assert.assertEquals(feedCreateButton.getText(), "Feed",
            "Unexpected text on create feed button");
        feedCreateButton.click();
        final FeedWizardPage feedPage = PageFactory.initElements(driver, FeedWizardPage.class);
        feedPage.checkPage();
        return feedPage;
    }

    public ProcessWizardPage doCreateProcess() {
        UIAssert.assertDisplayed(processCreateButton, "Process create button");
        Assert.assertEquals(processCreateButton.getText(), "Process",
            "Unexpected text on create process button");
        processCreateButton.click();
        final ProcessWizardPage processPage = PageFactory.initElements(driver, ProcessWizardPage.class);
        processPage.checkPage();
        return processPage;
    }

    public NewMirrorPage doCreateMirror() {
        UIAssert.assertDisplayed(mirrorCreateButton, "Mirror create button");
        Assert.assertEquals(mirrorCreateButton.getText(), "Mirror",
            "Unexpected text on create mirror button");
        mirrorCreateButton.click();
        final NewMirrorPage mirrorPage = PageFactory.initElements(driver, NewMirrorPage.class);
        mirrorPage.checkPage();
        return mirrorPage;
    }

    private List<String> getHomeUrls() {
        List<String> urls = new ArrayList<>();
        String homeUrl = MerlinConstants.PRISM_URL;
        urls.add(homeUrl);
        urls.add(homeUrl.replaceAll("/$", "") + "/#/");
        return urls;
    }

    public String getLoggedInUser() {
        return loginHeaderBox.findElement(By.tagName("div")).getText();
    }

    private WebElement getLogoutButton() {
        return loginHeaderBox.findElement(By.tagName("button"));
    }

    public LoginPage doLogout() {
        LOGGER.info("Going to logout.");
        getLogoutButton().click();
        final LoginPage loginPage = PageFactory.initElements(driver, LoginPage.class);
        loginPage.checkPage();
        return loginPage;
    }

}

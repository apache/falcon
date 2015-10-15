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

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.ui.pages.Page;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/** Parent page object for all the search ui pages. */
public abstract class AbstractSearchPage extends Page {

    public static final String UI_URL = MerlinConstants.PRISM_URL;
    private static final Logger LOGGER = Logger.getLogger(AbstractSearchPage.class);
    public static final int PAGELOAD_TIMEOUT_THRESHOLD = 10;
    public static final int ALERT_LIFETIME = 3000;

    public AbstractSearchPage(WebDriver driver) {
        super(driver);
        waitForAngularToFinish();
        LOGGER.info("Going to initialize Page Header.");
        pageHeader = PageFactory.initElements(driver, PageHeader.class);
        LOGGER.info("Initialization done.");
    }

    private PageHeader pageHeader;

    @FindBy(className = "mainUIView")
    protected WebElement mainUI;

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
    public static void sendKeysSlowly(WebElement webElement, String data){
        for (String str : data.split("")) {
            webElement.sendKeys(str);
        }

    }

    public static void clearAndSet(WebElement webElement, String val) {
        webElement.clear();
        webElement.sendKeys(val);
    }

    public static void clearAndSetSlowly(WebElement webElement, String val) {
        webElement.clear();
        sendKeysSlowly(webElement, val);
    }

    protected WebElement findElementByNgModel(String ngModelName) {
        // trying to get an xpath that looks like: "//*[@ng-model='UIModel.retry.policy']"
        final String xpathExpression = "//*[@ng-model='" + ngModelName + "']";
        final List<WebElement> webElements = driver.findElements(By.xpath(xpathExpression));
        Assert.assertEquals(webElements.size(), 1, "Element is not unique for ng-model: " + ngModelName);
        return webElements.get(0);
    }

    protected void selectNgModelByVisibleText(String ngModelName, String visibleText) {
        final WebElement webElement = findElementByNgModel(ngModelName);
        final Select select = new Select(webElement);
        select.selectByVisibleText(visibleText);
    }

    protected void clearAndSetByNgModel(String ngModelName, String value) {
        final WebElement webElement = findElementByNgModel(ngModelName);
        clearAndSet(webElement, value);
    }

    protected void clearAndSetSlowlyByNgModel(String ngModelName, String value) {
        final WebElement webElement = findElementByNgModel(ngModelName);
        clearAndSetSlowly(webElement, value);
    }

    protected void clickById(String id) {
        final List<WebElement> webElements = driver.findElements(By.id(id));
        Assert.assertEquals(webElements.size(), 1, "Element is not unique.");
        webElements.get(0).click();
    }

    protected void clickByNgModel(String ngModelName) {
        final WebElement webElement = findElementByNgModel(ngModelName);
        webElement.click();
    }

    // Utility method to get Dropdown Values
    public List<String> getDropdownValues(Select element){
        List<WebElement> allOptions = element.getOptions();
        List<String> values = new ArrayList<>();
        for (WebElement option:allOptions){
            values.add(option.getText());
        }
        return values;
    }


    protected void waitForAngularToFinish() {
        final String javaScript = "return (window.angular != null) && "
            + "(angular.element(document).injector() != null) && "
            + "(angular.element(document).injector().get('$http').pendingRequests.length === 0)";
        boolean isLoaded = false;
        for (int i = 0; i < PAGELOAD_TIMEOUT_THRESHOLD && !isLoaded; i++) {
            TimeLimiter timeLimiter = new SimpleTimeLimiter();
            final JavascriptExecutor proxyJsExecutor =
                timeLimiter.newProxy((JavascriptExecutor) driver, JavascriptExecutor.class, 10, TimeUnit.SECONDS);
            try {
                final Object output = proxyJsExecutor.executeScript(javaScript);
                isLoaded = Boolean.valueOf(output.toString());
            } catch (Exception e) {
                LOGGER.info("Checking of pending request failed because of: " + ExceptionUtils.getFullStackTrace(e));
            }
            LOGGER.info(i+1 + ". waiting on angular to finish.");
            TimeUtil.sleepSeconds(1);
        }
        LOGGER.info("angular is done continuing...");
    }

    public String getActiveAlertText() {
        if (waitForAlert()) {
            waitForAngularToFinish();
            String script = "return $('div.messages.notifs > div:last-child').text();";
            String message = (String)((JavascriptExecutor)driver).executeScript(script);
            return message.trim();
        } else {
            return null;
        }
    }

    /**
     * Wait for active alert. Check it's lifetime (the period when alert is displayed).
     */
    public void validateAlertLifetime() {
        final WebElement alertsBlock = driver.findElement(By.xpath("//div[@class='messages notifs']"));
        try {
            final MutablePair<Long, Long> pair = new MutablePair<>(Long.MAX_VALUE, Long.MAX_VALUE);
            // wait 5 seconds for alert to start blinking and record time of first blink
            new WebDriverWait(driver, 5, 100).until(new ExpectedCondition<Boolean>() {
                @Nullable
                @Override
                public Boolean apply(WebDriver webDriver) {
                    String style = alertsBlock.getAttribute("style");
                    if ((style.contains("opacity") && !style.contains("opacity: 1;"))
                            || style.contains("display: block;")) {
                        pair.setLeft(System.currentTimeMillis());
                        return true;
                    }
                    return false;
                }
            });
            // wait 5 seconds for alert to stop blinking and record time of stoppage
            for (int i = 0; i < ALERT_LIFETIME + 3000; i += 100) {
                String style = alertsBlock.getAttribute("style");
                if (style.contains("display: none;")) {
                    pair.setRight(Math.min(System.currentTimeMillis(), pair.getRight()));
                } else {
                    pair.setRight(Long.MAX_VALUE);
                }
                TimeUtil.sleepSeconds(0.1);
            }
            long diff = pair.getRight() - pair.getLeft();
            LOGGER.info(String.format("Alert was live %d millis.", pair.getRight() - pair.getLeft()));
            Assert.assertTrue(ALERT_LIFETIME <= diff, "Alert was present for too short period of time");
        } catch (TimeoutException e) {
            Assert.fail("Alert didn't appear in 5 seconds.");
        }
    }

    /**
     * Wait for active alert.
     * @return true is alert is present
     */
    protected boolean waitForAlert() {
        final WebElement alertsBlock = driver.findElement(By.xpath("//div[@class='messages notifs']"));
        try {
            new WebDriverWait(driver, 5).until(new ExpectedCondition<Boolean>() {
                @Nullable
                @Override
                public Boolean apply(WebDriver webDriver) {
                    String style = alertsBlock.getAttribute("style");
                    return (style.contains("opacity") && !style.contains("opacity: 1;"))
                            || style.contains("display: block;");
                }
            });
            return true;
        } catch (TimeoutException e) {
            return false;
        }
    }

    /**
     * Performs simple check of element presence.
     */
    public WebElement getElementOrNull(String xpath) {
        try {
            return driver.findElement(By.xpath(xpath));
        } catch (NoSuchElementException ignored) {
            return null;
        }
    }


    /**
     * Method imitates click on check box. If click is not performed method retries the click.
     * @param expectedState whether check box is expected to be enabled or not after click.
     */
    protected void clickCheckBoxSecurely(WebElement checkBox, boolean expectedState) {
        double gap = 0.5;
        for (int attempt = 1; attempt <= (DEFAULT_TIMEOUT / gap); attempt++) {
            LOGGER.info("Attempt to click a check box: " + attempt);
            checkBox.click();
            if (checkBox.isSelected() == expectedState) {
                return;
            }
            TimeUtil.sleepSeconds(gap);
        }
        Assert.fail("Check box state was not changed even in " + DEFAULT_TIMEOUT + " seconds.");
    }

}

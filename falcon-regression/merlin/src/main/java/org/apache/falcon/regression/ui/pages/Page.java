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

package org.apache.falcon.regression.ui.pages;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.SearchContext;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;


public abstract class Page {
    protected final static int DEFAULT_TIMEOUT = 10;
    protected String URL;
    protected WebDriver driver;

    protected String expectedElement;
    protected String notFoundMsg;

    private static final Logger logger = Logger.getLogger(Page.class);

    Page(WebDriver driver, ColoHelper helper) {
        this.driver = driver;
        URL = helper.getClusterHelper().getHostname();
    }

    /**
     * Go to page in browser
     */
    public void navigateTo() {
        logger.info("Navigating to " + URL);
        driver.get(URL);
        waitForElement(expectedElement, DEFAULT_TIMEOUT, notFoundMsg);
    }

    /**
     * Refresh page
     */
    public void refresh() {
        logger.info("Refreshing page " + URL);
        driver.navigate().refresh();
    }

    /**
     * Wait for some WebElement defined by xpath. Throws TimeoutException if element is not visible after defined time.
     * @param webElement find xpath inside this WebElement
     * @param xpath xpath of expected WebElement
     * @param timeoutSeconds how many seconds we should wait for element
     * @param errMessage message for TimeoutException
     */
    public void waitForElement(WebElement webElement, final String xpath,
                               final long timeoutSeconds, String errMessage) {
        waitForElementAction(webElement, xpath, timeoutSeconds, errMessage, true);
    }

    /**
     * Wait for some WebElement defined by xpath. Throws TimeoutException if element is not visible after defined time.
     * @param xpath xpath of expected WebElement
     * @param timeoutSeconds how many seconds we should wait for element
     * @param errMessage message for TimeoutException
     */
    public void waitForElement(final String xpath, final long timeoutSeconds, String errMessage) {
        waitForElementAction(null, xpath, timeoutSeconds, errMessage, true);
    }

    /**
     * Wait until WebElement disappears.
     * @param xpath xpath of expected WebElement
     * @param timeoutSeconds how many seconds we should wait for disappearing
     * @param errMessage message for TimeoutException
     */
    public void waitForDisappear(final String xpath, final long timeoutSeconds, String errMessage) {
        waitForElementAction(null, xpath, timeoutSeconds, errMessage, false);
    }

    /**
     * Wait until WebElement became visible
     * @param xpath xpath of expected WebElement
     * @param timeoutSeconds how many seconds we should wait for visibility
     * @param errMessage message for TimeoutException
     */
    public void waitForDisplayed(String xpath, long timeoutSeconds, String errMessage) {
        waitForElement(xpath, timeoutSeconds, errMessage);
        WebElement element = driver.findElement(By.xpath(xpath));
        for (int i = 0; i < timeoutSeconds * 10; i++) {
            if (element.isDisplayed()) return;
            TimeUtil.sleepSeconds(0.1);
        }
        throw new TimeoutException(errMessage);
    }

    private void waitForElementAction(WebElement webElement, String xpath, long timeoutSeconds,
                                      String errMessage, boolean expected) {
        try {
            new WebDriverWait(driver, timeoutSeconds)
                .until(new Condition(webElement, xpath, expected));
        } catch (TimeoutException e) {
            TimeoutException ex = new TimeoutException(errMessage);
            ex.initCause(e);
            throw ex;
        }
    }

    static class Condition implements ExpectedCondition<Boolean> {

        private final boolean isPresent;
        private String xpath;
        private WebElement webElement;

        public Condition(String xpath, boolean isPresent) {
            this.xpath = xpath;
            this.isPresent = isPresent;
        }

        public Condition(WebElement webElement, String xpath, boolean isPresent) {
            this.webElement = webElement;
            this.xpath = xpath;
            this.isPresent = isPresent;
        }

        @Override
        public Boolean apply(WebDriver webDriver) {
            SearchContext search = (webElement == null) ? webDriver : webElement;
            return search.findElements(By.xpath(xpath)).isEmpty() != isPresent;
        }
    }
}

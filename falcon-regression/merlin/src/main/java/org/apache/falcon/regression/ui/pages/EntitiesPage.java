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


import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntitiesPage extends Page {

    private static final Logger logger = Logger.getLogger(EntitiesPage.class);

    private final static String ACTIVE_NXT_BTN
            = "//ul/li[not(@class)]/a[contains(text(),'»')]";
    protected final static String ENTITIES_TABLE
            = "//table[@id='entity-list']/tbody/tr";
    private final static String PAGE_NUMBER = "//ul[@class='pagination']/li[@class='active']/a";

    public EntitiesPage(WebDriver driver, ColoHelper helper, EntityType type) {
        super(driver, helper);
        URL += "/index.html?type=" + type.toString().toLowerCase();

        expectedElement = ENTITIES_TABLE;
        notFoundMsg = String.format("No entities on %sS page", type);
    }

    /**
     * Returns status of defined entity
     * @param entityName name of entity
     * @return status of defined entity
     */
    public EntityStatus getEntityStatus(String entityName) {
        navigateTo();
        while (true) {
            String status = getEntitiesOnPage().get(entityName);
            if (status != null) return EntityStatus.valueOf(status);
            if (nextPagePresent()) {
                goNextPage();
            } else {
                break;
            }
        }
        return null;
    }

    /**
     * Loads next page
     */
    private void goNextPage() {
        logger.info("Navigating to next page...");
        WebElement nextButton = driver.findElement(By.xpath(ACTIVE_NXT_BTN));
        nextButton.click();
        waitForElement(expectedElement, DEFAULT_TIMEOUT, "Next page didn't load");
    }


    /**
     * Checks if next page is present
     * @return true if next page is present
     */

    private boolean nextPagePresent() {
        logger.info("Checking if next page is present...");
        try {
            new WebDriverWait(driver, DEFAULT_TIMEOUT).until(new Condition(ACTIVE_NXT_BTN, true));
            return true;
        } catch (TimeoutException e) {
            return false;
        }
    }

    /**
     * Returns page number
     * @return page number
     */
    public int getPageNumber() {
        String number = driver.findElement(By.xpath(PAGE_NUMBER)).getText();
        return Integer.parseInt(number);
    }

    private Map<String,String> getEntitiesOnPage() {
        logger.info("Reading all entities on page...");
        List<WebElement> lines = driver.findElements(By.xpath(ENTITIES_TABLE));
        Map<String, String> entities = new HashMap<String, String>();
        for (WebElement line : lines) {
            WebElement name = line.findElement(By.xpath("./td[1]/a"));
            WebElement status = line.findElement(By.xpath("./td[2]"));
            entities.put(name.getText(), status.getText());
        }
        return entities;
    }

    /**
     * Status of entity that can be shown on Falcon UI
     */
    public enum EntityStatus {
        UNKNOWN, SUBMITTED, RUNNING, SUSPENDED
    }
}

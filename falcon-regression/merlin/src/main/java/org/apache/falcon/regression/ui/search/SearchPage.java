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

import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Page object for the Search Page. */
public class SearchPage extends AbstractSearchPage {
    public SearchPage(WebDriver driver) {
        super(driver);
    }


    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "searchBoxContainer")
    })
    private WebElement searchBlock;

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "dashboardBox")
    })
    private WebElement resultBlock;

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "dashboardBox"),
        @FindBy(tagName = "thead")
    })
    private WebElement resultHeader;

    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "dashboardBox"),
        @FindBy(className = "buttonRow")
    })
    private WebElement resultButtons;

    private List<WebElement> getSearchResultElements() {
        return resultBlock.findElements(By.className("entityRow"));
    }

    public List<SearchResult> getSearchResults() {
        List<SearchResult> searchResults = new ArrayList<>();
        for (WebElement oneResultElement : getSearchResultElements()) {
            final List<WebElement> resultParts = oneResultElement.findElements(By.tagName("td"));
            final String entityName = resultParts.get(1).getText();
            final SearchResult searchResult = SearchResult.create(entityName);

            final String[] allClasses = oneResultElement.getAttribute("class").split(" ");
            final String classOfSelectedRow = "rowSelected";
            if (Arrays.asList(allClasses).contains(classOfSelectedRow)) {
                searchResult.withChecked(true);
            }

            final String tags = resultParts.get(2).getText();
            searchResult.withTags(tags);

            final String clusterName = resultParts.get(3).getText();
            searchResult.withClusterName(clusterName);

            final String type = resultParts.get(4).getText();
            searchResult.withType(type);

            final String status = resultParts.get(5).getText();
            searchResult.withStatus(status);
            searchResults.add(searchResult);
        }
        return searchResults;
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(searchBlock, "Cluster box");
    }

    private WebElement getSearchBox() {
        return searchBlock.findElement(By.className("input"));
    }

    public List<SearchResult> doSearch(String searchString) {
        clearSearch();
        return appendAndSearch(searchString);
    }

    public List<SearchResult> appendAndSearch(String appendedPart) {
        for(String queryParam : appendedPart.split("\\s+")) {
            getSearchBox().sendKeys(queryParam);
            getSearchBox().sendKeys(Keys.SPACE);
        }
        String activeAlert = getActiveAlertText();
        if (activeAlert != null) {
            Assert.assertEquals(activeAlert.trim(), "No results matched the search criteria.");
            return Collections.emptyList();
        }
        UIAssert.assertDisplayed(resultBlock, "Search result block");
        return getSearchResults();

    }

    private String getActiveAlertText() {
        WebElement alertsBlock = driver.findElement(By.className("messages-to-show"));
        List<WebElement> alerts = alertsBlock.findElements(By.className("ng-animate"));
        if (!alerts.isEmpty()) {
            WebElement last = alerts.get(alerts.size() - 1);
            if (last.isDisplayed()) {
                return last.getText();
            }
        }
        return null;
    }

    public SearchQuery getSearchQuery() {
        return new SearchQuery(searchBlock);
    }

    public void clearSearch() {
        getSearchBox().clear();
        SearchQuery query = getSearchQuery();
        for (int i = 0; i < query.getElementsNumber(); i++) {
            removeLastParam();
        }
    }

    public void removeLastParam() {
        getSearchBox().sendKeys(Keys.BACK_SPACE);
        getSearchBox().sendKeys(Keys.BACK_SPACE);
    }


    public void checkNoResult() {
        UIAssert.assertNotDisplayed(resultBlock, "Search result block");
    }

    /** Class representing search query displayed in the search box. */
    public static final class SearchQuery {
        private WebElement searchBlock;
        private String name;
        private String type;
        private int elementsNumber;
        private final List<String> tags = new ArrayList<>();
        private static final Logger LOGGER = Logger.getLogger(SearchQuery.class);

        public SearchQuery(WebElement searchBlock) {
            this.searchBlock = searchBlock;
            updateElements();
        }

        private SearchQuery updateElements() {
            name = null;
            type = null;
            tags.clear();
            final WebElement queryGroup = searchBlock.findElement(By.className("tag-list"));
            final List<WebElement> queryParts = queryGroup.findElements(By.tagName("li"));
            elementsNumber = queryParts.size();
            for (WebElement queryPart : queryParts) {
                final WebElement queryLabel = queryPart.findElement(By.tagName("strong"));
                final String queryText = queryPart.findElement(By.tagName("span")).getText();
                switch (queryLabel.getText().trim()) {
                case "NAME:":
                    if (name != null) {
                        LOGGER.warn(String.format("NAME block is already added: '%s' => '%s'",
                            name, queryText));
                    }
                    name = queryText;
                    break;
                case "TAG:":
                    tags.add(queryText);
                    break;
                case "TYPE:":
                    if (type != null) {
                        LOGGER.warn(String.format("TYPE block is already added: '%s' => '%s'",
                            type, queryText));
                    }
                    type = queryText;
                    break;
                default:
                    Assert.fail("There should be only TAGs or TYPE");
                }
            }
            return this;
        }


        public String getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public List<String> getTags() {
            return tags;
        }

        public int getElementsNumber() {
            return elementsNumber;
        }

        /**
         * Delete element by index (1, 2, 3,..).
         * @param index of element in search query.
         * @return true if deletion was successful
         */
        public boolean deleteByIndex(int index) {
            if (index > elementsNumber || index < 1) {
                LOGGER.warn("There is no element with index=" + index);
                return false;
            }
            int oldElementsNumber = elementsNumber;
            final WebElement queryGroup = searchBlock.findElement(By.className("tag-list"));
            final List<WebElement> queryParts = queryGroup.findElements(By.tagName("li"));
            queryParts.get(index - 1).findElement(By.className("remove-button")).click();
            this.updateElements();
            boolean result = oldElementsNumber == elementsNumber + 1;
            LOGGER.info(String.format(
                "Element with index=%d was%s deleted", index, result ? "" : "n't"));
            return result;
        }

        public boolean deleteLast() {
            return deleteByIndex(elementsNumber);
        }
    }

    /** Class representing search result displayed on the entity table page. */
    public static final class SearchResult {
        private boolean isChecked = false;
        private String entityName;
        private String tags = "";
        private String clusterName;
        private String type;
        private EntityStatus status;

        public static SearchResult create(String entityName) {
            return new SearchResult(entityName);
        }

        public SearchResult withChecked(boolean pIsChecked) {
            this.isChecked = pIsChecked;
            return this;
        }

        private SearchResult(String entityName) {
            this.entityName = entityName;
        }

        public SearchResult withTags(String pTags) {
            this.tags = pTags;
            return this;
        }

        public SearchResult withClusterName(String pClusterName) {
            this.clusterName = pClusterName;
            return this;
        }

        public SearchResult withType(String pType) {
            this.type = pType;
            return this;
        }

        public SearchResult withStatus(String pStatus) {
            this.status = EntityStatus.valueOf(pStatus);
            return this;
        }

        public boolean isChecked() {
            return isChecked;
        }

        public String getEntityName() {
            return entityName;
        }

        public String getTags() {
            return tags;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getType() {
            return type;
        }

        public EntityStatus getStatus() {
            return status;
        }
    }

}

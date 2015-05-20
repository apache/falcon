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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.openqa.selenium.support.PageFactory;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Page object for the Search Page. */
public class SearchPage extends AbstractSearchPage {

    private static final String CLASS_OF_SELECTED_ROW = "rowSelected";

    private static final Logger LOGGER = Logger.getLogger(SearchPage.class);

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
            if (Arrays.asList(allClasses).contains(CLASS_OF_SELECTED_ROW)) {
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


    public EntityPage openEntityPage(String entityName) {
        return click(doSearch(entityName).get(0));
    }

    public EntityPage click(SearchResult result) {
        LOGGER.info("attempting to click: " + result + " on search page.");
        for (WebElement oneResultElement : getSearchResultElements()) {
            final List<WebElement> resultParts = oneResultElement.findElements(By.tagName("td"));
            final WebElement entityNameElement = resultParts.get(1);
            final String entityName = entityNameElement.getText();
            if (entityName.equals(result.getEntityName())) {
                entityNameElement.findElement(By.tagName("button")).click();
                return PageFactory.initElements(driver, EntityPage.class);
            }
        }
        return  null;
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

    public void selectRow(int row) {
        changeRowClickedStatus(row, true);
    }

    public void deselectRow(int row) {
        changeRowClickedStatus(row, false);
    }

    private void changeRowClickedStatus(int row, boolean checked) {
        WebElement checkboxBlock = resultBlock.findElements(By.className("entityRow")).get(row - 1);
        if (checked != checkboxBlock.getAttribute("class").contains(CLASS_OF_SELECTED_ROW)) {
            checkboxBlock.findElement(By.xpath("./td/input")).click();
        }
    }

    public void clickSelectAll() {
        resultBlock.findElement(By.xpath(".//input[@ng-model='selectedAll']")).click();
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

    public Set<Button> getButtons(boolean active) {
        List<WebElement> buttons = resultBlock.findElement(By.className("buttonsRow"))
            .findElements(By.className("btn"));
        Set<Button> result = EnumSet.noneOf(Button.class);
        for (WebElement button : buttons) {
            if ((button.getAttribute("disabled") == null) == active) {
                result.add(Button.valueOf(button.getText()));
            }
        }
        return result;
    }

    public void clickButton(Button button) {
        resultBlock.findElement(By.className("buttonsRow"))
            .findElements(By.className("btn")).get(button.ordinal()).click();
        waitForAngularToFinish();
    }

    /**
     * Buttons available for entities in result box.
     */
    public enum Button {
        Schedule,
        Resume,
        Suspend,
        Edit,
        Copy,
        Delete,
        XML
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
            Assert.assertFalse(clusterName.contains(","), "getClusterName() called"
                + " in multi-cluster setup: " + clusterName + ", maybe use getClusterNames()");
            return clusterName;
        }

        public List<String> getClusterNames() {
            return Arrays.asList(clusterName.split(","));
        }


        public String getType() {
            return type;
        }

        public EntityStatus getStatus() {
            return status;
        }

        @Override
        public String toString() {
            return "SearchResult{"
                + "isChecked=" + isChecked
                + ", entityName='" + entityName + '\''
                + ", tags='" + tags + '\''
                + ", clusterName='" + clusterName + '\''
                + ", type='" + type + '\''
                + ", status='" + status + '\''
                + '}';
        }

        public static void assertEqual(List<SearchResult> searchResults,
                                       List<Entity> expectedEntities, String errorMessage) {
            Assert.assertEquals(searchResults.size(), expectedEntities.size(), errorMessage
                + "(Length of lists don't match, searchResults: " + searchResults
                + " expectedEntities: " + expectedEntities + ")");
            for (Entity entity : expectedEntities) {
                boolean found = false;
                for (SearchResult result : searchResults) {
                    //entities are same if they have same name & type
                    if (entity.getName().equals(result.entityName)) {
                        //entity type in SearchResults has a different meaning
                        //so, not comparing entity types

                        //equality of cluster names
                        List<String> entityClusters = null;
                        switch (entity.getEntityType()) {
                        case FEED:
                            final FeedMerlin feed = (FeedMerlin) entity;
                            entityClusters = feed.getClusterNames();
                            // tags equality check
                            Assert.assertEquals(result.getTags(),
                                StringUtils.trimToEmpty(feed.getTags()),
                                errorMessage + "(tags mismatch: " + result.entityName
                                    + " & " + entity.toShortString() + ")");
                            break;
                        case PROCESS:
                            final ProcessMerlin process = (ProcessMerlin) entity;
                            entityClusters = process.getClusterNames();
                            // tags equality check
                            Assert.assertEquals(result.getTags(),
                                StringUtils.trimToEmpty(process.getTags()),
                                errorMessage + "(tags mismatch: " + result.entityName
                                    + " & " + entity.toShortString() + ")");
                            break;
                        default:
                            Assert.fail("Cluster entity is unexpected: " + entity);
                            break;
                        }
                        Collections.sort(entityClusters);
                        final List<String> actualClusters = result.getClusterNames();
                        Collections.sort(actualClusters);
                        Assert.assertEquals(actualClusters, entityClusters, errorMessage
                            + "(cluster names mismatch: " + result + " " + entity + ")");
                        found = true;
                    }
                }
                Assert.assertTrue(found,
                    "Entity: " + entity.toShortString() + " not found in: " + searchResults);
            }
        }

    }

}

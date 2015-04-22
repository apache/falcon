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
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
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
        getSearchBox().sendKeys(searchString + "\n");
        UIAssert.assertDisplayed(resultBlock, "Search result block");
        return getSearchResults();
    }

    public SearchQuery getSearchQuery() {
        final WebElement queryGroup = searchBlock.findElement(By.className("tag-list"));
        final List<WebElement> queryParts = queryGroup.findElements(By.tagName("li"));
        if (queryParts.size() == 0) {
            return SearchQuery.create(null);
        } else {
            final WebElement namePart = queryParts.remove(0);
            final WebElement nameLabel = namePart.findElement(By.tagName("strong"));
            Assert.assertEquals(nameLabel.getText(), "NAME: ", "Name label of query");
            final WebElement nameElem = namePart.findElement(By.tagName("span"));
            SearchQuery searchQuery = SearchQuery.create(nameElem.getText());
            for (WebElement tagPart : queryParts) {
                final WebElement tagLabel = tagPart.findElement(By.tagName("strong"));
                Assert.assertEquals(tagLabel.getText(), "TAG: ", "Tag label of query");
                final WebElement tagElem = tagPart.findElement(By.tagName("span"));
                searchQuery.withTag(tagElem.getText());
            }
            return searchQuery;
        }
    }

    public void checkNoResult() {
        UIAssert.assertNotDisplayed(resultBlock, "Search result block");
    }

    /** Class representing search query displayed in the search box. */
    public static final class SearchQuery {
        private final String name;

        private final List<String> tags = new ArrayList<>();

        private SearchQuery(String name) {
            this.name = name;
        }

        public static SearchQuery create(String name) {
            return new SearchQuery(name);
        }

        public SearchQuery withTag(String tag) {
            tags.add(tag);
            return this;
        }

        public String getName() {
            return name;
        }

        public List<String> getTags() {
            return tags;
        }
    }

    /** Class representing search result displayed on the entity table page. */
    public static final class SearchResult {
        private boolean isChecked = false;
        private String entityName;
        private String tags = "";
        private String clusterName;
        private String type;
        private String status;

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
            this.status = pStatus;
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

        public String getStatus() {
            return status;
        }
    }

}

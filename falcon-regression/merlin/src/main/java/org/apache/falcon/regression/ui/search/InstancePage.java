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
import org.openqa.selenium.support.PageFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;


/**
 * Class representation of Search UI entity page.
 */
public class InstancePage extends AbstractSearchPage {
    private final String nominalTime;

    /**
     * Possible instance actions available on instance page.
     */
    public enum Button {
        Resume,
        Suspend,
        Stop
    }

    public InstancePage(WebDriver driver) {
        super(driver);
        nominalTime = driver.findElement(By.xpath("//h3")).getText().split("\\|")[1].trim();
    }


    @FindBys({
            @FindBy(className = "detailsBox"),
            @FindBy(className = "row")
    })
    private WebElement detailsBox;

    @FindBys({
            @FindBy(xpath = "//h3/a")
    })
    private WebElement entityLink;



    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(detailsBox, "Dependency box");
        UIAssert.assertDisplayed(entityLink, "Link to parrent entity");
    }

    public InstancePage refreshPage() {
        return backToEntityPage().openInstance(nominalTime);
    }

    public String getStatus() {
        return driver.findElement(By.xpath("//h4[@class='instance-title']/span")).getText();
    }

    public String getEntityName() {
        return entityLink.getText();
    }

    public boolean isLineagePresent() {
        List<WebElement> lineage = driver.findElements(By.className("lineage-graph"));
        return !lineage.isEmpty() && lineage.get(0).isDisplayed();
    }


    public Set<Button> getButtons(boolean active) {
        List<WebElement> buttons = detailsBox.findElement(By.className("buttonCell"))
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
        detailsBox.findElement(By.className("buttonCell"))
                .findElements(By.className("btn")).get(button.ordinal()).click();
        waitForAngularToFinish();
    }

    public EntityPage backToEntityPage() {
        entityLink.click();
        return PageFactory.initElements(driver, EntityPage.class);
    }
}

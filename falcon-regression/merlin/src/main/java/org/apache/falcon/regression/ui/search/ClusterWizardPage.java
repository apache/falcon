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
import org.apache.falcon.entity.v0.cluster.ACL;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.core.util.UIAssert;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.FindBys;
import org.testng.Assert;

import java.util.List;

/** Page object of the Cluster creation page. */
public class ClusterWizardPage extends AbstractSearchPage {
    private static final Logger LOGGER = Logger.getLogger(ClusterWizardPage.class);
    @FindBys({
        @FindBy(className = "mainUIView"),
        @FindBy(className = "clusterForm")
    })
    private WebElement clusterBox;
    @FindBy(id = "cluster.step1")
    private WebElement next;
    @FindBy(id = "cluster.step2")
    private WebElement save;
    @FindBy(id = "cluster.backToStep1")
    private WebElement previous;
    @FindBy(xpath = "//a[contains(text(), 'Cancel')]")
    private WebElement cancel;
    @FindBy(id = "cluster.editXML")
    private WebElement editXML;
    @FindBy(xpath = "//div[contains(@class, 'clusterSummaryRow')][h4]")
    private WebElement summaryBox;
    @FindBy(xpath = "//div[contains(@class, 'xmlPreviewContainer')]//textarea")
    private WebElement xmlPreview;

    public ClusterWizardPage(WebDriver driver) {
        super(driver);
    }

    @Override
    public void checkPage() {
        UIAssert.assertDisplayed(clusterBox, "Cluster box");
    }

    /**
     * Fills cluster setup forms with values retrieved from Filling object.
     */
    public void fillForm(ClusterMerlin cluster) {
        setName(cluster.getName());
        setColo(cluster.getColo());
        setDescription(cluster.getDescription());
        setTags(cluster.getTags());
        ACL acl = cluster.getACL();
        setOwner(acl.getOwner());
        setGroup(acl.getGroup());
        setPermissions(acl.getPermission());
        for(Interface iface : cluster.getInterfaces().getInterfaces()) {
            setInterface(iface);
        }
        for (Property property : cluster.getProperties().getProperties()) {
            addProperty(property.getName(), property.getValue());
        }
        setLocations(cluster.getLocations().getLocations());
    }

    /**
     * Methods to fill specific wizard fields.
     */
    public void setName(String name) {
        WebElement nameInput = getNameInput();
        nameInput.clear();
        sendKeysSlowly(nameInput, name);
    }

    public String getName() {
        return getNameInput().getAttribute("value");
    }

    private WebElement getNameInput() {
        return driver.findElement(By.xpath("//div[label[text()='Name']]/input"));
    }

    public void setColo(String colo) {
        WebElement coloInput = clusterBox.findElement(By.xpath("//div[label[text()='Colo']]/input"));
        coloInput.clear();
        sendKeysSlowly(coloInput, colo);
    }

    public void setDescription(String description) {
        WebElement descriptionInput = clusterBox.findElement(By.xpath("//div[label[text()='Description']]/input"));
        descriptionInput.clear();
        sendKeysSlowly(descriptionInput, description);
    }

    public void setOwner(String owner) {
        WebElement ownerInput = clusterBox.findElement(By.xpath("//div[label[text()='Owner']]/input"));
        ownerInput.clear();
        sendKeysSlowly(ownerInput, owner);
    }

    public void setGroup(String group) {
        WebElement groupInput = clusterBox.findElement(By.xpath("//div[label[text()='Group']]/input"));
        groupInput.clear();
        sendKeysSlowly(groupInput, group);
    }

    public void setPermissions(String permissions) {
        WebElement permissionsInput = clusterBox.findElement(By.xpath("//div[label[text()='Permissions']]/input"));
        permissionsInput.clear();
        sendKeysSlowly(permissionsInput, permissions);
    }

    /**
     * Common method to fill interfaces.
     */
    public void setInterface(Interface iface) {
        String root = String.format("//div[contains(., '%s')]", iface.getType().value());
        String xpath = root + "/div/input[contains(@ng-model, '%s')]";
        WebElement ifaceEndpoint = clusterBox.findElement(By.xpath(String.format(xpath, "_interface._endpoint")));
        WebElement ifaceVersion = clusterBox.findElement(By.xpath(String.format(xpath, "_interface._version")));
        ifaceEndpoint.clear();
        sendKeysSlowly(ifaceEndpoint, iface.getEndpoint());
        if (iface.getVersion() != null) {
            ifaceVersion.clear();
            sendKeysSlowly(ifaceVersion, iface.getVersion());
        }
    }

    /**
     * Populates form with tags.
     */
    public void setTags(String tagsStr){
        if (!StringUtils.isEmpty(tagsStr)) {
            String [] tags = tagsStr.split(",");
            for (int i = 0; i < tags.length; i++) {
                if (i > 0){
                    clickAddTag();
                }
                String key = tags[i].trim().split("=")[0];
                String value = tags[i].trim().split("=")[1];
                addTag(key, value);
            }
        }
    }

    /**
     * Populates last (empty) tag and value fields and creates new fields.
     * @param key tag key
     * @param value tag value
     */
    public void addTag(String key, String value) {
        List<WebElement> tagInputs = clusterBox.findElements(By.xpath("//input[@ng-model='tag.key']"));
        List<WebElement> valueInputs = clusterBox.findElements(By.xpath("//input[@ng-model='tag.value']"));
        WebElement tagInput = tagInputs.get(tagInputs.size() - 1);
        sendKeysSlowly(tagInput, key);
        WebElement valueInput = valueInputs.get(valueInputs.size() - 1);
        sendKeysSlowly(valueInput, value);
    }

    public void clickAddTag() {
        clusterBox.findElement(By.xpath("//button[contains(., 'add tag')]")).click();
    }

    public void clickDeleteTag() {
        List<WebElement> buttons = clusterBox
            .findElements(By.xpath("//div[@class='row dynamic-table-spacer ng-scope']//button[contains(.,'delete')]"));
        buttons.get(buttons.size() - 1).click();
    }

    /**
     * Fills property fields and creates new empty property fields.
     */
    public void addProperty(String name, String value) {
        List<WebElement> propInputs = clusterBox.findElements(By.xpath("//input[@ng-model='property._name']"));
        List<WebElement> valueInputs = clusterBox.findElements(By.xpath("//input[@ng-model='property._value']"));
        WebElement propInput = propInputs.get(propInputs.size()-1);
        sendKeysSlowly(propInput, name);
        WebElement valueInput = valueInputs.get(valueInputs.size()-1);
        sendKeysSlowly(valueInput, value);
        clickAddProperty();
    }

    public void clickAddProperty() {
        clusterBox.findElement(By.xpath("//button[contains(., 'add property')]")).click();
    }

    /**
     * Method to set locations. Location can be only one of ClusterLocationType. So adding new location only after
     * respective compulsory location was filled.
     * @param locations locations
     */
    public void setLocations(List<Location> locations) {
        boolean staging = false, temp = false, working = false;
        for(Location location : locations) {
            WebElement pathInput = null;
            if (location.getName() == ClusterLocationType.STAGING && !staging) {
                pathInput = clusterBox.findElement(By.id("location.staging"));
                staging = true;
            } else  if (location.getName() == ClusterLocationType.TEMP && !temp) {
                pathInput = clusterBox.findElement(By.id("location.temp"));
                temp = true;
            } else if (location.getName() == ClusterLocationType.WORKING && !working) {
                pathInput = clusterBox.findElement(By.id("location.working"));
                working = true;
            } else {
                fillAdditionalLocation(location);
            }
            if (pathInput != null) {
                pathInput.clear();
                sendKeysSlowly(pathInput, location.getPath());
            }
        }
    }

    /**
     * Method populates last location fields with values and creates new fields.
     */
    public void fillAdditionalLocation(Location location) {
        List<WebElement> allNameInputs = clusterBox
            .findElements(By.xpath("//input[contains(@ng-model, 'location._name')]"));
        sendKeysSlowly(allNameInputs.get(allNameInputs.size() - 1), location.getName().value());
        List<WebElement> allPathInputs = clusterBox
            .findElements(By.xpath("//input[contains(@ng-model, 'location._path')]"));
        sendKeysSlowly(allPathInputs.get(allPathInputs.size() - 1), location.getPath());
    }

    public void clickAddLocation() {
        clusterBox.findElement(By.xpath("//button[contains(., 'add location')]")).click();
    }

    public void clickDeleteLocation() {
        List<WebElement> buttons = clusterBox
            .findElements(By.xpath("//div[@class='row ng-scope']//button[contains(.,'delete')]"));
        Assert.assertFalse(buttons.isEmpty(), "Delete button should be present.");
        buttons.get(buttons.size() - 1).click();
    }

    public boolean checkElementByContent(String elementTag, String content) {
        List<WebElement> elements = clusterBox.findElements(By.xpath("//" + elementTag));
        for(WebElement element : elements) {
            if (element.getAttribute("value").equals(content)) {
                return true;
            }
        }
        return false;
    }

    public ClusterMerlin getXmlPreview() {
        //preview block fetches changes slower then they appear on the form
        waitForAngularToFinish();
        String previewData = xmlPreview.getAttribute("value");
        return new ClusterMerlin(previewData);
    }

    public void setClusterXml(String clusterXml) {
        clickEditXml(true);
        xmlPreview.clear();
        xmlPreview.sendKeys(clusterXml);
        waitForAngularToFinish();
        clickEditXml(false);
    }

    /**
     * Retrieves hte value of the summary box and parses it to cluster properties.
     * @param draft empty cluster to contain all properties.
     * @return cluster filled with properties from the summary.
     */
    public ClusterMerlin getSummary(ClusterMerlin draft) {
        ClusterMerlin cluster = new ClusterMerlin(draft.toString());
        String summaryBoxText = summaryBox.getText();
        LOGGER.info("Summary block text : " + summaryBoxText);

        String[] slices;
        //retrieve basic properties
        String basicProps = summaryBoxText.split("ACL")[0];
        for (String line : basicProps.split("\\n")) {
            slices = line.split(" ");
            String label = slices[0].replace(":", "").trim();
            String value = slices[1].trim();
            switch (label) {
            case "Name":
                cluster.setName(value);
                break;
            case "Colo":
                cluster.setColo(value);
                break;
            case "Description":
                cluster.setDescription(value);
                break;
            case "Tags":
                cluster.setTags(value);
                break;
            default:
                break;
            }
        }
        //retrieve ALC
        String propsLeft = summaryBoxText.split("ACL")[1];
        String[] acl = propsLeft.split("Interfaces")[0].split(" ");
        cluster.getACL().setOwner(acl[1]);
        cluster.getACL().setGroup(acl[3]);
        cluster.getACL().setPermission(acl[5].trim());

        //retrieve interfaces
        propsLeft = propsLeft.split("Interfaces")[1];
        boolean propertiesPresent = propsLeft.contains("Properties");
        String nextLabel = propertiesPresent ? "Properties" : "Locations";
        String interfaces = propsLeft.split(nextLabel)[0].trim();
        for (String line : interfaces.split("\\n")) {
            slices = line.split(" ");
            String label = slices[0].replace(":", "").trim();
            String endpoint = slices[1].trim();
            String version = slices[3].trim();
            switch (label) {
            case "readonly":
                cluster.addInterface(Interfacetype.READONLY, endpoint, version);
                break;
            case "write":
                cluster.addInterface(Interfacetype.WRITE, endpoint, version);
                break;
            case "execute":
                cluster.addInterface(Interfacetype.EXECUTE, endpoint, version);
                break;
            case "workflow":
                cluster.addInterface(Interfacetype.WORKFLOW, endpoint, version);
                break;
            case "messaging":
                cluster.addInterface(Interfacetype.MESSAGING, endpoint, version);
                break;
            case "registry":
                cluster.addInterface(Interfacetype.REGISTRY, endpoint, version);
                break;
            default:
                break;
            }
        }
        //retrieve properties
        if (propertiesPresent) {
            propsLeft = propsLeft.split("Properties")[1];
            String properties = propsLeft.split("Locations")[0].trim();
            for (String line : properties.split("\\n")) {
                int indx = line.indexOf(":");
                String name = line.substring(0, indx).trim();
                String value = line.substring(indx + 1, line.length()).trim();
                cluster.addProperty(name, value);
            }
        }
        //retrieve locations
        propsLeft = propsLeft.split("Locations")[1].trim();
        for (String line : propsLeft.split("\\n")) {
            slices = line.split(":");
            String label = slices[0].trim();
            String path = slices[1].trim();
            switch (label) {
            case "staging":
                cluster.addLocation(ClusterLocationType.STAGING, path);
                break;
            case "temp":
                cluster.addLocation(ClusterLocationType.TEMP, path);
                break;
            default:
                cluster.addLocation(ClusterLocationType.WORKING, path);
                break;
            }
        }
        return cluster;
    }

    /**
     * Clicks on cancel button.
     */
    public void cancel() {
        cancel.click();
    }

    /**
     * Clicks on editXml button.
     */
    public void clickEditXml(boolean shouldBeEnabled) {
        editXML.click();
        String disabled = xmlPreview.getAttribute("disabled");
        Assert.assertEquals(disabled == null, shouldBeEnabled,
            "Xml preview should be " + (shouldBeEnabled ? "enabled" : "disabled"));
    }

    /**
     *  Click on next button which is the same as finish step 1.
     */
    public void clickNext() {
        next.click();
        Assert.assertTrue(summaryBox.isDisplayed(), "Summary box should be displayed.");
    }

    /**
     * Click on save button.
     */
    public void clickSave() {
        save.click();
        waitForAlert();
    }

    /**
     * Clicks on previous button.
     */
    public void clickPrevious() {
        previous.click();
        UIAssert.assertDisplayed(clusterBox, "Cluster box");
    }

    public void checkRegistry() {
        clusterBox.findElement(By.xpath("//input[@type='checkbox']")).click();
    }

    public String getInterfaceEndpoint(Interfacetype interfacetype) {
        String xpath = String.format("(//input[@ng-model='_interface._endpoint'])[%s]", interfacetype.ordinal() + 1);
        WebElement endpoint = clusterBox.findElement(By.xpath(xpath));
        return endpoint.getAttribute("value");
    }

    public String getInterfaceVersion(Interfacetype interfacetype) {
        String xpath = String.format("(//input[@ng-model='_interface._version'])[%s]", interfacetype.ordinal() + 1);
        WebElement version = clusterBox.findElement(By.xpath(xpath));
        return version.getAttribute("value");
    }

    /**
     * Checks whether registry interface is enabled for input or not.
     */
    public boolean isRegistryEnabled() {
        WebElement endpoint = clusterBox.findElement(By.xpath("(//input[@ng-model='_interface._endpoint'])[6]"));
        WebElement version = clusterBox.findElement(By.xpath("(//input[@ng-model='_interface._version'])[6]"));
        return endpoint.isEnabled() && version.isEnabled();
    }

    private WebElement getNameUnavailable(){
        return clusterBox.findElement(By.xpath(
            "//div[contains(@class, 'nameInputDisplay') and contains(@class, 'custom-danger')]"));
    }

    public void checkNameUnavailableDisplayed(boolean isDisplayed) {
        if (isDisplayed){
            UIAssert.assertDisplayed(getNameUnavailable(), "Name Unavailable not displayed");
        }else {
            try{
                getNameUnavailable();
                Assert.fail("Name Unavailable found");
            } catch (Exception ex){
                LOGGER.info("Name Unavailable not found");
            }
        }
    }

}

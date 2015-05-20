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
package org.apache.falcon.regression.searchUI;

import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.ClusterWizardPage;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Tests for cluster setup page.
 */
public class ClusterSetupTest extends BaseUITestClass{
    private ClusterWizardPage clusterSetup = null;
    private ColoHelper cluster = servers.get(0);
    private ClusterMerlin sourceCluster;

    @BeforeClass(alwaysRun = true)
    public void prepareCluster() throws IOException {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        openBrowser();
        SearchPage homePage = LoginPage.open(getDriver()).doDefaultLogin();
        clusterSetup = homePage.getPageHeader().doCreateCluster();
        clusterSetup.checkPage();
        sourceCluster = bundles[0].getClusterElement();
        //add custom cluster properties
        sourceCluster.setTags("myTag1=myValue1");
        sourceCluster.setDescription("description");
    }

    @Test
    public void testHeader() {
        clusterSetup.getPageHeader().checkHeader();
    }

    /**
     * Default cluster creation scenario. Populate fields with valid values. Click next. Return back and click
     * next again. Check that all values are present on Summary page. Save cluster.
     * Check the cluster definition trough /definition API.
     */
    @Test
    public void testDefaultScenario()
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        clusterSetup.fillForm(sourceCluster);
        clusterSetup.clickNext();
        clusterSetup.clickPrevious();
        clusterSetup.clickNext();
        ClusterMerlin summaryBlock = clusterSetup.getSummary(sourceCluster.getEmptyCluster());
        //summary block should contain the same info as source
        sourceCluster.assertEquals(summaryBlock);
        clusterSetup.clickSave();
        String alertText = clusterSetup.getActiveAlertText();
        Assert.assertEquals(alertText, "falcon/default/Submit successful (cluster) " + sourceCluster.getName());
        ClusterMerlin definition = new ClusterMerlin(cluster.getClusterHelper()
            .getEntityDefinition(bundles[0].getClusterElement().toString()).getMessage());
        //definition should be the same that the source
        sourceCluster.assertEquals(definition);
    }

    /**
     * Populate fields with valid values. Check that changes are reflected on XMLPreview block. Click next.
     * Check that XML is what we have populated on the previous step.
     */
    @Test
    public void testXmlPreview() {
        clusterSetup.fillForm(sourceCluster);
        ClusterMerlin generalStepPreview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(generalStepPreview);
        sourceCluster.assertEquals(generalStepPreview);
        clusterSetup.clickNext();
        ClusterMerlin summaryStepPreview = clusterSetup.getXmlPreview();
        sourceCluster.assertEquals(summaryStepPreview);
        generalStepPreview.assertEquals(summaryStepPreview);
    }

    private void cleanGeneralPreview(ClusterMerlin clusterMerlin) {
        //On general step xml preview has extra empty values which should be removed to compare data.
        List<Location> locations = clusterMerlin.getLocations().getLocations();
        int last = locations.size() - 1;
        if (locations.get(last).getName() == null && locations.get(last).getPath().isEmpty()) {
            locations.remove(last);
        }
        List<Interface> interfaces = clusterMerlin.getInterfaces().getInterfaces();
        last = interfaces.size() - 1;
        if (interfaces.get(last).getEndpoint().isEmpty() && interfaces.get(last).getVersion().isEmpty()) {
            interfaces.remove(last);
        }
        List<Property> properties = clusterMerlin.getProperties().getProperties();
        last = properties.size() - 1;
        if (properties.get(last).getName().isEmpty() && properties.get(last).getValue().isEmpty()) {
            properties.remove(last);
        }
    }

    /**
     * Add location to cluster. Check that it is present. Check XML preview has it.
     * Delete the location. Check that it has been deleted from wizard window.
     */
    @Test
    public void testAddDeleteLocation() {
        //to make addLocation button enabled
        sourceCluster.addLocation(ClusterLocationType.WORKING, "/one-another-temp");
        clusterSetup.fillForm(sourceCluster);

        //check without extra location
        ClusterMerlin preview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(preview);
        sourceCluster.assertEquals(preview);

        //add one more location to the form and check results
        String path = "/one-extra-working";
        Location location = new Location();
        location.setName(ClusterLocationType.WORKING);
        location.setPath(path);
        clusterSetup.clickAddLocation();
        clusterSetup.fillAdditionalLocation(location);
        Assert.assertTrue(clusterSetup.checkElementByContent("input", path), "Location should be present.");
        preview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(preview);
        //add location to source to compare equality
        sourceCluster.addLocation(ClusterLocationType.WORKING, path);
        sourceCluster.assertEquals(preview);

        //delete location and check results
        clusterSetup.clickDeleteLocation();
        Assert.assertFalse(clusterSetup.checkElementByContent("input", path), "Location should be absent.");
        preview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(preview);
        //remove location from source to check equality
        int last = sourceCluster.getLocations().getLocations().size() - 1;
        sourceCluster.getLocations().getLocations().remove(last);
        sourceCluster.assertEquals(preview);
    }

    /**
     * Add tag to cluster. Check that it is present. Check XML preview has it.
     * Delete the tag. Check that it has been deleted from wizard window.
     */
    @Test
    public void testAddDeleteTag() {
        clusterSetup.fillForm(sourceCluster);

        //check without extra tag
        ClusterMerlin preview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(preview);
        sourceCluster.assertEquals(preview);

        //add one more tag to the form and check results
        clusterSetup.clickAddTag();
        clusterSetup.addTag("myTag2", "myValue2");
        Assert.assertTrue(clusterSetup.checkElementByContent("input", "myTag2"), "Tag should be present");
        Assert.assertTrue(clusterSetup.checkElementByContent("input", "myValue2"), "Tag should be present");
        preview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(preview);
        //add tag to source to compare equality
        sourceCluster.setTags("myTag1=myValue1,myTag2=myValue2");
        sourceCluster.assertEquals(preview);

        //delete location and check results
        clusterSetup.clickDeleteTag();
        Assert.assertFalse(clusterSetup.checkElementByContent("input", "myTag2"), "Tag should be absent.");
        Assert.assertFalse(clusterSetup.checkElementByContent("input", "myValue2"), "Tag should be absent.");
        preview = clusterSetup.getXmlPreview();
        cleanGeneralPreview(preview);
        //remove location from source to check equality
        sourceCluster.setTags("myTag1=myValue1");
        sourceCluster.assertEquals(preview);
    }

    /**
     * Check that staging interface is unavailable by default but becomes available when we set matching checkbox.
     */
    @Test
    public void testRegistryInterface() {
        Assert.assertFalse(clusterSetup.isRegistryEnabled(), "Registry should be disabled.");
        clusterSetup.checkRegistry();
        Assert.assertTrue(clusterSetup.isRegistryEnabled(), "Registry should be enabled.");
        clusterSetup.checkRegistry();
        Assert.assertFalse(clusterSetup.isRegistryEnabled(), "Registry should be disabled again.");
    }

    /**
     * Populate working location with value pointing to directory with wider permissions then 755.
     * Check that user is not allowed to create a cluster and is notified with an alert.
     */
    @Test
    public void testLocationsBadPermissions() throws IOException {
        //reverse staging and working location dirs
        String staging = sourceCluster.getLocation(ClusterLocationType.STAGING).get(0).getPath();
        String working = sourceCluster.getLocation(ClusterLocationType.WORKING).get(0).getPath();
        sourceCluster.getLocation(ClusterLocationType.WORKING).get(0).setPath(staging);
        sourceCluster.getLocation(ClusterLocationType.STAGING).get(0).setPath(working);
        clusterSetup.fillForm(sourceCluster);
        clusterSetup.clickNext();
        clusterSetup.clickSave();
        String alertMessage = clusterSetup.getActiveAlertText();
        Assert.assertEquals(alertMessage,
            String.format("Path %s has permissions: rwxrwxrwx, should be rwxr-xr-x", staging));
    }

    /**
     * Provide locations which are formally correct but don't exist.
     * Check that user can't create cluster and has been notified with an alert.
     */
    @Test
    public void testLocationsNonExistent() throws IOException {
        String nonExistent = "/non-existent-directory";
        sourceCluster.getLocation(ClusterLocationType.STAGING).get(0).setPath(nonExistent);
        clusterSetup.fillForm(sourceCluster);
        clusterSetup.clickNext();
        clusterSetup.clickSave();
        String alertMessage = clusterSetup.getActiveAlertText();
        Assert.assertEquals(alertMessage,
            String.format("Location %s for cluster %s must exist.", nonExistent, sourceCluster.getName()));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        closeBrowser();
    }
}

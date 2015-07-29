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

import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.MirrorWizardPage;
import org.apache.falcon.regression.ui.search.MirrorWizardPage.Location;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.falcon.resource.EntityList;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.EnumSet;
import java.util.Set;
import java.util.TreeSet;

/** UI tests for mirror creation. */
@Test(groups = "search-ui")
public class MirrorSourceTargetOptionsTest extends BaseUITestClass{
    private final ColoHelper cluster = servers.get(0);
    private SearchPage searchPage;
    private MirrorWizardPage mirrorPage;
    private MirrorWizardPage.ClusterBlock source;
    private MirrorWizardPage.ClusterBlock target;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        openBrowser();
        searchPage = LoginPage.open(getDriver()).doDefaultLogin();
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(cluster);

    }

    @BeforeMethod(alwaysRun = true)
    public void refreshMirrorPage() throws Exception {
        searchPage.refresh();
        mirrorPage = searchPage.getPageHeader().doCreateMirror();
        source = mirrorPage.getSourceBlock();
        target = mirrorPage.getTargetBlock();
    }


    @Test
    public void testExclusiveWhereToRunJob() {
        source.selectRunHere();
        target.selectRunHere();
        Assert.assertFalse(source.isRunHereSelected(), "'Run job here' shouldn't be selected on Source");
        Assert.assertTrue(target.isRunHereSelected(), "'Run job here' should be selected on Target");

        source.selectRunHere();
        Assert.assertTrue(source.isRunHereSelected(), "'Run job here' should be selected on Source");
        Assert.assertFalse(target.isRunHereSelected(), "'Run job here' shouldn't be selected on Target");

        mirrorPage.setMirrorType(FalconCLI.RecipeOperation.HIVE_DISASTER_RECOVERY);

        target.selectRunHere();
        Assert.assertFalse(source.isRunHereSelected(), "'Run job here' shouldn't be selected on Source");
        Assert.assertTrue(target.isRunHereSelected(), "'Run job here' should be selected on Target");

        source.selectRunHere();
        Assert.assertTrue(source.isRunHereSelected(), "'Run job here' should be selected on Source");
        Assert.assertFalse(target.isRunHereSelected(), "'Run job here' shouldn't be selected on Target");

        mirrorPage.setMirrorType(FalconCLI.RecipeOperation.HDFS_REPLICATION);
        source.setLocationType(Location.AZURE);
        Assert.assertFalse(source.isRunHereAvailable(),
                "'Run job here' shouldn't be available on source if Source=Azure");

        source.setLocationType(Location.S3);
        Assert.assertFalse(source.isRunHereAvailable(),
                "'Run job here' shouldn't be available on source if Source=S3");

        source.setLocationType(Location.HDFS);
        target.setLocationType(Location.AZURE);
        Assert.assertFalse(target.isRunHereAvailable(),
                "'Run job here' shouldn't be available on target if Target=Azure");

        target.setLocationType(Location.S3);
        Assert.assertFalse(target.isRunHereAvailable(),
                "'Run job here' shouldn't be available on target if Target=S3");

    }

    @Test
    public void testExclusiveFSOptions() {
        source.setLocationType(Location.HDFS);
        Assert.assertEquals(target.getAvailableLocationTypes(),
                EnumSet.allOf(Location.class), "All target types should be available if source=HDFS");


        source.setLocationType(Location.AZURE);
        Assert.assertEquals(target.getAvailableLocationTypes(),
                EnumSet.of(Location.HDFS), "Only HDFS should be available as target if source=Azure");

        source.setLocationType(Location.S3);
        Assert.assertEquals(target.getAvailableLocationTypes(),
                EnumSet.of(Location.HDFS), "Only HDFS should be available as target if source=S3");

        source.setLocationType(Location.HDFS);
        target.setLocationType(Location.HDFS);
        Assert.assertEquals(target.getAvailableLocationTypes(),
                EnumSet.allOf(Location.class), "All source types should be available if target=HDFS");


        target.setLocationType(Location.AZURE);
        Assert.assertEquals(source.getAvailableLocationTypes(),
                EnumSet.of(Location.HDFS), "Only HDFS should be available as source if target=Azure");

        target.setLocationType(Location.S3);
        Assert.assertEquals(source.getAvailableLocationTypes(),
                EnumSet.of(Location.HDFS), "Only HDFS should be available as source if target=S3");
    }

    @Test
    public void testClustersDropDownList() throws Exception {
        //add more clusters
        ClusterMerlin clusterMerlin = bundles[0].getClusterElement();
        String clusterName = clusterMerlin.getName() + '-';
        for (int i = 0; i < 5; i++) {
            clusterMerlin.setName(clusterName + i);
            prism.getClusterHelper().submitEntity(clusterMerlin.toString());
        }
        EntityList result =
            prism.getClusterHelper().listAllEntities().getEntityList();
        Assert.assertNotNull(result.getElements(),
            "There should be more than 5 clusters in result");
        Set<String> apiClusterNames = new TreeSet<>();
        for (EntityList.EntityElement element : result.getElements()) {
            apiClusterNames.add(element.name);
        }

        //refresh page to get new clusters on UI
        refreshMirrorPage();

        mirrorPage.setMirrorType(FalconCLI.RecipeOperation.HDFS_REPLICATION);
        source.setLocationType(Location.HDFS);
        target.setLocationType(Location.HDFS);

        Assert.assertEquals(source.getAvailableClusters(), apiClusterNames,
            "Clusters available via API are not the same as on Source for HDFS replication");
        Assert.assertEquals(target.getAvailableClusters(), apiClusterNames,
            "Clusters available via API are not the same as on Target for HDFS replication");

        mirrorPage.setMirrorType(FalconCLI.RecipeOperation.HIVE_DISASTER_RECOVERY);

        Assert.assertEquals(source.getAvailableClusters(), apiClusterNames,
            "Clusters available via API are not the same as on Source for HIVE replication");
        Assert.assertEquals(target.getAvailableClusters(), apiClusterNames,
            "Clusters available via API are not the same as on Target for HIVE replication");
    }

    @Test
    public void testInvalidValidity() {
        mirrorPage.setName(bundles[0].getProcessName());
        mirrorPage.setMirrorType(FalconCLI.RecipeOperation.HDFS_REPLICATION);
        String baseTestDir = cleanAndGetTestDir();
        source.setPath(baseTestDir);
        source.selectCluster(bundles[0].getClusterNames().get(0));
        target.setPath(baseTestDir);
        target.selectCluster(bundles[0].getClusterNames().get(0));

        mirrorPage.setStartTime("2010-01-01T02:00Z");
        mirrorPage.setEndTime("2010-01-01T01:00Z");
        mirrorPage.next();
        mirrorPage.save();
        Assert.assertTrue(mirrorPage.getActiveAlertText().contains("should be before process end"),
            "Warning about wrong Validity should be present");
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() {
        removeTestClassEntities();
        closeBrowser();
    }

}

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

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.MirrorWizardPage;
import org.apache.falcon.regression.ui.search.MirrorWizardPage.Summary;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.EnumMap;
import java.util.Map;


/** UI tests for mirror creation. */
@Test(groups = "search-ui")
public class MirrorSummaryTest extends BaseUITestClass{
    private static final Logger LOGGER = Logger.getLogger(MirrorSummaryTest.class);

    private final ColoHelper cluster = servers.get(0);
    private SearchPage searchPage;
    private MirrorWizardPage mirrorPage;
    private String baseTestDir = cleanAndGetTestDir();
    private String start = "2010-01-01T01:00Z";
    private String end = "2010-01-01T02:00Z";
    private Map<Summary, String> baseMap;

    @BeforeClass(alwaysRun = true)
    public void setupClass() throws Exception {
        baseMap = new EnumMap<>(Summary.class);
        baseMap.put(Summary.MAX_MAPS, "5");
        baseMap.put(Summary.MAX_BANDWIDTH, "100");
        baseMap.put(Summary.ACL_OWNER, LoginPage.UI_DEFAULT_USER);
        baseMap.put(Summary.ACL_GROUP, "users");
        baseMap.put(Summary.ACL_PERMISSIONS, "0x755");
        baseMap.put(Summary.RETRY_POLICY, "periodic");
        baseMap.put(Summary.RETRY_DELAY, "30 minutes");
        baseMap.put(Summary.RETRY_ATTEMPTS, "3");
        baseMap.put(Summary.FREQUENCY, "5 minutes");
        baseMap.put(Summary.SOURCE_PATH, baseTestDir);
        baseMap.put(Summary.TARGET_PATH, baseTestDir);
        baseMap.put(Summary.START, start);
        baseMap.put(Summary.END, end);

        //HDFS is default mirror type
        baseMap.put(Summary.TYPE, "HDFS");
        baseMap.put(Summary.TAGS, "_falcon_mirroring_type - HDFS");
        baseMap.put(Summary.SOURCE_LOCATION, "HDFS");
        baseMap.put(Summary.TARGET_LOCATION, "HDFS");

        openBrowser();
        searchPage = LoginPage.open(getDriver()).doDefaultLogin();
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        removeTestClassEntities();
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(cluster);
        searchPage.refresh();
        mirrorPage = searchPage.getPageHeader().doCreateMirror();
        MirrorWizardPage.ClusterBlock source = mirrorPage.getSourceBlock();
        MirrorWizardPage.ClusterBlock target = mirrorPage.getTargetBlock();
        String clusterName = bundles[0].getClusterNames().get(0);
        String mirrorName = bundles[0].getProcessName();

        baseMap.put(Summary.RUN_ON, clusterName);
        baseMap.put(Summary.NAME, mirrorName);
        baseMap.put(Summary.SOURCE_CLUSTER, clusterName);
        baseMap.put(Summary.TARGET_CLUSTER, clusterName);

        mirrorPage.setName(mirrorName);

        source.setPath(baseTestDir);
        source.selectCluster(clusterName);
        target.setPath(baseTestDir);
        target.selectCluster(clusterName);

        mirrorPage.setStartTime(start);
        mirrorPage.setEndTime(end);

    }

    @Test
    public void testSummaryDefaultScenario() {
        mirrorPage.next();

        Map<Summary, String> actualParams = mirrorPage.getSummaryProperties();


        LOGGER.info("Actual parameters: " + actualParams);
        LOGGER.info("Expected parameters: " + baseMap);

        Assert.assertEquals(actualParams, baseMap);

        mirrorPage.save();
        Assert.assertTrue(mirrorPage.getActiveAlertText().contains("Submit successful"),
            "Submit should be successful");
    }

    @Test
    public void testModificationOnPreviousStep() {
        mirrorPage.next();

        Map<Summary, String> actualParams = mirrorPage.getSummaryProperties();

        LOGGER.info("Actual parameters: " + actualParams);
        LOGGER.info("Expected parameters: " + baseMap);

        Assert.assertEquals(actualParams, baseMap);

        mirrorPage.previous();

        String newPath = baseTestDir + "/new";
        mirrorPage.getTargetBlock().setPath(newPath);

        Map<Summary, String> expectedParams = new EnumMap<>(baseMap);
        expectedParams.put(Summary.TARGET_PATH, newPath);

        LOGGER.info("Target path set to " + newPath);

        mirrorPage.next();

        Assert.assertEquals(mirrorPage.getSummaryProperties(), expectedParams);


    }


    @Test
    public void testAdvancedScenario() {

        mirrorPage.toggleAdvancedOptions();
        mirrorPage.setHdfsDistCpMaxMaps("9");
        mirrorPage.setHdfsMaxBandwidth("50");
        mirrorPage.setAclOwner("somebody");
        mirrorPage.setAclGroup("somegroup");
        mirrorPage.setAclPermission("0x000");
        mirrorPage.setFrequency(new Frequency("8", Frequency.TimeUnit.hours));
        Retry retry = new Retry();
        retry.setAttempts(8);
        retry.setPolicy(PolicyType.FINAL);
        retry.setDelay(new Frequency("13", Frequency.TimeUnit.days));
        mirrorPage.setRetry(retry);


        mirrorPage.next();

        Map<Summary, String> actualParams = mirrorPage.getSummaryProperties();
        Map<Summary, String> expectedParams = new EnumMap<>(baseMap);
        expectedParams.put(Summary.ACL_OWNER, "somebody");
        expectedParams.put(Summary.ACL_GROUP, "somegroup");
        expectedParams.put(Summary.ACL_PERMISSIONS, "0x000");
        expectedParams.put(Summary.MAX_MAPS, "9");
        expectedParams.put(Summary.MAX_BANDWIDTH, "50");
        expectedParams.put(Summary.FREQUENCY, "8 hours");
        expectedParams.put(Summary.RETRY_ATTEMPTS, "8");
        expectedParams.put(Summary.RETRY_POLICY, "final");
        expectedParams.put(Summary.RETRY_DELAY, "13 days");


        LOGGER.info("Actual parameters: " + actualParams);
        LOGGER.info("Expected parameters: " + expectedParams);

        Assert.assertEquals(actualParams, expectedParams);


    }


    @AfterClass(alwaysRun = true)
    public void tearDownClass() {
        removeTestClassEntities();
        closeBrowser();
    }

}

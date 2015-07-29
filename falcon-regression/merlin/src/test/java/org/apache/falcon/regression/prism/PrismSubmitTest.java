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

package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.ConnectException;
import java.util.List;

/**
 * Submit tests with prism.
 */
@Test(groups = "distributed")
public class PrismSubmitTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private String baseTestDir = cleanAndGetTestDir();
    private String randomHDFSPath = baseTestDir + "/someRandomPath";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private boolean restartRequired;
    private static final Logger LOGGER = Logger.getLogger(PrismSubmitTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        restartRequired = false;
        bundles[0] = new Bundle(BundleUtil.readELBundle(), cluster1);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.startService(prism.getFeedHelper());
            Util.startService(cluster1.getFeedHelper());
        }
        removeTestClassEntities();
    }

    @Test(groups = "distributed")
    public void submitCluster1Prism1ColoPrismDown() throws Exception {
        restartRequired = true;
        Util.shutDownService(prism.getClusterHelper());

        List<String> beforeSubmit = cluster1.getClusterHelper().getStoreInfo();
        try {
            prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        } catch (ConnectException e) {
            Assert.assertTrue(e.getMessage().contains("Connection to "
                + prism.getClusterHelper().getHostname() + " refused"), e.getMessage());
        }
        List<String> afterSubmit = cluster1.getClusterHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmit, afterSubmit, 0);

    }

    @Test(groups = "distributed")
    public void submitClusterReSubmitDiffContent() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();

        bundles[0].setCLusterWorkingPath(bundles[0].getClusters().get(0), randomHDFSPath);
        LOGGER.info("modified cluster Data: "
            + Util.prettyPrintXml(bundles[0].getClusters().get(0)));
        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test(groups = "distributed")
    public void submitClusterResubmitAlreadyPartialWithAllUp() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);

        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        Util.startService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);

        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
    }

    @Test(groups = "distributed")
    public void submitProcess1ColoDownAfter2FeedSubmitStartAfterProcessSubmitAnsDeleteProcess()
        throws Exception {
        restartRequired = true;
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(bundles[0].getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(bundles[0].getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        Util.shutDownService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(12);


        List<String> beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));

        AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(bundles[0].getProcessData()));
        AssertUtil.assertFailed(r);
        List<String> afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, bundles[0].getProcessName(), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        Util.startService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(15);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().delete(bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, bundles[0].getProcessName(), -1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test(groups = "distributed")
    public void submitProcessIdeal() throws Exception {

        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> beforeSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getFeedHelper().getStoreInfo();

        r = prism.getFeedHelper().submitEntity(bundles[0].getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getFeedHelper().submitEntity(bundles[0].getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getFeedHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 2);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 2);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().submitEntity(bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, bundles[0].getProcessName(), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, bundles[0].getProcessName(), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

    }

    @Test(groups = "distributed")
    public void submitCluster1Prism1ColoColoDown() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());

        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));


        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,

            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);

        Util.startService(cluster1.getClusterHelper());

        TimeUtil.sleepSeconds(10);

        beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        //should be succeeded
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster1Prism1ColoSubmitDeleted() throws Exception {
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        prism.getClusterHelper().delete(bundles[0].getClusters().get(0));

        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
    }

    @Test(groups = "embedded")
    public void submitProcessWoClusterSubmit() throws Exception {
        ServiceResponse r = prism.getProcessHelper().submitEntity(bundles[0].getProcessData());

        Assert.assertTrue(r.getMessage().contains("FAILED"));
        Assert.assertTrue(r.getMessage().contains("is not registered"));
    }

    @Test(groups = "embedded")
    public void submitProcessWoFeedSubmit() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        r = prism.getProcessHelper().submitEntity(bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("FAILED"));
        Assert.assertTrue(r.getMessage().contains("is not registered"));
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void submitClusterReSubmitAlreadyPartial() throws Exception {
        restartRequired = true;
        bundles[1] = new Bundle(BundleUtil.readELBundle(), cluster2);
        bundles[1].generateUniqueBundle(this);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);

        List<String> beforeCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforePrism = prism.getClusterHelper().getStoreInfo();
        List<String> beforeCluster2 = cluster2.getClusterHelper().getStoreInfo();

        Util.shutDownService(cluster1.getFeedHelper());

        bundles[1].setCLusterColo(cluster2.getClusterHelper().getColoName());
        LOGGER.info("cluster b2: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> parCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> parPrism = prism.getClusterHelper().getStoreInfo();
        List<String> parCluster2 = cluster2.getClusterHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(parCluster1, beforeCluster1, 0);
        AssertUtil.compareDataStoreStates(beforePrism, parPrism,
            Util.readEntityName(bundles[1].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeCluster2, parCluster2,
            Util.readEntityName(bundles[1].getClusters().get(0)), 1);

        Util.restartService(cluster1.getFeedHelper());

        bundles[0].setCLusterColo(cluster1.getClusterHelper().getColoName());
        LOGGER.info("cluster b1: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));
        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterPrism = prism.getClusterHelper().getStoreInfo();
        List<String> afterCluster2 = cluster2.getClusterHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(parCluster1, afterCluster1,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(afterPrism, parPrism, 0);
        AssertUtil.compareDataStoreStates(afterCluster2, parCluster2, 0);
    }

    @Test(groups = "distributed")
    public void submitClusterPolarization() throws Exception {
        restartRequired = true;
        //shutdown one colo and submit
        Util.shutDownService(cluster1.getClusterHelper());
        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);

        //resubmit PARTIAL success
        Util.startService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);
        beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test(groups = "distributed")
    public void submitClusterResubmitDiffContentPartial() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        Util.startService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        bundles[0].setCLusterWorkingPath(bundles[0].getClusters().get(0), randomHDFSPath);
        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test
    public void submitClusterPartialDeletedOfPartialSubmit() throws Exception {
        restartRequired = true;
        Util.shutDownService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().delete(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getClusters().get(0)), -1);

        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
    }

    @Test(groups = "distributed")
    public void submitClusterSubmitPartialDeleted() throws Exception {
        restartRequired = true;
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        TimeUtil.sleepSeconds(30);

        Util.shutDownService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().delete(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("PARTIAL"));
        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getClusters().get(0)), -1);

        Util.startService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);

        beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
    }

    @Test(groups = "embedded")
    public void submitClusterReSubmitAlreadySucceeded() throws Exception {
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> beforeSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        List<String> afterSubmitCluster = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = cluster2.getClusterHelper().getStoreInfo();
        AssertUtil.compareDataStoreStates(beforeSubmitCluster, afterSubmitCluster, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
    }

    @Test(groups = "distributed")
    public void submitCluster1Prism1ColoAllUp() throws Exception {
        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2,
            Util.readEntityName(bundles[0].getClusters().get(0)), 1);
    }

    @Test(groups = "embedded")
    public void submitCluster1Prism1ColoAlreadySubmitted() throws Exception {
        prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        List<String> beforeSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getClusterHelper().getStoreInfo();

        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));

        List<String> afterSubmitCluster1 = cluster1.getClusterHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getClusterHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getClusterHelper().getStoreInfo();

        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }

    @Test
    public void submitProcess1ColoDownAfter1FeedSubmitStartAfter2feed() throws Exception {
        restartRequired = true;
        ServiceResponse r = prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());

        r = prism.getFeedHelper().submitEntity(bundles[0].getDataSets().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"), r.getMessage());

        Util.shutDownService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(30);

        List<String> beforeSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> beforeSubmitPrism = prism.getFeedHelper().getStoreInfo();

        r = prism.getFeedHelper().submitEntity(bundles[0].getDataSets().get(1));
        Assert.assertTrue(r.getMessage().contains("FAILED"));

        List<String> afterSubmitCluster1 = cluster1.getFeedHelper().getStoreInfo();
        List<String> afterSubmitCluster2 = cluster2.getFeedHelper().getStoreInfo();
        List<String> afterSubmitPrism = prism.getFeedHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism,
            Util.readEntityName(bundles[0].getDataSets().get(1)), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);

        Util.startService(cluster1.getClusterHelper());
        TimeUtil.sleepSeconds(15);

        beforeSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        beforeSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        beforeSubmitPrism = prism.getProcessHelper().getStoreInfo();

        r = prism.getProcessHelper().submitEntity(bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("FAILED"), r.getMessage());

        afterSubmitCluster1 = cluster1.getProcessHelper().getStoreInfo();
        afterSubmitCluster2 = cluster2.getProcessHelper().getStoreInfo();
        afterSubmitPrism = prism.getProcessHelper().getStoreInfo();

        AssertUtil.compareDataStoreStates(beforeSubmitCluster1, afterSubmitCluster1, 0);
        AssertUtil.compareDataStoreStates(beforeSubmitPrism, afterSubmitPrism, bundles[0].getProcessName(), 1);
        AssertUtil.compareDataStoreStates(beforeSubmitCluster2, afterSubmitCluster2, 0);
    }
}

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
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Test(groups = "distributed")
public class PrismFeedDeleteTest extends BaseTestClass {

    private boolean restartRequired;
    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    private String cluster1Colo = cluster1.getClusterHelper().getColoName();
    private String cluster2Colo = cluster2.getClusterHelper().getColoName();
    String aggregateWorkflowDir = baseHDFSDir + "/PrismFeedDeleteTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedDeleteTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);

        bundles[1] = new Bundle(bundle, cluster2);
        bundles[1].generateUniqueBundle();
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(cluster1.getFeedHelper());
        }
        removeBundles();
    }

    /**
     * NOTE: All test cases assume that there are two entities scheduled in each colo
     */

    @Test(groups = {"multiCluster"})
    public void testServer1FeedDeleteInBothColos() throws Exception {
        bundles[0].submitFeed();
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String feedName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, feedName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, feedName);

        //server1:
        compareDataStoreStates(initialServer1Store, finalServer1Store, feedName);
        compareDataStoreStates(finalServer1ArchiveStore, initialServer1ArchiveStore, feedName);

        //server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);

    }

    @Test(groups = {"multiCluster"})
    public void testServer1FeedDeleteWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        bundles[0].submitFeed();
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();


        //bring down Server2 colo :P
        Util.shutDownService(cluster1.getFeedHelper());

        //lets now delete the cluster from both colos
        AssertUtil.assertFailed(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);

        Util.startService(cluster1.getFeedHelper());

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        List<String> Server2ArchivePostUp = cluster2.getFeedHelper().getArchiveInfo();
        List<String> Server2StorePostUp = cluster2.getFeedHelper().getStoreInfo();

        List<String> Server1ArchivePostUp = cluster1.getFeedHelper().getArchiveInfo();
        List<String> Server1StorePostUp = cluster1.getFeedHelper().getStoreInfo();

        List<String> prismHelperArchivePostUp = prism.getFeedHelper().getArchiveInfo();
        List<String> prismHelperStorePostUp = prism.getFeedHelper().getStoreInfo();

        compareDataStoreStates(finalPrismStore, prismHelperStorePostUp, clusterName);
        compareDataStoreStates(prismHelperArchivePostUp, finalPrismArchiveStore, clusterName);

        compareDataStoreStates(initialServer1Store, Server1StorePostUp, clusterName);
        compareDataStoreStates(Server1ArchivePostUp, finalServer1ArchiveStore, clusterName);

        compareDataStoresForEquality(finalServer2Store, Server2StorePostUp);
        compareDataStoresForEquality(finalServer2ArchiveStore, Server2ArchivePostUp);
    }


    @Test(groups = {"multiCluster"})
    public void testServer1FeedDeleteAlreadyDeletedFeed() throws Exception {
        restartRequired = true;
        bundles[0].submitFeed();
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(initialServer2ArchiveStore, finalServer2ArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);
    }


    @Test(groups = {"multiCluster"})
    public void testServer1FeedDeleteTwiceWhen1ColoIsDownDuring1stDelete() throws Exception {
        restartRequired = true;

        bundles[0].submitFeed();

        Util.shutDownService(cluster1.getClusterHelper());


        //lets now delete the cluster from both colos
        AssertUtil.assertFailed(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //start up service
        Util.startService(cluster1.getFeedHelper());

        //delete again
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //get final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));

        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(initialServer2ArchiveStore, finalServer2ArchiveStore);

        //Server1:
        compareDataStoreStates(initialServer1Store, finalServer1Store, clusterName);
        compareDataStoreStates(finalServer1ArchiveStore, initialServer1ArchiveStore, clusterName);
    }

    @Test(groups = {"multiCluster"})
    public void testServer1FeedDeleteNonExistent() throws Exception {
        //now lets get the final states
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //get final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(initialServer2ArchiveStore, finalServer2ArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);
    }


    @Test(groups = {"multiCluster"})
    public void testServer1FeedDeleteNonExistentWhen1ColoIsDownDuringDelete() throws Exception {
        restartRequired = true;
        bundles[0] = new Bundle(bundles[0], cluster1);
        bundles[1] = new Bundle(bundles[1], cluster2);

        bundles[0].setCLusterColo(cluster1Colo);
        logger.info("cluster bundle1: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));

        ServiceResponse r = prism.getClusterHelper()
            .submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundles[1].setCLusterColo(cluster2Colo);
        logger.info("cluster bundle2: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeServer1 = "2012-10-01T12:00Z";
        String startTimeServer2 = "2012-10-01T12:00Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeServer1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                baseHDFSDir + "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeServer2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                baseHDFSDir +
                    "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        Util.shutDownService(cluster1.getFeedHelper());

        AssertUtil.assertSucceeded(prism.getFeedHelper().delete(Util.URLS.DELETE_URL, feed));
    }


    @Test(groups = {"multiCluster"})
    public void testDeleteFeedScheduledInOneColo() throws Exception {
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //Server1:
        compareDataStoreStates(initialServer1Store, finalServer1Store, clusterName);
        compareDataStoreStates(finalServer1ArchiveStore, initialServer1ArchiveStore, clusterName);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);


    }

    @Test(groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColo() throws Exception {
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //suspend Server1 colo thingy
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //Server1:
        compareDataStoreStates(initialServer1Store, finalServer1Store, clusterName);
        compareDataStoreStates(finalServer1ArchiveStore, initialServer1ArchiveStore, clusterName);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);


    }


    @Test(groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileBothFeedsAreSuspended() throws Exception {
        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //suspend Server1 colo thingy
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().suspend(URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //Server1:
        compareDataStoreStates(initialServer1Store, finalServer1Store, clusterName);
        compareDataStoreStates(finalServer1ArchiveStore, initialServer1ArchiveStore, clusterName);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);
    }

    @Test(groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileThatColoIsDown()
        throws Exception {
        restartRequired = true;

        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //shutdown Server1
        Util.shutDownService(cluster1.getFeedHelper());

        //lets now delete the cluster from both colos
        AssertUtil.assertFailed(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);

        Util.startService(cluster1.getClusterHelper());

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        List<String> Server1StorePostUp = cluster1.getFeedHelper().getStoreInfo();
        List<String> Server1ArchivePostUp = cluster1.getFeedHelper().getArchiveInfo();

        List<String> Server2StorePostUp = cluster2.getFeedHelper().getStoreInfo();
        List<String> Server2ArchivePostUp = cluster2.getFeedHelper().getArchiveInfo();

        List<String> prismStorePostUp = prism.getFeedHelper().getStoreInfo();
        List<String> prismArchivePostUp = prism.getFeedHelper().getArchiveInfo();


        compareDataStoresForEquality(Server2StorePostUp, finalServer2Store);
        compareDataStoresForEquality(Server2ArchivePostUp, finalServer2ArchiveStore);

        compareDataStoreStates(finalServer1Store, Server1StorePostUp, clusterName);
        compareDataStoreStates(Server1ArchivePostUp, finalServer1ArchiveStore, clusterName);

        compareDataStoreStates(finalPrismStore, prismStorePostUp, clusterName);
        compareDataStoreStates(prismArchivePostUp, finalPrismArchiveStore, clusterName);
    }


    @Test(groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileThatColoIsDownAndOtherHasSuspendedFeed()
        throws Exception {
        restartRequired = true;

        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, bundles[0].getDataSets().get(0)));
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, bundles[1].getDataSets().get(0)));
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //shutdown Server1
        Util.shutDownService(cluster1.getFeedHelper());

        //lets now delete the feed from both colos
        AssertUtil.assertFailed(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);

        Util.startService(cluster1.getFeedHelper());

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        HashMap<String, List<String>> finalSystemState = getSystemState(EntityType.FEED);

        compareDataStoreStates(finalSystemState.get("prismArchive"), finalPrismArchiveStore,
            clusterName);
        compareDataStoreStates(finalPrismStore, finalSystemState.get("prismStore"), clusterName);

        compareDataStoreStates(finalServer1Store, finalSystemState.get("Server1Store"),
            clusterName);
        compareDataStoreStates(finalSystemState.get("Server1Archive"), finalServer1ArchiveStore,
            clusterName);

        compareDataStoresForEquality(finalSystemState.get("Server2Archive"),
            finalServer2ArchiveStore);
        compareDataStoresForEquality(finalSystemState.get("Server2Store"), finalServer2Store);
    }

    @Test(groups = {"multiCluster"})
    public void testDeleteFeedScheduledInOneColoWhileThatColoIsDown() throws Exception {
        restartRequired = true;

        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //shutdown Server1
        Util.shutDownService(cluster1.getFeedHelper());

        //lets now delete the cluster from both colos
        AssertUtil.assertFailed(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);

        //Server2:
        compareDataStoresForEquality(initialServer2Store, finalServer2Store);
        compareDataStoresForEquality(finalServer2ArchiveStore, initialServer2ArchiveStore);


        Util.startService(cluster1.getClusterHelper());
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        HashMap<String, List<String>> systemStatePostUp = getSystemState(EntityType.FEED);

        compareDataStoreStates(finalPrismStore, systemStatePostUp.get("prismStore"), clusterName);
        compareDataStoreStates(systemStatePostUp.get("prismArchive"), finalPrismArchiveStore,
            clusterName);

        compareDataStoreStates(finalServer1Store, systemStatePostUp.get("Server1Store"),
            clusterName);
        compareDataStoreStates(systemStatePostUp.get("Server1Archive"), finalServer1ArchiveStore,
            clusterName);

        compareDataStoresForEquality(finalServer2ArchiveStore,
            systemStatePostUp.get("Server2Archive"));
        compareDataStoresForEquality(finalServer2Store, systemStatePostUp.get("Server2Store"));
    }

    @Test(groups = {"multiCluster"})
    public void testDeleteFeedSuspendedInOneColoWhileAnotherColoIsDown() throws Exception {
        restartRequired = true;

        bundles[0].setCLusterColo(cluster1Colo);
        logger.info("cluster bundle1: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));

        ServiceResponse r =
            prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundles[1].setCLusterColo(cluster2Colo);
        logger.info("cluster bundle2: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeServer1 = "2012-10-01T12:00Z";
        String startTimeServer2 = "2012-10-01T12:00Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeServer1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                baseHDFSDir + "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeServer2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                baseHDFSDir +
                    "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        logger.info("feed: " + Util.prettyPrintXml(feed));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);

        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        TimeUtil.sleepSeconds(15);

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        Util.shutDownService(cluster1.getFeedHelper());

        r = prism.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertPartial(r);
        Assert
            .assertTrue(r.getMessage().contains(cluster1Colo + "/org.apache.falcon.FalconException")
                && r.getMessage().contains(cluster2Colo + "/" + Util.readEntityName(feed)));

        ServiceResponse response = prism.getFeedHelper().delete(Util.URLS.DELETE_URL, feed);
        Assert.assertTrue(
            response.getMessage().contains(cluster1Colo + "/org.apache.falcon.FalconException")
                && response.getMessage().contains(cluster2Colo + "/" + Util.readEntityName(feed)));
        AssertUtil.assertPartial(response);

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(finalServer1ArchiveStore, initialServer1ArchiveStore);

        //Server2:
        compareDataStoreStates(initialServer2Store, finalServer2Store, clusterName);
        compareDataStoreStates(finalServer2ArchiveStore, initialServer2ArchiveStore, clusterName);
    }

    @Test(enabled = true)
    public void testDeleteFeedSuspendedInOneColoWhileAnotherColoIsDownWithFeedSuspended()
        throws Exception {
        restartRequired = true;

        bundles[0].setCLusterColo(cluster1Colo);
        logger.info("cluster bundle1: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));

        ServiceResponse r =
            prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        bundles[1].setCLusterColo(cluster2Colo);
        logger.info("cluster bundle2: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));
        r = prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        String startTimeServer1 = "2012-10-01T12:00Z";
        String startTimeServer2 = "2012-10-01T12:00Z";

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);
        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeServer1, "2099-10-01T12:10Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readEntityName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                baseHDFSDir + "/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        feed = InstanceUtil
            .setFeedCluster(feed, XmlUtil.createValidity(startTimeServer2, "2099-10-01T12:25Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readEntityName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                baseHDFSDir + "/clusterPath/localDC/rc/billing/${YEAR}/${MONTH}/${DAY}/${HOUR}/$" +
                    "{MINUTE}");

        logger.info("feed: " + Util.prettyPrintXml(feed));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);

        AssertUtil.assertSucceeded(r);

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.assertSucceeded(r);
        TimeUtil.sleepSeconds(15);

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        r = prism.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
        TimeUtil.sleepSeconds(10);
        AssertUtil.assertSucceeded(r);

        Util.shutDownService(cluster1.getFeedHelper());

        ServiceResponse response = prism.getFeedHelper().delete(Util.URLS.DELETE_URL, feed);
        Assert.assertTrue(response.getMessage().contains(cluster1Colo + "/org.apache.falcon" +
            ".FalconException")
            && response.getMessage().contains(cluster2Colo + "/" + Util.readEntityName(feed)));
        AssertUtil.assertPartial(response);

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(finalServer1ArchiveStore, initialServer1ArchiveStore);

        //Server2:
        compareDataStoreStates(initialServer2Store, finalServer2Store, clusterName);
        compareDataStoreStates(finalServer2ArchiveStore, initialServer2ArchiveStore, clusterName);
    }


    @Test(groups = {"multiCluster"})
    public void testDeleteFeedScheduledInOneColoWhileAnotherColoIsDown() throws Exception {
        restartRequired = true;

        bundles[0].submitAndScheduleFeed();
        bundles[1].submitAndScheduleFeed();

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //shutdown Server1
        Util.shutDownService(cluster1.getFeedHelper());

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[1].getDataSets().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getFeedHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getFeedHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getFeedHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getFeedHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getFeedHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getFeedHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[1].getDataSets().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);
        compareDataStoresForEquality(initialServer1ArchiveStore, finalServer1ArchiveStore);

        //Server2:
        compareDataStoreStates(initialServer2Store, finalServer2Store, clusterName);
        compareDataStoreStates(finalServer2ArchiveStore, initialServer2ArchiveStore, clusterName);

        Util.startService(cluster1.getFeedHelper());

        AssertUtil.assertSucceeded(
            prism.getFeedHelper().delete(Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0)));

        clusterName = Util.readEntityName(bundles[0].getDataSets().get(0));

        HashMap<String, List<String>> systemPostUp = getSystemState(EntityType.FEED);

        compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
            clusterName);
        compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), clusterName);

        compareDataStoreStates(systemPostUp.get("Server1Archive"), finalServer1ArchiveStore,
            clusterName);
        compareDataStoreStates(finalServer1Store, systemPostUp.get("Server1Store"), clusterName);

        compareDataStoresForEquality(finalServer2ArchiveStore, systemPostUp.get("Server2Archive"));
        compareDataStoresForEquality(finalServer2Store, systemPostUp.get("Server2Store"));
    }

    private void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                        String filename) {
        List<String> temp = new ArrayList<String>();
        temp.addAll(initialState);
        temp.removeAll(finalState);
        Assert.assertEquals(temp.size(), 1);
        Assert.assertTrue(temp.get(0).contains(filename));

    }

    private void compareDataStoresForEquality(List<String> store1, List<String> store2) {
        Assert.assertEquals(store1.size(), store2.size(), "DataStores are not equal!");
        Assert.assertTrue(Arrays.deepEquals(store2.toArray(new String[store2.size()]),
            store1.toArray(new String[store1.size()])), "DataStores are not equal!");
    }

    public HashMap<String, List<String>> getSystemState(EntityType entityType) throws Exception {
        IEntityManagerHelper prismHelper = prism.getClusterHelper();
        IEntityManagerHelper server1Helper = cluster1.getClusterHelper();
        IEntityManagerHelper server2Helper = cluster2.getClusterHelper();

        if (entityType == EntityType.FEED) {
            prismHelper = prism.getFeedHelper();
            server1Helper = cluster1.getFeedHelper();
            server2Helper = cluster2.getFeedHelper();
        }

        if (entityType == EntityType.PROCESS) {
            prismHelper = prism.getProcessHelper();
            server1Helper = cluster1.getProcessHelper();
            server2Helper = cluster2.getProcessHelper();
        }

        HashMap<String, List<String>> temp = new HashMap<String, List<String>>();
        temp.put("prismArchive", prismHelper.getArchiveInfo());
        temp.put("prismStore", prismHelper.getStoreInfo());
        temp.put("Server1Archive", server1Helper.getArchiveInfo());
        temp.put("Server1Store", server1Helper.getStoreInfo());
        temp.put("Server2Archive", server2Helper.getArchiveInfo());
        temp.put("Server2Store", server2Helper.getStoreInfo());

        return temp;
    }

}

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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

@Test(groups = "distributed")
public class PrismClusterDeleteTest extends BaseTestClass {

    private boolean restartRequired;
    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismClusterDeleteTest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismClusterDeleteTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = BundleUtil.readLateDataBundle();
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(cluster1.getFeedHelper());
        }
        removeBundles();
    }


    @Test(groups = {"multiCluster"})
    public void testServer1ClusterDeleteInBothColos() throws Exception {
        AssertUtil.assertSucceeded((prism.getClusterHelper()
            .submitEntity(Util.URLS.SUBMIT_URL, bundles[0].getClusters().get(0))));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getClusters().get(0));
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, clusterName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, clusterName);

        //Server1:
        compareDataStoreStates(initialServer1Store, finalServer1Store, clusterName);
        compareDataStoreStates(finalServer1ArchiveStore, initialServer1ArchiveStore, clusterName);

        //Server2:
        compareDataStoreStates(initialServer2Store, finalServer2Store, clusterName);
        compareDataStoreStates(finalServer2ArchiveStore, initialServer2ArchiveStore, clusterName);


    }

    @Test(groups = {"multiCluster"})
    public void testServer1ClusterDeleteWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(Util.URLS.SUBMIT_URL, bundles[0].getClusters().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();


        //bring down Server1 colo :P
        Util.shutDownService(cluster1.getClusterHelper());

        //lets now delete the cluster from both colos
        AssertUtil.assertPartial(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getClusters().get(0));
        //prism:
        compareDataStoresForEquality(initialPrismStore, finalPrismStore);
        compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

        //Server2:
        compareDataStoreStates(initialServer2Store, finalServer2Store, clusterName);
        compareDataStoreStates(finalServer2ArchiveStore, initialServer2ArchiveStore, clusterName);

        //Server1:
        compareDataStoresForEquality(initialServer1Store, finalServer1Store);

        //now bring up the service and roll forward the delete
        Util.startService(cluster1.getClusterHelper());
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //get final data states:
        List<String> server1ArchiveFinalState2 = cluster1.getClusterHelper().getArchiveInfo();
        List<String> server1StoreFinalState2 = cluster1.getClusterHelper().getStoreInfo();

        List<String> prismStorePostUp = prism.getClusterHelper().getStoreInfo();
        List<String> prismArchivePostUp = prism.getClusterHelper().getArchiveInfo();

        compareDataStoreStates(finalServer1Store, server1StoreFinalState2, clusterName);
        compareDataStoreStates(server1ArchiveFinalState2, finalServer1ArchiveStore, clusterName);

        List<String> server2ArchiveStateFinal = cluster2.getClusterHelper().getArchiveInfo();
        List<String> server2StoreStateFinal = cluster2.getClusterHelper().getStoreInfo();

        compareDataStoreStates(server2ArchiveStateFinal, initialServer2ArchiveStore, clusterName);
        compareDataStoreStates(initialServer2Store, server2StoreStateFinal, clusterName);

        compareDataStoreStates(finalPrismStore, prismStorePostUp, clusterName);
        compareDataStoreStates(prismArchivePostUp, finalPrismArchiveStore, clusterName);
    }


    @Test(groups = {"multiCluster"})
    public void testServer1ClusterDeleteAlreadyDeletedCluster() throws Exception {
        restartRequired = true;
        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(Util.URLS.SUBMIT_URL, bundles[0].getClusters().get(0)));
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //now lets get the final states
        List<String> finalPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

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
    public void testServer1ClusterDeleteTwiceWhen1ColoIsDownDuring1stDelete() throws Exception {
        restartRequired = true;

        AssertUtil.assertSucceeded(prism.getClusterHelper()
            .submitEntity(Util.URLS.SUBMIT_URL, bundles[0].getClusters().get(0)));
        Util.shutDownService(cluster1.getClusterHelper());

        //lets now delete the cluster from both colos
        AssertUtil.assertPartial(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //now lets get the final states
        List<String> initialPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //start up service
        Util.startService(cluster1.getClusterHelper());

        //delete again
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //get final states
        List<String> finalPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String clusterName = Util.readEntityName(bundles[0].getClusters().get(0));
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
    public void testServer1ClusterDeleteNonExistent() throws Exception {
        //now lets get the final states
        List<String> initialPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //delete
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //get final states
        List<String> finalPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

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
    public void testServer1ClusterDeleteNonExistentWhen1ColoIsDownDuringDelete()
        throws Exception {
        restartRequired = true;

        //now lets get the final states
        List<String> initialPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> initialServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> initialServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> initialServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

        //bring down Server1
        Util.shutDownService(cluster1.getClusterHelper());

        //delete
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        //get final states
        List<String> finalPrismStore = prism.getClusterHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getClusterHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalServer1Store = cluster1.getClusterHelper().getStoreInfo();
        List<String> finalServer1ArchiveStore = cluster1.getClusterHelper().getArchiveInfo();

        List<String> finalServer2Store = cluster2.getClusterHelper().getStoreInfo();
        List<String> finalServer2ArchiveStore = cluster2.getClusterHelper().getArchiveInfo();

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

        Util.startService(cluster1.getFeedHelper());
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().delete(Util.URLS.DELETE_URL, bundles[0].getClusters().get(0)));

        List<String> server1StorePostUp = cluster1.getClusterHelper().getStoreInfo();
        List<String> server1ArchivePostUp = cluster1.getClusterHelper().getArchiveInfo();

        List<String> server2StorePostUp = cluster2.getClusterHelper().getStoreInfo();
        List<String> server2ArchivePostUp = cluster2.getClusterHelper().getArchiveInfo();

        compareDataStoresForEquality(server2StorePostUp, finalServer2Store);
        compareDataStoresForEquality(server2ArchivePostUp, finalServer2ArchiveStore);

        compareDataStoresForEquality(finalServer1Store, server1StorePostUp);
        compareDataStoresForEquality(server1ArchivePostUp, finalServer1ArchiveStore);
    }

    private void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                        String filename) {
        initialState.removeAll(finalState);
        Assert.assertEquals(initialState.size(), 1);
        Assert.assertTrue(initialState.get(0).contains(filename));

    }

    private void compareDataStoresForEquality(List<String> store1, List<String> store2) {
        Assert.assertEquals(store1.size(), store2.size(), "DataStores are not equal!");
        Assert.assertTrue(Arrays.deepEquals(store2.toArray(new String[store2.size()]),
            store1.toArray(new String[store1.size()])), "DataStores are not equal!");
    }

}

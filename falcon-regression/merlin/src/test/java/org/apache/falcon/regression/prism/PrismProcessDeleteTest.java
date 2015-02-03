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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Delete process via prism tests.
 */
@Test(groups = "distributed")
public class PrismProcessDeleteTest extends BaseTestClass {

    private Bundle bundle;
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(PrismProcessDeleteTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundle = BundleUtil.readLateDataBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /* NOTE: All test cases assume that there are two entities scheduled in each colo
        PrismProcessDeleteTest.testUA1ProcessDeleteAlreadyDeletedProcess */
    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteInBothColos() throws Exception {
        //now submit the thing to prism
        bundles[0].submitFeedsScheduleProcess();
        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String processName = bundle.getProcessName();
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, processName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, processName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);
    }

    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteWhen1ColoIsDown() throws Exception {
        try {
            //now submit the thing to prism
            bundles[0].submitFeedsScheduleProcess();
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();


            //bring down UA2 colo :P
            Util.shutDownService(cluster2.getClusterHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertFailed(prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String processName = bundle.getProcessName();
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA2ArchiveStore);

            //bring service up
            Util.startService(cluster2.getProcessHelper());
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            HashMap<String, List<String>> systemPostUp = getSystemState(EntityType.PROCESS);

            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), processName);
            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                processName);

            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));
            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));

            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), processName);
            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                processName);
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getClusterHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteAlreadyDeletedProcess() throws Exception {
        try {
            //now submit the thing to prism
            bundles[0].submitFeedsScheduleProcess();
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);
            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);
            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            throw new TestNGException(e.getMessage());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteTwiceWhen1ColoIsDownDuring1stDelete()
        throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();

            Util.shutDownService(cluster2.getClusterHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertFailed(prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //now lets get the final states
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //start up service
            Util.startService(cluster2.getClusterHelper());

            //delete again
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //get final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String processName = bundle.getProcessName();
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);

            //UA1:
            compareDataStoreStates(initialUA1Store, finalUA1Store, processName);
            compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, processName);

        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getClusterHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteNonExistent() throws Exception {
        try {
            //now lets get the final states
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //delete
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //get final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            throw new TestNGException(e.getMessage());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testUA1ProcessDeleteNonExistentWhen1ColoIsDownDuringDelete()
        throws Exception {
        try {
            //now lets get the final states
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //bring down UA1
            Util.shutDownService(cluster2.getClusterHelper());

            //delete
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //get final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(initialPrismArchiveStore, finalPrismArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(initialUA2ArchiveStore, finalUA2ArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            Util.startService(cluster2.getClusterHelper());
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

        } catch (Exception e) {
            LOGGER.info(e.getMessage());
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getClusterHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColo() throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        bundles[1].submitFeedsScheduleProcess();

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String processName = bundle.getProcessName();
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, processName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, processName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);
    }

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColo() throws Exception {
        //create a UA1 bundle
        bundles[0].submitFeedsScheduleProcess();
        bundles[1].submitFeedsScheduleProcess();

        //suspend UA1 colo thingy
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String processName = bundles[0].getProcessName();
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, processName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, processName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileBothProcessesAreSuspended()
        throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        bundles[1].submitFeedsScheduleProcess();

        //suspend UA1 colo thingy
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[1].getProcessData()));

        //fetch the initial store and archive state for prism
        List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the initial store and archive for both colos
        List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //lets now delete the cluster from both colos
        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(bundles[0].getProcessData()));

        //now lets get the final states
        List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
        List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

        //fetch the final store and archive for both colos
        List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
        List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

        List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
        List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

        //now ensure that data has been deleted from all cluster store and is present in the
        // cluster archives

        String processName = bundle.getProcessName();
        //prism:
        compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
        compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

        //UA1:
        compareDataStoreStates(initialUA1Store, finalUA1Store, processName);
        compareDataStoreStates(finalUA1ArchiveStore, initialUA1ArchiveStore, processName);

        //UA2:
        compareDataStoresForEquality(initialUA2Store, finalUA2Store);
        compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);
    }

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileThatColoIsDown()
        throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            AssertUtil.assertSucceeded(
                prism.getProcessHelper().suspend(bundles[0].getProcessData()));

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertFailed(prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColoWhileThatColoIsDown()
        throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertFailed(prism.getProcessHelper().delete(bundles[0].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String processName = bundles[0].getProcessName();
            //prism:
            compareDataStoresForEquality(initialPrismStore, finalPrismStore);
            compareDataStoresForEquality(finalPrismArchiveStore, initialPrismArchiveStore);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoresForEquality(initialUA2Store, finalUA2Store);
            compareDataStoresForEquality(finalUA2ArchiveStore, initialUA2ArchiveStore);

            Util.startService(cluster2.getClusterHelper());
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            HashMap<String, List<String>> systemPostUp = getSystemState(EntityType.PROCESS);

            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));
            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));

            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), processName);
            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                processName);

            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), processName);
            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                processName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileAnotherColoIsDown()
        throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //now submit the thing to prism
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().suspend(bundles[1].getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[1].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String processName = bundle.getProcessName();
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, processName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, processName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessSuspendedInOneColoWhileAnotherColoIsDownWithFeedSuspended()
        throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //now submit the thing to prism
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().suspend(bundles[1].getProcessData()));
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().suspend(bundles[0].getProcessData()));
            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[1].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String processName = bundle.getProcessName();
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, processName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, processName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testDeleteProcessScheduledInOneColoWhileAnotherColoIsDown()
        throws Exception {
        try {
            bundles[0].submitFeedsScheduleProcess();
            bundles[1].submitFeedsScheduleProcess();

            //fetch the initial store and archive state for prism
            List<String> initialPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> initialPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the initial store and archive for both colos
            List<String> initialUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> initialUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> initialUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> initialUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //shutdown UA1
            Util.shutDownService(cluster2.getFeedHelper());

            //lets now delete the cluster from both colos
            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[1].getProcessData()));

            //now lets get the final states
            List<String> finalPrismStore = prism.getProcessHelper().getStoreInfo();
            List<String> finalPrismArchiveStore = prism.getProcessHelper().getArchiveInfo();

            //fetch the final store and archive for both colos
            List<String> finalUA1Store = cluster2.getProcessHelper().getStoreInfo();
            List<String> finalUA1ArchiveStore = cluster2.getProcessHelper().getArchiveInfo();

            List<String> finalUA2Store = cluster1.getProcessHelper().getStoreInfo();
            List<String> finalUA2ArchiveStore = cluster1.getProcessHelper().getArchiveInfo();

            //now ensure that data has been deleted from all cluster store and is present in the
            // cluster archives

            String processName = bundles[1].getProcessName();
            //prism:
            compareDataStoreStates(initialPrismStore, finalPrismStore, processName);
            compareDataStoreStates(finalPrismArchiveStore, initialPrismArchiveStore, processName);

            //UA1:
            compareDataStoresForEquality(initialUA1Store, finalUA1Store);
            compareDataStoresForEquality(initialUA1ArchiveStore, finalUA1ArchiveStore);

            //UA2:
            compareDataStoreStates(initialUA2Store, finalUA2Store, processName);
            compareDataStoreStates(finalUA2ArchiveStore, initialUA2ArchiveStore, processName);


            Util.startService(cluster2.getClusterHelper());

            AssertUtil.assertSucceeded(
                prism.getProcessHelper().delete(bundles[0].getProcessData()));

            HashMap<String, List<String>> systemPostUp = getSystemState(EntityType.PROCESS);

            processName = bundles[0].getProcessName();

            compareDataStoresForEquality(finalUA2Store, systemPostUp.get("ua2Store"));
            compareDataStoresForEquality(finalUA2ArchiveStore, systemPostUp.get("ua2Archive"));

            compareDataStoreStates(finalPrismStore, systemPostUp.get("prismStore"), processName);
            compareDataStoreStates(systemPostUp.get("prismArchive"), finalPrismArchiveStore,
                processName);

            compareDataStoreStates(finalUA1Store, systemPostUp.get("ua1Store"), processName);
            compareDataStoreStates(systemPostUp.get("ua1Archive"), finalUA1ArchiveStore,
                processName);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }


    private void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                        String filename) {

        List<String> temp = new ArrayList<String>(initialState);
        temp.removeAll(finalState);
        Assert.assertEquals(temp.size(), 1);
        Assert.assertTrue(temp.get(0).contains(filename));

    }


    private void compareDataStoresForEquality(List<String> store1, List<String> store2) {
        Assert.assertTrue(Arrays.deepEquals(store2.toArray(new String[store2.size()]),
            store1.toArray(new String[store1.size()])));
    }

    public HashMap<String, List<String>> getSystemState(EntityType entityType) throws Exception {
        AbstractEntityHelper prizm = prism.getClusterHelper();
        AbstractEntityHelper ua1 = cluster2.getClusterHelper();
        AbstractEntityHelper ua2 = cluster1.getClusterHelper();

        if (entityType == EntityType.FEED) {
            prizm = prism.getFeedHelper();
            ua1 = cluster2.getFeedHelper();
            ua2 = cluster1.getFeedHelper();
        }

        if (entityType == EntityType.PROCESS) {
            prizm = prism.getProcessHelper();
            ua1 = cluster2.getProcessHelper();
            ua2 = cluster1.getProcessHelper();
        }

        HashMap<String, List<String>> temp = new HashMap<String, List<String>>();
        temp.put("prismArchive", prizm.getArchiveInfo());
        temp.put("prismStore", prizm.getStoreInfo());
        temp.put("ua1Archive", ua1.getArchiveInfo());
        temp.put("ua1Store", ua1.getStoreInfo());
        temp.put("ua2Archive", ua2.getArchiveInfo());
        temp.put("ua2Store", ua2.getStoreInfo());

        return temp;
    }
}

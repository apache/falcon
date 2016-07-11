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

package org.apache.falcon.regression.hive.dr;

import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.RecipeMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.ExecResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

/**
 * Hdfs recipe test.
 */
@Test(groups = {"embedded", "multiCluster"})
public class HdfsRecipeTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(HdfsRecipeTest.class);
    private final ColoHelper cluster = servers.get(0);
    private final ColoHelper cluster2 = servers.get(1);
    private final FileSystem clusterFS = serverFS.get(0);
    private final FileSystem clusterFS2 = serverFS.get(1);
    private final OozieClient clusterOC = serverOC.get(0);
    private final OozieClient clusterOC2 = serverOC.get(1);
    private final String baseTestHDFSDir = cleanAndGetTestDir() + "/HdfsDR";
    private String sourceDataLocation = baseTestHDFSDir + "/source";
    private String targetDataLocation = baseTestHDFSDir + "/target";
    private RecipeMerlin hdfsRecipe;

    @DataProvider
    public Object[][] getRecipeLocation() {
        return MatrixUtil.crossProduct(RecipeExecLocation.values());
    }

    private void setUp(RecipeExecLocation recipeExecLocation) throws Exception {
        bundles[0] = new Bundle(BundleUtil.readELBundle(), cluster);
        bundles[1] = new Bundle(BundleUtil.readELBundle(), cluster2);
        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
        final ClusterMerlin srcCluster = bundles[0].getClusterElement();
        final ClusterMerlin tgtCluster = bundles[1].getClusterElement();
        String recipeDir = "HdfsRecipe";
        Bundle.submitCluster(recipeExecLocation.getRecipeBundle(bundles[0], bundles[1]));
        hdfsRecipe = RecipeMerlin.readFromDir(recipeDir, FalconCLI.RecipeOperation.HDFS_REPLICATION)
            .withRecipeCluster(recipeExecLocation.getRecipeCluster(srcCluster, tgtCluster));
        hdfsRecipe.withSourceCluster(srcCluster)
            .withTargetCluster(tgtCluster)
            .withFrequency(new Frequency("5", Frequency.TimeUnit.minutes))
            .withValidity(TimeUtil.getTimeWrtSystemTime(-5), TimeUtil.getTimeWrtSystemTime(15));
        hdfsRecipe.setUniqueName(this.getClass().getSimpleName());
    }

    /**
     * Test recipe based replication with 1 source and 1 target.
     */
    @Test(dataProvider = "getRecipeLocation")
    public void test1Source1Target(RecipeExecLocation execLocation) throws Exception {
        setUp(execLocation);
        hdfsRecipe.withSourceDir(sourceDataLocation).withTargetDir(targetDataLocation);
        final List<String> command = hdfsRecipe.getSubmissionCommand();
        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(execLocation.getRecipeOC(clusterOC, clusterOC2),
            hdfsRecipe.getName(), 1, CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        HadoopUtil.copyDataToFolder(clusterFS, sourceDataLocation, OSUtil.NORMAL_INPUT);

        InstanceUtil.waitTillInstanceReachState(execLocation.getRecipeOC(clusterOC, clusterOC2),
            hdfsRecipe.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        //check if data has been replicated correctly
        List<Path> cluster1ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(clusterFS, new Path(sourceDataLocation));
        List<Path> cluster2ReplicatedData = HadoopUtil
            .getAllFilesRecursivelyHDFS(clusterFS2, new Path(targetDataLocation));

        AssertUtil.checkForListSizes(cluster1ReplicatedData, cluster2ReplicatedData);

        //particular check for https://issues.apache.org/jira/browse/FALCON-1643
        ExecResult execResult = cluster.getProcessHelper().getCLIMetrics(hdfsRecipe.getName());
        AssertUtil.assertCLIMetrics(execResult, hdfsRecipe.getName(), 1, true);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        try {
            prism.getProcessHelper().deleteByName(hdfsRecipe.getName(), null);
        } catch (Exception e) {
            LOGGER.info("Deletion of process: " + hdfsRecipe.getName() + " failed with exception: " + e);
        }
        removeTestClassEntities();
        cleanTestsDirs();
    }
}

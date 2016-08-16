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
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.NotifyingAssert;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.HiveAssert;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.apache.falcon.regression.core.util.HiveUtil.runSql;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.bootstrapCopy;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.createVanillaTable;

/**
 * Hive DR Testing for Hive database replication.
 */
@Test(groups = {"embedded", "multiCluster"})
public class HiveDbDRTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(HiveDbDRTest.class);
    private final ColoHelper cluster = servers.get(0);
    private final ColoHelper cluster2 = servers.get(1);
    private final FileSystem clusterFS = serverFS.get(0);
    private final FileSystem clusterFS2 = serverFS.get(1);
    private final OozieClient clusterOC = serverOC.get(0);
    private final OozieClient clusterOC2 = serverOC.get(1);
    private HCatClient clusterHC;
    private HCatClient clusterHC2;
    private RecipeMerlin recipeMerlin;
    private Connection connection;
    private Connection connection2;

    @DataProvider
    public Object[][] getRecipeLocation() {
        return MatrixUtil.crossProduct(RecipeExecLocation.values());
    }

    private void setUp(RecipeExecLocation recipeExecLocation) throws Exception {
        clusterHC = cluster.getClusterHelper().getHCatClient();
        clusterHC2 = cluster2.getClusterHelper().getHCatClient();
        bundles[0] = new Bundle(BundleUtil.readHCatBundle(), cluster);
        bundles[1] = new Bundle(BundleUtil.readHCatBundle(), cluster2);
        bundles[0].generateUniqueBundle(this);
        bundles[1].generateUniqueBundle(this);
        final ClusterMerlin srcCluster = bundles[0].getClusterElement();
        final ClusterMerlin tgtCluster = bundles[1].getClusterElement();
        Bundle.submitCluster(recipeExecLocation.getRecipeBundle(bundles[0], bundles[1]));

        String recipeDir = "HiveDrRecipe";
        if (MerlinConstants.IS_SECURE) {
            recipeDir = "HiveDrSecureRecipe";
        }
        recipeMerlin = RecipeMerlin.readFromDir(recipeDir, FalconCLI.RecipeOperation.HIVE_DISASTER_RECOVERY)
            .withRecipeCluster(recipeExecLocation.getRecipeCluster(srcCluster, tgtCluster));
        recipeMerlin.withSourceCluster(srcCluster)
            .withTargetCluster(tgtCluster)
            .withFrequency(new Frequency("5", Frequency.TimeUnit.minutes))
            .withValidity(TimeUtil.getTimeWrtSystemTime(-1), TimeUtil.getTimeWrtSystemTime(11));
        recipeMerlin.setUniqueName(this.getClass().getSimpleName());

        connection = cluster.getClusterHelper().getHiveJdbcConnection();

        connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
    }

    private void setUpDb(String dbName, Connection conn) throws SQLException {
        runSql(conn, "drop database if exists " + dbName + " cascade");
        runSql(conn, "create database " + dbName);
        runSql(conn, "use " + dbName);
    }

    @Test(dataProvider = "getRecipeLocation")
    public void drDbDropDb(final RecipeExecLocation recipeExecLocation) throws Exception {
        setUp(recipeExecLocation);
        final String dbName = "drDbDropDb";
        setUpDb(dbName, connection);
        setUpDb(dbName, connection2);
        recipeMerlin.withSourceDb(dbName).withSourceTable("*");
        final List<String> command = recipeMerlin.getSubmissionCommand();

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        runSql(connection, "drop database " + dbName);

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final List<String> dstDbs = runSql(connection2, "show databases");
        Assert.assertFalse(dstDbs.contains(dbName), "dstDbs = " + dstDbs + " was not expected to "
            + "contain " + dbName);
    }


    @Test(dataProvider = "isDBReplication")
    public void drDbFailPass(Boolean isDBReplication) throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String dbName = "drDbFailPass";
        final String tblName = "vanillaTable";
        final String hiveWarehouseLocation = Config.getProperty("hive.warehouse.location", "/apps/hive/warehouse/");
        final String dbPath = HadoopUtil.joinPath(hiveWarehouseLocation, dbName.toLowerCase() + ".db");
        setUpDb(dbName, connection);
        runSql(connection, "create table " + tblName + "(data string)");
        setUpDb(dbName, connection2);
        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        recipeMerlin.withSourceDb(dbName).withSourceTable(isDBReplication ? "*" : tblName);

        final List<String> command = recipeMerlin.getSubmissionCommand();
        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        runSql(connection, "insert into table " + tblName + " values('cannot be replicated now')");
        final String noReadWritePerm = "d---r-xr-x";
        LOGGER.info("Setting " + clusterFS2.getUri() + dbPath + " to : " + noReadWritePerm);
        clusterFS2.setPermission(new Path(dbPath), FsPermission.valueOf(noReadWritePerm));

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.KILLED, EntityType.PROCESS);

        final String readWritePerm = "drwxr-xr-x";
        LOGGER.info("Setting " + clusterFS2.getUri() + dbPath + " to : " + readWritePerm);
        clusterFS2.setPermission(new Path(dbPath), FsPermission.valueOf(readWritePerm));

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(dbName, tblName),
            cluster2, clusterHC2.getTable(dbName, tblName), new NotifyingAssert(true)
        ).assertAll();
    }

    @Test
    public void drDbAddDropTable() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String dbName = "drDbAddDropTable";
        final String tblToBeDropped = "table_to_be_dropped";
        final String tblToBeDroppedAndAdded = "table_to_be_dropped_and_readded";
        final String newTableToBeAdded = "new_table_to_be_added";

        setUpDb(dbName, connection);
        setUpDb(dbName, connection2);
        recipeMerlin.withSourceDb(dbName).withSourceTable("*")
            .withFrequency(new Frequency("2", Frequency.TimeUnit.minutes));
        final List<String> command = recipeMerlin.getSubmissionCommand();

        createVanillaTable(connection, tblToBeDropped);
        createVanillaTable(connection, tblToBeDroppedAndAdded);
        bootstrapCopy(connection, clusterFS, tblToBeDropped,
            connection2, clusterFS2, tblToBeDropped);
        bootstrapCopy(connection, clusterFS, tblToBeDroppedAndAdded,
            connection2, clusterFS2, tblToBeDroppedAndAdded);

        /* For first replication - two tables are dropped & one table is added */
        runSql(connection, "drop table " + tblToBeDropped);
        runSql(connection, "drop table " + tblToBeDroppedAndAdded);
        createVanillaTable(connection, newTableToBeAdded);

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final NotifyingAssert anAssert = new NotifyingAssert(true);
        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase(dbName),
            cluster2, clusterHC2.getDatabase(dbName), anAssert);

        /* For second replication - a dropped tables is added back */
        createVanillaTable(connection, tblToBeDroppedAndAdded);

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 2, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase(dbName),
            cluster2, clusterHC2.getDatabase(dbName), anAssert);
        anAssert.assertAll();
    }

    @Test
    public void drDbNonReplicatableTable() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String dbName = "drDbNonReplicatableTable";
        final String tblName = "vanillaTable";
        final String tblView = "vanillaTableView";
        final String tblOffline = "offlineTable";

        setUpDb(dbName, connection);
        setUpDb(dbName, connection2);
        recipeMerlin.withSourceDb(dbName).withSourceTable("*")
            .withFrequency(new Frequency("2", Frequency.TimeUnit.minutes));
        final List<String> command = recipeMerlin.getSubmissionCommand();

        createVanillaTable(connection, tblName);
        runSql(connection, "create view " + tblView + " as select * from " + tblName);
        createVanillaTable(connection, tblOffline);
        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);
        bootstrapCopy(connection, clusterFS, tblOffline, connection2, clusterFS2, tblOffline);
        final String newComment = "'new comment for offline table should not reach destination'";
        runSql(connection,
            "alter table " + tblOffline + " set tblproperties ('comment' =" + newComment +")");
        runSql(connection, "alter table " + tblOffline + " enable offline");
        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        //vanilla table gets replicated, offline table & view are not replicated
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(dbName, tblName),
            cluster2, clusterHC2.getTable(dbName, tblName), new NotifyingAssert(true)).assertAll();
        final List<String> dstTables = runSql(connection2, "show tables");
        Assert.assertFalse(dstTables.contains(tblView),
            "dstTables = " + dstTables + " was not expected to contain " + tblView);
        final List<String> dstComment =
            runSql(connection2, "show tblproperties " + tblOffline + "('comment')");
        Assert.assertFalse(dstComment.contains(newComment),
            tblOffline + " comment = " + dstComment + " was not expected to contain " + newComment);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        try {
            prism.getProcessHelper().deleteByName(recipeMerlin.getName(), null);
        } catch (Exception e) {
            LOGGER.info("Deletion of process: " + recipeMerlin.getName() + " failed with exception: " + e);
        }
        removeTestClassEntities();
        cleanTestsDirs();
    }

    @DataProvider
    public Object[][] isDBReplication() {
        return new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}};
    }
}

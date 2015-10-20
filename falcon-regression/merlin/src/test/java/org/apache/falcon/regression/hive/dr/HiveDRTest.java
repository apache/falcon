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
import org.apache.falcon.regression.core.util.HiveAssert;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.apache.falcon.regression.core.util.HiveUtil.runSql;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.bootstrapCopy;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.createExternalTable;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.createExternalPartitionedTable;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.createPartitionedTable;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.createSerDeTable;
import static org.apache.falcon.regression.hive.dr.HiveObjectCreator.createVanillaTable;

/**
 * Hive DR Testing.
 */
@Test(groups = "embedded")
public class HiveDRTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(HiveDRTest.class);
    private static final String DB_NAME = "hdr_sdb1";
    private final ColoHelper cluster = servers.get(0);
    private final ColoHelper cluster2 = servers.get(1);
    private final ColoHelper cluster3 = servers.get(2);
    private final FileSystem clusterFS = serverFS.get(0);
    private final FileSystem clusterFS2 = serverFS.get(1);
    private final FileSystem clusterFS3 = serverFS.get(2);
    private final OozieClient clusterOC = serverOC.get(0);
    private final OozieClient clusterOC2 = serverOC.get(1);
    private final OozieClient clusterOC3 = serverOC.get(2);
    private final String baseTestHDFSDir = cleanAndGetTestDir() + "/HiveDR/";
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
        String recipeDir = "HiveDrRecipe";
        if (MerlinConstants.IS_SECURE) {
            recipeDir = "HiveDrSecureRecipe";
        }
        Bundle.submitCluster(recipeExecLocation.getRecipeBundle(bundles[0], bundles[1]));
        recipeMerlin = RecipeMerlin.readFromDir(recipeDir, FalconCLI.RecipeOperation.HIVE_DISASTER_RECOVERY)
            .withRecipeCluster(recipeExecLocation.getRecipeCluster(srcCluster, tgtCluster));
        recipeMerlin.withSourceCluster(srcCluster)
            .withTargetCluster(tgtCluster)
            .withFrequency(new Frequency("5", Frequency.TimeUnit.minutes))
            .withValidity(TimeUtil.getTimeWrtSystemTime(-5), TimeUtil.getTimeWrtSystemTime(15));
        recipeMerlin.setUniqueName(this.getClass().getSimpleName());

        connection = cluster.getClusterHelper().getHiveJdbcConnection();
        runSql(connection, "drop database if exists hdr_sdb1 cascade");
        runSql(connection, "create database hdr_sdb1");
        runSql(connection, "use hdr_sdb1");

        connection2 = cluster2.getClusterHelper().getHiveJdbcConnection();
        runSql(connection2, "drop database if exists hdr_sdb1 cascade");
        runSql(connection2, "create database hdr_sdb1");
        runSql(connection2, "use hdr_sdb1");
    }

    @Test(dataProvider = "getRecipeLocation")
    public void drPartition(final RecipeExecLocation recipeExecLocation) throws Exception {
        setUp(recipeExecLocation);
        final String tblName = "partitionDR";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        runSql(connection,
            "create table " + tblName + "(comment string) partitioned by (pname string)");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'DELETE') values"
                + "('this partition is going to be deleted - should NOT appear after dr')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'REPLACE') values"
                + "('this partition is going to be replaced - should NOT appear after dr')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'ADD_DATA') values"
                + "('this partition will have more data - should appear after dr')");

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'NEW_PART') values"
                + "('this partition has been added post bootstrap - should appear after dr')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'ADD_DATA') values"
                + "('more data has been added post bootstrap - should appear after dr')");
        runSql(connection,
            "alter table " + tblName + " drop partition(pname = 'DELETE')");
        runSql(connection,
            "alter table " + tblName + " drop partition(pname = 'REPLACE')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'REPLACE') values"
                + "('this partition has been replaced - should appear after dr')");

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();
    }

    @Test
    public void drInsertOverwritePartition() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "drInsertOverwritePartition";
        final String hlpTblName = "drInsertOverwritePartitionHelperTbl";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();
        runSql(connection, "create table " + hlpTblName + "(comment string)");
        runSql(connection,
            "insert into table " + hlpTblName
                + " values('overwrite data - should appear after dr')");
        runSql(connection,
            "insert into table " + hlpTblName + " values('newdata row2 - should appear after dr')");
        runSql(connection,
            "insert into table " + hlpTblName + " values('newdata row1 - should appear after dr')");

        runSql(connection,
            "create table " + tblName + "(comment string) partitioned by (pname string)");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'OLD_PART') values"
                + "('this data should be retained - should appear after dr')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname = 'OVERWRITE_PART') values"
                + "('this data should get overwritten - should NOT appear after dr')");

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        runSql(connection,
            "insert overwrite table " + tblName + " partition (pname = 'OVERWRITE_PART') "
                + "select * from " + hlpTblName + " where comment REGEXP '^overwrite'");
        runSql(connection,
            "insert overwrite table " + tblName + " partition (pname = 'NEW_DATA') "
                + "select * from " + hlpTblName + " where comment REGEXP '^newdata'");

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();
    }

    @Test
    public void drTwoTablesOneRequest() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.TargetCluster;
        setUp(recipeExecLocation);
        final String tblName = "firstTableDR";
        final String tbl2Name = "secondTableDR";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName + ',' + tbl2Name);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        runSql(connection,
            "create table " + tblName + "(comment string)");
        runSql(connection,
            "create table " + tbl2Name + "(comment string)");

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);
        bootstrapCopy(connection, clusterFS, tbl2Name, connection2, clusterFS2, tbl2Name);

        runSql(connection,
            "insert into table " + tblName + " values"
                + "('this string has been added post bootstrap - should appear after dr')");
        runSql(connection,
            "insert into table " + tbl2Name + " values"
                + "('this string has been added post bootstrap - should appear after dr')");

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final NotifyingAssert anAssert = new NotifyingAssert(true);
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), anAssert);
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tbl2Name),
            cluster2, clusterHC2.getTable(DB_NAME, tbl2Name), anAssert);
        anAssert.assertAll();

    }

    @Test
    public void drSerDeWithProperties() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "serdeTable";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        runSql(connection,
            "create table " + tblName + "(comment string) "
                + "row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'");

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        runSql(connection,
            "insert into table " + tblName + " values"
                + "('this string has been added post bootstrap - should appear after dr')");

        runSql(connection,
            "ALTER TABLE " + tblName + " SET SERDEPROPERTIES ('someProperty' = 'value')");

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();

    }

    @Test
    public void drChangeColumn() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "tableForColumnChange";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command1 = recipeMerlin.getSubmissionCommand();
        final String recipe1Name = recipeMerlin.getName();
        runSql(connection,
            "create table " + tblName + "(id int)");

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        Assert.assertEquals(Bundle.runFalconCLI(command1), 0, "Recipe submission failed.");
        runSql(connection,
            "ALTER TABLE " + tblName + " CHANGE id id STRING COMMENT 'some_comment'");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipe1Name, 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();
    }


    @Test
    public void drTwoDstTablesTwoRequests() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.TargetCluster;
        setUp(recipeExecLocation);
        final HCatClient clusterHC3 = cluster3.getClusterHelper().getHCatClient();
        final Connection connection3 = cluster3.getClusterHelper().getHiveJdbcConnection();
        runSql(connection3, "drop database if exists hdr_sdb1 cascade");
        runSql(connection3, "create database hdr_sdb1");
        runSql(connection3, "use hdr_sdb1");

        final String tblName = "vanillaTable";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final String recipe1Name = recipeMerlin.getName();
        final List<String> command1 = recipeMerlin.getSubmissionCommand();

        final Bundle bundle3 = new Bundle(BundleUtil.readHCatBundle(), cluster3);
        bundle3.generateUniqueBundle(this);
        bundle3.submitClusters(prism);
        recipeMerlin.withTargetCluster(bundle3.getClusterElement())
                .withRecipeCluster(recipeExecLocation.getRecipeCluster(
                        bundles[0].getClusterElement(), bundle3.getClusterElement()));
        recipeMerlin.setUniqueName(this.getClass().getSimpleName());

        final List<String> command2 = recipeMerlin.getSubmissionCommand();
        final String recipe2Name = recipeMerlin.getName();

        runSql(connection, "create table " + tblName + "(comment string)");

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);
        bootstrapCopy(connection, clusterFS, tblName, connection3, clusterFS3, tblName);

        runSql(connection,
            "insert into table " + tblName + " values"
                + "('this string has been added post bootstrap - should appear after dr')");

        Assert.assertEquals(Bundle.runFalconCLI(command1), 0, "Recipe submission failed.");
        Assert.assertEquals(Bundle.runFalconCLI(command2), 0, "Recipe submission failed.");


        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipe1Name, 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC3),
            recipe2Name, 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final NotifyingAssert anAssert = new NotifyingAssert(true);
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), anAssert);
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster3, clusterHC3.getTable(DB_NAME, tblName), anAssert);
        anAssert.assertAll();
    }

    @Test
    public void drExternalToNonExternal() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "externalToNonExternal";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        createExternalTable(connection, clusterFS, baseTestHDFSDir + "click_data/", tblName);
        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        //change column name
        runSql(connection,
            "alter table " + tblName + " change column data data_new string");

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final NotifyingAssert anAssert = new NotifyingAssert(true);
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), anAssert, false);
        anAssert.assertNotEquals(clusterHC2.getTable(DB_NAME, tblName).getTabletype(),
            clusterHC.getTable(DB_NAME, tblName).getTableName(),
            "Source and destination tables should have different Tabletype");
        anAssert.assertNotEquals(clusterHC2.getTable(DB_NAME, tblName).getTblProps().get("EXTERNAL"),
            clusterHC.getTable(DB_NAME, tblName).getTblProps().get("EXTERNAL"),
            "Source and destination tables should have different value of property EXTERNAL");
        anAssert.assertAll();
    }

    @Test
    public void drExtPartitionedToNonExtPartitioned() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "extPartitionedToNonExtPartitioned";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        createExternalPartitionedTable(connection, clusterFS,
            baseTestHDFSDir + "click_data/", tblName);
        runSql(connection2,
            "create table " + tblName + " (data string, time string) partitioned by (date_ string)");
        runSql(connection2, "alter table " + tblName + " add partition "
            + "(date_='2001-01-01') location '" + baseTestHDFSDir + "click_data/2001-01-01/'");
        runSql(connection2, "alter table " + tblName + " add partition "
            + "(date_='2001-01-02') location '" + baseTestHDFSDir + "click_data/2001-01-02/'");

        runSql(connection2, "insert into table " + tblName + " partition (date_='2001-01-01') "
            + "values ('click1', '01:01:01')");
        runSql(connection2, "insert into table " + tblName + " partition (date_='2001-01-02') "
            + "values ('click2', '02:02:02')");

        final NotifyingAssert anAssert = new NotifyingAssert(true);
        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), anAssert, false);


        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        //change column name
        runSql(connection,
            "alter table " + tblName + " change column data data_new string");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), anAssert, false);
        anAssert.assertNotEquals(clusterHC2.getTable(DB_NAME, tblName).getTabletype(),
            clusterHC.getTable(DB_NAME, tblName).getTableName(),
            "Source and destination tables should have different Tabletype");
        anAssert.assertNotEquals(clusterHC2.getTable(DB_NAME, tblName).getTblProps().get("EXTERNAL"),
            clusterHC.getTable(DB_NAME, tblName).getTblProps().get("EXTERNAL"),
            "Source and destination tables should have different value of property EXTERNAL");
        anAssert.assertAll();
    }

    /**
     * 1 src tbl 1 dst tbl. Change table properties and comment at the source.
     * Changes should get reflected at destination.
     */
    @Test
    public void drChangeCommentAndPropertyTest() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "myTable";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        runSql(connection, "create table " + tblName + "(field string)");
        //add new table property
        runSql(connection,
            "ALTER TABLE " + tblName + " SET TBLPROPERTIES('someProperty' = 'initialValue')");
        //set comment
        runSql(connection,
            "ALTER TABLE " + tblName + " SET TBLPROPERTIES('comment' = 'this comment will be "
                + "changed, SHOULD NOT appear')");

        LOGGER.info(tblName + " before bootstrap copy: ");
        runSql(connection, "describe extended " + tblName);

        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        //change table property and comment
        runSql(connection,
            "ALTER TABLE " + tblName + " SET TBLPROPERTIES('someProperty' = 'anotherValue')");
        runSql(connection,
            "ALTER TABLE " + tblName + " SET TBLPROPERTIES('comment' = 'this comment should "
                + "appear after replication done')");

        LOGGER.info(tblName + " after modifications, before replication: ");
        runSql(connection, "describe extended " + tblName);

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();
    }

    @Test
    public void dataGeneration() throws Exception {
        setUp(RecipeExecLocation.SourceCluster);
        runSql(connection, "use hdr_sdb1");
        createVanillaTable(connection, "store_sales");
        createSerDeTable(connection);
        createPartitionedTable(connection);
        createExternalTable(connection, clusterFS,
            baseTestHDFSDir + "click_data/", "click_data");
        createExternalPartitionedTable(connection, clusterFS,
            baseTestHDFSDir + "click_data2/", "click_data2");

        runSql(connection2, "use hdr_sdb1");
        createVanillaTable(connection2, "store_sales");
        createSerDeTable(connection2);
        createPartitionedTable(connection2);
        createExternalTable(connection2, clusterFS2,
            baseTestHDFSDir + "click_data/", "click_data");
        createExternalPartitionedTable(connection2, clusterFS2,
            baseTestHDFSDir + "click_data2/", "click_data2");

        final NotifyingAssert anAssert = new NotifyingAssert(true);
        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("hdr_sdb1"),
            cluster2, clusterHC2.getDatabase("hdr_sdb1"), anAssert);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable("hdr_sdb1", "click_data"),
            cluster2, clusterHC2.getTable("hdr_sdb1", "click_data"), anAssert);
        anAssert.assertAll();

    }

    @Test(enabled = false)
    public void assertionTest() throws Exception {
        setUp(RecipeExecLocation.SourceCluster);
        final SoftAssert anAssert = new SoftAssert();
        HiveAssert.assertTableEqual(
            cluster, clusterHC.getTable("default", "hcatsmoke10546"),
            cluster2, clusterHC2.getTable("default", "hcatsmoke10548"), anAssert);
        HiveAssert.assertDbEqual(cluster, clusterHC.getDatabase("default"), cluster2,
            clusterHC2.getDatabase("default"), anAssert);
        anAssert.assertAll();
    }

    /**
     * Test creates a table on first cluster using static partitioning. Then it creates the same
     * table on the second cluster using dynamic partitioning. Finally it checks the equality of
     * these tables.
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void dynamicPartitionsTest() throws Exception {
        setUp(RecipeExecLocation.SourceCluster);
        //create table with static partitions on first cluster
        createPartitionedTable(connection, false);

        //create table with dynamic partitions on second cluster
        createPartitionedTable(connection2, true);

        //check that both tables are equal
        HiveAssert.assertTableEqual(
            cluster, clusterHC.getTable("hdr_sdb1", "global_store_sales"),
            cluster2, clusterHC2.getTable("hdr_sdb1", "global_store_sales"), new SoftAssert()
        ).assertAll();
    }

    /**
     * 1 src tbl 1 dst tbl replication. Insert/delete/replace partitions using dynamic partition
     * queries. The changes should get reflected at destination.
     */
    @Test
    public void drInsertDropReplaceDynamicPartition() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "dynamicPartitionDR";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        //disable strict mode to use only dynamic partition
        runSql(connection, "set hive.exec.dynamic.partition.mode=nonstrict");

        runSql(connection,
            "create table " + tblName + "(comment string) partitioned by (pname string)");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this partition is going to be deleted - should NOT appear after dr', 'DELETE')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this partition is going to be replaced - should NOT appear after dr', 'REPLACE')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this partition will have more data - should appear after dr', 'ADD_DATA')");

        LOGGER.info(tblName + " before bootstrap copying: ");
        runSql(connection, "select * from " + tblName);
        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this partition has been added post bootstrap - should appear after dr', 'NEW_PART')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('more data has been added post bootstrap - should appear after dr', 'ADD_DATA')");
        runSql(connection,
            "alter table " + tblName + " drop partition(pname = 'DELETE')");
        runSql(connection,
            "alter table " + tblName + " drop partition(pname = 'REPLACE')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this partition has been replaced - should appear after dr', 'REPLACE')");

        LOGGER.info(tblName + " after modifications, before replication: ");
        runSql(connection, "select * from " + tblName);

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();
    }

    /**
     * 1 src tbl 1 dst tbl replication. Insert/overwrite partitions using dynamic partitions
     * queries. The changes should get reflected at destination.
     * @throws Exception
     */
    @Test
    public void drInsertOverwriteDynamicPartition() throws Exception {
        final RecipeExecLocation recipeExecLocation = RecipeExecLocation.SourceCluster;
        setUp(recipeExecLocation);
        final String tblName = "drInsertOverwritePartition";
        final String hlpTblName = "drInsertOverwritePartitionHelperTbl";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName);
        final List<String> command = recipeMerlin.getSubmissionCommand();

        //disable strict mode to use only dynamic partition
        runSql(connection, "set hive.exec.dynamic.partition.mode=nonstrict");

        runSql(connection,
            "create table " + hlpTblName + "(comment string) partitioned by (pname string)");
        runSql(connection,
            "insert into table " + hlpTblName + " partition (pname)"
                + " values('overwrite data - should appear after dr', 'OVERWRITE_PART')");
        runSql(connection,
            "insert into table " + hlpTblName + " partition (pname)"
            + " values('newdata row2 - should appear after dr', 'NEW_DATA')");
        runSql(connection,
            "insert into table " + hlpTblName + " partition (pname)"
                + " values('newdata row1 - should appear after dr', 'NEW_DATA')");

        runSql(connection,
            "create table " + tblName + "(comment string) partitioned by (pname string)");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this data should be retained - should appear after dr', 'OLD_PART')");
        runSql(connection,
            "insert into table " + tblName + " partition (pname) values"
                + "('this data should get overwritten - should NOT appear after dr', 'OVERWRITE_PART')");

        LOGGER.info(tblName + " before bootstrap copying: ");
        runSql(connection, "select * from " + tblName);
        bootstrapCopy(connection, clusterFS, tblName, connection2, clusterFS2, tblName);

        runSql(connection,
            "insert overwrite table " + tblName + " partition (pname) "
                + "select comment, pname from " + hlpTblName + " where comment REGEXP '^overwrite'");
        runSql(connection,
            "insert overwrite table " + tblName + " partition (pname) "
                + "select comment, pname from " + hlpTblName + " where comment REGEXP '^newdata'");

        LOGGER.info(tblName + " after modifications, before replication: ");
        runSql(connection, "select * from " + tblName);

        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");

        InstanceUtil.waitTillInstanceReachState(recipeExecLocation.getRecipeOC(clusterOC, clusterOC2),
            recipeMerlin.getName(), 1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        HiveAssert.assertTableEqual(cluster, clusterHC.getTable(DB_NAME, tblName),
            cluster2, clusterHC2.getTable(DB_NAME, tblName), new NotifyingAssert(true)
        ).assertAll();
    }

    /**
     * Run recipe with different frequencies. Submission should go through.
     * Check frequency of the launched oozie job
     */
    @Test(dataProvider = "frequencyGenerator")
    public void differentRecipeFrequenciesTest(String frequency) throws Exception {
        setUp(RecipeExecLocation.SourceCluster);
        LOGGER.info("Testing with frequency: " + frequency);
        String tblName = "myTable";
        recipeMerlin.withSourceDb(DB_NAME).withSourceTable(tblName)
            .withFrequency(new Frequency(frequency));
        runSql(connection, "create table " + tblName + "(comment string)");
        final List<String> command = recipeMerlin.getSubmissionCommand();
        Assert.assertEquals(Bundle.runFalconCLI(command), 0, "Recipe submission failed.");
        LOGGER.info("Submission went through.");

        InstanceUtil.waitTillInstanceReachState(clusterOC, recipeMerlin.getName(), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS);
        String filter = "name=FALCON_PROCESS_" + recipeMerlin.getName();
        List<BundleJob> bundleJobs = OozieUtil.getBundles(clusterOC, filter, 0, 10);
        List<String> bundleIds = OozieUtil.getBundleIds(bundleJobs);
        String bundleId = OozieUtil.getMaxId(bundleIds);
        List<CoordinatorJob> coords = clusterOC.getBundleJobInfo(bundleId).getCoordinators();
        List<String> cIds = new ArrayList<String>();
        for (CoordinatorJob coord : coords) {
            cIds.add(coord.getId());
        }
        String coordId = OozieUtil.getMinId(cIds);
        CoordinatorJob job = clusterOC.getCoordJobInfo(coordId);
        CoordinatorJob.Timeunit timeUnit = job.getTimeUnit();
        String freq = job.getFrequency();
        LOGGER.info("Frequency of running job: " + timeUnit + " " + freq);
        Assert.assertTrue(frequency.contains(timeUnit.name().toLowerCase().replace("_", ""))
            && frequency.contains(freq), "Running job has different frequency.");
    }

    @DataProvider(name = "frequencyGenerator")
    public Object[][] frequencyGenerator() {
        return new Object[][]{{"minutes(10)"}, {"minutes(10000)"},
            {"days(3)"}, {"days(3000)"}, {"months(1)"}, {"months(1000)"}, };
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

}

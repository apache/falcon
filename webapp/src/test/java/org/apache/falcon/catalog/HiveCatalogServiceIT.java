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

package org.apache.falcon.catalog;

import org.apache.falcon.FalconException;
import org.apache.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatCreateDBDesc;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests Hive Meta Store service.
 */
public class HiveCatalogServiceIT {

    private static final String METASTORE_URL = "thrift://localhost:49083";
    private static final String DATABASE_NAME = "falcon_db";
    private static final String TABLE_NAME = "falcon_table";
    private static final String EXTERNAL_TABLE_NAME = "falcon_external";
    private static final String EXTERNAL_TABLE_LOCATION = "jail://global:00/falcon/staging/falcon_external";

    private HiveCatalogService hiveCatalogService;
    private HCatClient client;

    @BeforeClass
    public void setUp() throws Exception {
        hiveCatalogService = new HiveCatalogService();
        client = HiveCatalogService.get(METASTORE_URL);

        createDatabase();
        createTable();
        createExternalTable();
    }

    private void createDatabase() throws Exception {
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(DATABASE_NAME)
                .ifNotExists(true).build();
        client.createDatabase(dbDesc);
    }

    public void createTable() throws Exception {
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.INT, "id comment"));
        cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

        List<HCatFieldSchema> partitionSchema = Arrays.asList(
                new HCatFieldSchema("ds", HCatFieldSchema.Type.STRING, ""),
                new HCatFieldSchema("region", HCatFieldSchema.Type.STRING, "")
        );

        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(DATABASE_NAME, TABLE_NAME, cols)
                .fileFormat("rcfile")
                .ifNotExists(true)
                .comments("falcon integration test")
                .partCols(new ArrayList<HCatFieldSchema>(partitionSchema))
                .build();
        client.createTable(tableDesc);
    }

    public void createExternalTable() throws Exception {
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.INT, "id comment"));
        cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

        List<HCatFieldSchema> partitionSchema = Arrays.asList(
                new HCatFieldSchema("ds", HCatFieldSchema.Type.STRING, ""),
                new HCatFieldSchema("region", HCatFieldSchema.Type.STRING, "")
        );

        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(DATABASE_NAME, EXTERNAL_TABLE_NAME, cols)
                .fileFormat("rcfile")
                .ifNotExists(true)
                .comments("falcon integration test")
                .partCols(new ArrayList<HCatFieldSchema>(partitionSchema))
                .isTableExternal(true)
                .location(EXTERNAL_TABLE_LOCATION)
                .build();
        client.createTable(tableDesc);
    }

    @AfterClass
    public void tearDown() throws Exception {
        dropTable(EXTERNAL_TABLE_NAME);
        dropTable(TABLE_NAME);
        dropDatabase();
    }

    private void dropTable(String tableName) throws Exception {
        client.dropTable(DATABASE_NAME, tableName, true);
    }

    private void dropDatabase() throws Exception {
        client.dropDatabase(DATABASE_NAME, true, HCatClient.DropDBMode.CASCADE);
    }

    @BeforeMethod
    private void addPartitions() throws Exception {
        Map<String, String> firstPtn = new HashMap<String, String>();
        firstPtn.put("ds", "20130903"); //yyyyMMDD
        firstPtn.put("region", "us");
        HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, firstPtn).build();
        client.addPartition(addPtn);

        Map<String, String> secondPtn = new HashMap<String, String>();
        secondPtn.put("ds", "20130903");
        secondPtn.put("region", "in");
        HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, secondPtn).build();
        client.addPartition(addPtn2);

        Map<String, String> thirdPtn = new HashMap<String, String>();
        thirdPtn.put("ds", "20130902");
        thirdPtn.put("region", "in");
        HCatAddPartitionDesc addPtn3 = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, thirdPtn).build();
        client.addPartition(addPtn3);
    }

    @AfterMethod
    private void dropPartitions() throws Exception {
        Map<String, String> partitionSpec = new HashMap<String, String>();
        partitionSpec.put("ds", "20130903");
        client.dropPartitions(DATABASE_NAME, TABLE_NAME, partitionSpec, true);

        partitionSpec = new HashMap<String, String>();
        partitionSpec.put("ds", "20130902");
        client.dropPartitions(DATABASE_NAME, TABLE_NAME, partitionSpec, true);
    }

    @Test
    public void testGet() throws Exception {
        Assert.assertNotNull(HiveCatalogService.get(METASTORE_URL));
    }

    @Test
    public void testIsAlive() throws Exception {
        Assert.assertTrue(hiveCatalogService.isAlive(METASTORE_URL));
    }

    @Test (expectedExceptions = FalconException.class)
    public void testIsAliveNegative() throws Exception {
        hiveCatalogService.isAlive("thrift://localhost:9999");
    }

    @Test (expectedExceptions = FalconException.class)
    public void testTableExistsNegative() throws Exception {
        hiveCatalogService.tableExists(METASTORE_URL, DATABASE_NAME, "blah");
    }

    @Test
    public void testTableExists() throws Exception {
        Assert.assertTrue(hiveCatalogService.tableExists(METASTORE_URL, DATABASE_NAME, TABLE_NAME));
    }

    @Test
    public void testIsTableExternalFalse() throws Exception {
        Assert.assertFalse(hiveCatalogService.isTableExternal(METASTORE_URL, DATABASE_NAME, TABLE_NAME));
    }
    @Test
    public void testIsTableExternalTrue() throws Exception {
        Assert.assertTrue(hiveCatalogService.isTableExternal(METASTORE_URL, DATABASE_NAME, EXTERNAL_TABLE_NAME));
    }

    @Test
    public void testListPartitionsByFilterNull() throws Exception {

        List<CatalogPartition> filteredPartitions = hiveCatalogService.listPartitionsByFilter(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, null);
        Assert.assertEquals(filteredPartitions.size(), 3);
    }

    @DataProvider (name = "lessThanFilter")
    public Object[][] createLessThanFilter() {
        return new Object[][] {
            {"ds < \"20130905\"", 3},
            {"ds < \"20130904\"", 3},
            {"ds < \"20130903\"", 1},
            {"ds < \"20130902\"", 0},
        };
    }

    @Test (dataProvider = "lessThanFilter")
    public void testListPartitionsByFilterLessThan(String lessThanFilter, int expectedPartitionCount)
        throws Exception {

        List<CatalogPartition> filteredPartitions = hiveCatalogService.listPartitionsByFilter(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, lessThanFilter);
        Assert.assertEquals(filteredPartitions.size(), expectedPartitionCount);
    }

    @DataProvider (name = "greaterThanFilter")
    public Object[][] createGreaterThanFilter() {
        return new Object[][] {
            {"ds > \"20130831\"", 3},
            {"ds > \"20130905\"", 0},
            {"ds > \"20130904\"", 0},
            {"ds > \"20130903\"", 0},
            {"ds > \"20130902\"", 2},
        };
    }

    @Test (dataProvider = "greaterThanFilter")
    public void testListPartitionsByFilterGreaterThan(String greaterThanFilter, int expectedPartitionCount)
        throws Exception {

        List<CatalogPartition> filteredPartitions = hiveCatalogService.listPartitionsByFilter(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, greaterThanFilter);
        Assert.assertEquals(filteredPartitions.size(), expectedPartitionCount);
    }

    @Test
    public void testGetPartitionsFullSpec() throws Exception {
        Map<String, String> partitionSpec = new HashMap<String, String>();
        partitionSpec.put("ds", "20130902");
        partitionSpec.put("region", "in");

        HCatPartition ptn = client.getPartition(DATABASE_NAME, TABLE_NAME, partitionSpec);
        Assert.assertTrue(ptn != null);
    }

    @Test
    public void testGetPartitionsPartialSpec() throws Exception {
        Map<String, String> partialPartitionSpec = new HashMap<String, String>();
        partialPartitionSpec.put("ds", "20130903");

        List<HCatPartition> partitions = client.getPartitions(DATABASE_NAME, TABLE_NAME, partialPartitionSpec);
        Assert.assertEquals(partitions.size(), 2);
    }

    @Test
    public void testDropPartition() throws Exception {
        Map<String, String> partialPartitionSpec = new HashMap<String, String>();
        partialPartitionSpec.put("ds", "20130903");

        Assert.assertTrue(hiveCatalogService.dropPartitions(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, partialPartitionSpec));

        List<HCatPartition> partitions = client.getPartitions(DATABASE_NAME, TABLE_NAME);
        Assert.assertEquals(1, partitions.size(), "Unexpected number of partitions");
        Assert.assertEquals(new String[]{"20130902", "in"},
                partitions.get(0).getValues().toArray(), "Mismatched partition");

        partialPartitionSpec = new HashMap<String, String>();
        partialPartitionSpec.put("ds", "20130902");

        Assert.assertTrue(hiveCatalogService.dropPartitions(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, partialPartitionSpec));
        partitions = client.getPartitions(DATABASE_NAME, TABLE_NAME);
        Assert.assertEquals(partitions.size(), 0, "Unexpected number of partitions");
    }

    @Test
    public void testGetPartition() throws Exception {
        Map<String, String> partitionSpec = new HashMap<String, String>();
        partitionSpec.put("ds", "20130902");
        partitionSpec.put("region", "in");

        CatalogPartition partition = CatalogServiceFactory.getCatalogService().getPartition(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionSpec);
        Assert.assertNotNull(partition);

        long createTime = partition.getCreateTime();
        Assert.assertTrue(createTime > 0);
    }

    @Test
    public void testReInstatePartition() throws Exception {
        Map<String, String> partitionSpec = new HashMap<String, String>();
        partitionSpec.put("ds", "20130918");
        partitionSpec.put("region", "blah");

        HCatAddPartitionDesc first = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, partitionSpec).build();
        client.addPartition(first);

        CatalogPartition partition = CatalogServiceFactory.getCatalogService().getPartition(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionSpec);
        Assert.assertNotNull(partition);
        final long originalCreateTime = partition.getCreateTime();

        Thread.sleep(1000); // sleep before deletion
        client.dropPartitions(DATABASE_NAME, TABLE_NAME, partitionSpec, true);

        Thread.sleep(1000); // sleep so the next add is delayed a bit

        HCatAddPartitionDesc second = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, partitionSpec).build();
        client.addPartition(second);

        CatalogPartition reInstatedPartition = CatalogServiceFactory.getCatalogService().getPartition(
                METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionSpec);
        Assert.assertNotNull(reInstatedPartition);
        final long reInstatedCreateTime = reInstatedPartition.getCreateTime();

        Assert.assertTrue(reInstatedCreateTime > originalCreateTime);
    }
}

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
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
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
import java.util.LinkedHashMap;
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

    private final Configuration conf = new Configuration(false);
    private HiveCatalogService hiveCatalogService;
    private HCatClient client;

    @BeforeClass
    public void setUp() throws Exception {
        // setup a logged in user
        CurrentUser.authenticate(TestContext.REMOTE_USER);

        hiveCatalogService = new HiveCatalogService();
        client = TestContext.getHCatClient(METASTORE_URL);

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
    public void testIsAlive() throws Exception {
        Assert.assertTrue(hiveCatalogService.isAlive(conf, METASTORE_URL));
    }

    @Test (expectedExceptions = Exception.class)
    public void testIsAliveNegative() throws Exception {
        hiveCatalogService.isAlive(conf, "thrift://localhost:9999");
    }

    @Test
    public void testTableExistsNegative() throws Exception {
        Assert.assertFalse(hiveCatalogService.tableExists(conf, METASTORE_URL, DATABASE_NAME, "blah"));
    }

    @Test
    public void testTableExists() throws Exception {
        Assert.assertTrue(hiveCatalogService.tableExists(
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME));
    }

    @Test
    public void testIsTableExternalFalse() throws Exception {
        Assert.assertFalse(hiveCatalogService.isTableExternal(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME));
    }
    @Test
    public void testIsTableExternalTrue() throws Exception {
        Assert.assertTrue(hiveCatalogService.isTableExternal(conf, METASTORE_URL, DATABASE_NAME, EXTERNAL_TABLE_NAME));
    }

    @Test
    public void testListPartitionsByFilterNull() throws Exception {

        List<CatalogPartition> filteredPartitions = hiveCatalogService.listPartitionsByFilter(
            conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, null);
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
            conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, lessThanFilter);
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
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, greaterThanFilter);
        Assert.assertEquals(filteredPartitions.size(), expectedPartitionCount);
    }

    @Test
    public void testListPartititions() throws FalconException {
        List<String> filters = new ArrayList<String>();
        filters.add("20130903");
        List<CatalogPartition> partitions = hiveCatalogService.listPartitions(
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, filters);
        Assert.assertEquals(partitions.size(), 2);

        filters.add("us");
        partitions = hiveCatalogService.listPartitions(
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, filters);
        Assert.assertEquals(partitions.size(), 1);
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
        hiveCatalogService.dropPartition(
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, Arrays.asList("20130902", "in"), true);
        List<HCatPartition> partitions = client.getPartitions(DATABASE_NAME, TABLE_NAME);
        Assert.assertEquals(partitions.size(), 2, "Unexpected number of partitions");
        Assert.assertEquals(new String[]{"20130903", "in"},
                partitions.get(0).getValues().toArray(), "Mismatched partition");

        hiveCatalogService.dropPartitions(
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, Arrays.asList("20130903"), true);
        partitions = client.getPartitions(DATABASE_NAME, TABLE_NAME);
        Assert.assertEquals(partitions.size(), 0, "Unexpected number of partitions");
    }

    @Test
    public void testGetPartition() throws Exception {
        CatalogPartition partition = CatalogServiceFactory.getCatalogService().getPartition(
                conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, Arrays.asList("20130902", "in"));
        Assert.assertNotNull(partition);

        long createTime = partition.getCreateTime();
        Assert.assertTrue(createTime > 0);
    }

    @Test
    public void testReInstatePartition() throws Exception {
        Map<String, String> partitionSpec = new LinkedHashMap<String, String>();
        partitionSpec.put("ds", "20130918");
        partitionSpec.put("region", "blah");

        HCatAddPartitionDesc first = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, partitionSpec).build();
        client.addPartition(first);

        CatalogPartition partition = CatalogServiceFactory.getCatalogService().getPartition(
            conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, new ArrayList<String>(partitionSpec.values()));
        Assert.assertNotNull(partition);
        final long originalCreateTime = partition.getCreateTime();

        Thread.sleep(1000); // sleep before deletion
        client.dropPartitions(DATABASE_NAME, TABLE_NAME, partitionSpec, true);

        Thread.sleep(1000); // sleep so the next add is delayed a bit

        HCatAddPartitionDesc second = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, partitionSpec).build();
        client.addPartition(second);

        CatalogPartition reInstatedPartition = CatalogServiceFactory.getCatalogService().getPartition(
            conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, new ArrayList<String>(partitionSpec.values()));
        Assert.assertNotNull(reInstatedPartition);
        final long reInstatedCreateTime = reInstatedPartition.getCreateTime();

        Assert.assertTrue(reInstatedCreateTime > originalCreateTime);
    }

    @DataProvider (name = "tableName")
    public Object[][] createTableName() {
        return new Object[][] {
            {TABLE_NAME},
            {EXTERNAL_TABLE_NAME},
        };
    }

    @Test
    public void testGetPartitionColumns() throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        List<String> columns = catalogService.getPartitionColumns(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME);
        Assert.assertEquals(columns, Arrays.asList("ds", "region"));
    }

    @Test
    public void testAddPartition() throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        List<String> partitionValues = Arrays.asList("20130902", "us");
        String location = EXTERNAL_TABLE_LOCATION + "/20130902";
        catalogService.addPartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues, location);
        CatalogPartition partition =
                catalogService.getPartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues);
        Assert.assertEquals(partition.getLocation(), location);

        try {
            catalogService.addPartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues, location);
        } catch (FalconException e) {
            if (!(e.getCause() instanceof AlreadyExistsException)) {
                Assert.fail("Expected FalconException(AlreadyExistsException)");
            }
        }
    }

    @Test
    public void testUpdatePartition() throws FalconException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        List<String> partitionValues = Arrays.asList("20130902", "us");
        String location = EXTERNAL_TABLE_LOCATION + "/20130902";
        catalogService.addPartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues, location);
        CatalogPartition partition =
                catalogService.getPartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues);
        Assert.assertEquals(partition.getLocation(), location);

        String location2 = location + "updated";
        catalogService.updatePartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues, location2);
        partition = catalogService.getPartition(conf, METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionValues);
        Assert.assertEquals(partition.getLocation(), location2);
    }
}

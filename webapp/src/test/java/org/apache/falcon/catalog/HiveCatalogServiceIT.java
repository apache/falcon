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
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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

    private static final String DATABASE_NAME = "falcondb";
    private static final String TABLE_NAME = "foobar";
    private static final String METASTORE_URL = "thrift://localhost:49083";

    private HiveCatalogService hiveCatalogService;
    private HCatClient client;

    @BeforeClass
    public void setUp() throws Exception {
        hiveCatalogService = new HiveCatalogService();
        client = HiveCatalogService.get(METASTORE_URL);

        createDatabase();
        createTable();
        addPartition();
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

    private void addPartition() throws Exception {
        Map<String, String> firstPtn = new HashMap<String, String>();
        firstPtn.put("ds", "09/03/2013");
        firstPtn.put("region", "usa");
        HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, firstPtn).build();
        client.addPartition(addPtn);

        Map<String, String> secondPtn = new HashMap<String, String>();
        secondPtn.put("ds", "09/03/2013");
        secondPtn.put("region", "india");
        HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, secondPtn).build();
        client.addPartition(addPtn2);

        Map<String, String> thirdPtn = new HashMap<String, String>();
        thirdPtn.put("ds", "09/02/2013");
        thirdPtn.put("region", "india");
        HCatAddPartitionDesc addPtn3 = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, thirdPtn).build();
        client.addPartition(addPtn3);
    }

    @AfterClass
    public void tearDown() throws Exception {
        dropTable();
        dropDatabase();
    }

    private void dropTable() throws Exception {
        client.dropTable(DATABASE_NAME, TABLE_NAME, true);
    }

    private void dropDatabase() throws Exception {
        client.dropDatabase(DATABASE_NAME, true, HCatClient.DropDBMode.CASCADE);
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
    public void testListTableProperties() throws Exception {
        Map<String, String> tableProperties =
                hiveCatalogService.listTableProperties(METASTORE_URL, DATABASE_NAME, TABLE_NAME);
        Assert.assertEquals(tableProperties.get("database"), DATABASE_NAME);
        Assert.assertEquals(tableProperties.get("tableName"), TABLE_NAME);
        Assert.assertEquals(tableProperties.get("tabletype"), "MANAGED_TABLE");
        Assert.assertTrue(tableProperties.containsKey("location"));
    }
}

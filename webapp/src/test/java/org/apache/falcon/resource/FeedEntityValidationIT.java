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

package org.apache.falcon.resource;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.falcon.catalog.HiveCatalogService;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatCreateDBDesc;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Map;

/**
 * Tests feed entity validation to verify if the table specified is valid.
 */
public class FeedEntityValidationIT {

    private static final String METASTORE_URL = "thrift://localhost:49083";
    private static final String DATABASE_NAME = "falcondb";
    private static final String TABLE_NAME = "clicks";
    private static final String TABLE_URI =
            "catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}";

    private final TestContext context = new TestContext();
    private HCatClient client;

    @BeforeClass
    public void setup() throws Exception {
        TestContext.prepare();

        client = HiveCatalogService.get(METASTORE_URL);

        createDatabase();
        createTable();
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

        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(DATABASE_NAME, TABLE_NAME, cols)
                .fileFormat("rcfile")
                .ifNotExists(true)
                .comments("falcon integration test")
                .build();
        client.createTable(tableDesc);
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

    /**
     * Positive test.
     *
     * @throws Exception
     */
    @Test
    public void testFeedEntityWithValidTable() throws Exception {
        Map<String, String> overlay = context.getUniqueOverlay();
        overlay.put("colo", "default");

        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        // submission will parse and validate the feed with table
        overlay.put("tableUri", TABLE_URI);
        response = context.submitToFalcon("/hive-table-feed.xml", overlay, EntityType.FEED);
        context.assertSuccessful(response);
    }

    @DataProvider(name = "invalidTableUris")
    public Object[][] createInvalidTableUriData() {
        return new Object[][] {
            // does not match with group input's frequency
            {"catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"badscheme:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + DATABASE_NAME + ":" + "badtable" + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + "baddb" + ":" + TABLE_NAME + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
            {"catalog:" + "baddb" + ":" + "badtable" + "#ds=ds=${YEAR}-${MONTH}-${DAY}", ""},
        };
    }

    @Test (dataProvider = "invalidTableUris")
    public void testFeedEntityWithInvalidTableUri(String tableUri, String ignore)
        throws Exception {

        Map<String, String> overlay = context.getUniqueOverlay();
        overlay.put("colo", "default");

        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        // submission will parse and validate the feed with table
        overlay.put("tableUri", tableUri);
        response = context.submitToFalcon("/hive-table-feed.xml", overlay, EntityType.FEED);
        context.assertFailure(response);
    }
}

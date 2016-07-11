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

package org.apache.falcon.entity;

import org.apache.falcon.entity.v0.feed.LocationType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URISyntaxException;

/**
 * Test class for Catalog Table Storage.
 * Exists will be covered in integration tests as it actually checks if the table exists.
 */
public class CatalogStorageTest {

    @Test
    public void testGetType() throws Exception {
        String table = "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us";
        CatalogStorage storage = new CatalogStorage(CatalogStorage.CATALOG_URL, table);
        Assert.assertEquals(Storage.TYPE.TABLE, storage.getType());
        Assert.assertNotNull(storage.getConf());
    }

    @Test
    public void testParseFeedUriValid() throws URISyntaxException {
        String table = "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us";
        CatalogStorage storage = new CatalogStorage(CatalogStorage.CATALOG_URL, table);
        Assert.assertEquals("${hcatNode}", storage.getCatalogUrl());
        Assert.assertEquals("clicksdb", storage.getDatabase());
        Assert.assertEquals("clicks", storage.getTable());
        Assert.assertEquals(Storage.TYPE.TABLE, storage.getType());
        Assert.assertEquals(2, storage.getPartitions().size());
        Assert.assertEquals("us", storage.getPartitionValue("region"));
        Assert.assertTrue(storage.hasPartition("region"));
        Assert.assertNull(storage.getPartitionValue("unknown"));
        Assert.assertFalse(storage.hasPartition("unknown"));
        Assert.assertEquals(storage.getDatedPartitionKeys().get(0), "ds");
    }

    @Test
    public void testParseFeedUriValid2() throws URISyntaxException {
        String table = "catalog:clicksdb:clicks#ds=${YEAR}${MONTH}${DAY};region=us";
        CatalogStorage storage = new CatalogStorage(CatalogStorage.CATALOG_URL, table);
        Assert.assertEquals("${hcatNode}", storage.getCatalogUrl());
        Assert.assertEquals("clicksdb", storage.getDatabase());
        Assert.assertEquals("clicks", storage.getTable());
        Assert.assertEquals(Storage.TYPE.TABLE, storage.getType());
        Assert.assertEquals(2, storage.getPartitions().size());
        Assert.assertEquals("us", storage.getPartitionValue("region"));
        Assert.assertTrue(storage.hasPartition("region"));
        Assert.assertNull(storage.getPartitionValue("unknown"));
        Assert.assertFalse(storage.hasPartition("unknown"));
        Assert.assertEquals(storage.getDatedPartitionKeys().get(0), "ds");
    }

    @Test
    public void testCreateFromUriTemplate() throws Exception {
        String uriTemplate = "thrift://localhost:49083/clicksdb/clicks/region=us;ds=${YEAR}-${MONTH}-${DAY}";
        CatalogStorage storage = new CatalogStorage(uriTemplate);
        Assert.assertEquals("thrift://localhost:49083", storage.getCatalogUrl());
        Assert.assertEquals("clicksdb", storage.getDatabase());
        Assert.assertEquals("clicks", storage.getTable());
        Assert.assertEquals(Storage.TYPE.TABLE, storage.getType());
        Assert.assertEquals(2, storage.getPartitions().size());
        Assert.assertEquals("us", storage.getPartitionValue("region"));
        Assert.assertTrue(storage.hasPartition("region"));
        Assert.assertNull(storage.getPartitionValue("unknown"));
        Assert.assertFalse(storage.hasPartition("unknown"));
    }

    @DataProvider(name = "invalidFeedURITemplates")
    public Object[][] createInValidFeedUriTemplates() {
        return new Object[][] {
            {"thrift://localhost:49083/clicksdb/clicks/region=us;ds=${YEAR}/${MONTH}/${DAY}"},
            {"thrift://localhost:49083/clicksdb/clicks/region=us;ds=${YEAR}/${MONTH}-${DAY}"},
        };
    }

    @Test(dataProvider = "invalidFeedURITemplates", expectedExceptions = URISyntaxException.class)
    public void testParseInvalidFeedUriTemplate(String uriTemplate) throws URISyntaxException {
        new CatalogStorage(uriTemplate);
        Assert.fail("Exception must have been thrown");
    }

    @DataProvider(name = "invalidFeedURIs")
    public Object[][] createFeedUriInvalid() {
        return new Object[][] {
            {"catalog:default:clicks:ds=${YEAR}-${MONTH}-${DAY}#region=us"},
            {"default:clicks:ds=${YEAR}-${MONTH}-${DAY}#region=us"},
            {"catalog:default#ds=${YEAR}-${MONTH}-${DAY};region=us"},
            {"catalog://default/clicks#ds=${YEAR}-${MONTH}-${DAY}:region=us"},
        };
    }

    @Test(dataProvider = "invalidFeedURIs", expectedExceptions = URISyntaxException.class)
    public void testParseFeedUriInvalid(String tableUri) throws URISyntaxException {
        new CatalogStorage(CatalogStorage.CATALOG_URL, tableUri);
        Assert.fail("Exception must have been thrown");
    }

    @Test
    public void testIsIdenticalPositive() throws Exception {
        CatalogStorage table1 = new CatalogStorage(CatalogStorage.CATALOG_URL,
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        CatalogStorage table2 = new CatalogStorage(CatalogStorage.CATALOG_URL,
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        Assert.assertTrue(table1.isIdentical(table2));

        final String catalogUrl = "thrift://localhost:49083";
        CatalogStorage table3 = new CatalogStorage(catalogUrl,
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        CatalogStorage table4 = new CatalogStorage(catalogUrl,
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        Assert.assertTrue(table3.isIdentical(table4));
    }

    @Test
    public void testIsIdenticalNegative() throws Exception {
        CatalogStorage table1 = new CatalogStorage(CatalogStorage.CATALOG_URL,
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        CatalogStorage table2 = new CatalogStorage(CatalogStorage.CATALOG_URL,
                "catalog:clicksdb:impressions#ds=${YEAR}-${MONTH}-${DAY};region=us");
        Assert.assertFalse(table1.isIdentical(table2));

        final String catalogUrl = "thrift://localhost:49083";
        CatalogStorage table3 = new CatalogStorage(catalogUrl,
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        CatalogStorage table4 = new CatalogStorage(catalogUrl,
                "catalog:clicksdb:impressions#ds=${YEAR}-${MONTH}-${DAY};region=us");
        Assert.assertFalse(table3.isIdentical(table4));

        CatalogStorage table5 = new CatalogStorage("thrift://localhost:49084",
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        CatalogStorage table6 = new CatalogStorage("thrift://localhost:49083",
                "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us");
        Assert.assertFalse(table5.isIdentical(table6));
    }

    @Test
    public void testGetUriTemplateWithCatalogUrl() throws Exception {
        final String catalogUrl = "thrift://localhost:49083";
        String tableUri = "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us";
        String uriTemplate = "thrift://localhost:49083/clicksdb/clicks/ds=${YEAR}-${MONTH}-${DAY};region=us";

        CatalogStorage table = new CatalogStorage(catalogUrl, tableUri);
        Assert.assertNotNull(table.getConf());
        Assert.assertEquals(uriTemplate, table.getUriTemplate());
        Assert.assertEquals(uriTemplate, table.getUriTemplate(LocationType.DATA));
        Assert.assertEquals(table.getUriTemplate(), table.getUriTemplate(LocationType.DATA));
    }

    @Test
    public void testGetUriTemplateWithOutCatalogUrl() throws Exception {
        String tableUri = "catalog:clicksdb:clicks#ds=${YEAR}-${MONTH}-${DAY};region=us";
        String uriTemplate = "${hcatNode}/clicksdb/clicks/ds=${YEAR}-${MONTH}-${DAY};region=us";

        CatalogStorage table = new CatalogStorage(CatalogStorage.CATALOG_URL, tableUri);
        Assert.assertNotNull(table.getConf());
        Assert.assertEquals(uriTemplate, table.getUriTemplate());
        Assert.assertEquals(uriTemplate, table.getUriTemplate(LocationType.DATA));
        Assert.assertEquals(table.getUriTemplate(), table.getUriTemplate(LocationType.DATA));
    }

    @Test
    public void testToPartitionFilter() throws Exception {
        final String catalogUrl = "thrift://localhost:49083";
        String tableUri = "catalog:clicksdb:clicks#ds=20130918;region=us";
        String partitionFilter = "(ds='20130918';region='us')";
        CatalogStorage table = new CatalogStorage(catalogUrl, tableUri);
        Assert.assertEquals(table.toPartitionFilter(), partitionFilter);
        Assert.assertNotNull(table.getConf());
    }

    @Test
    public void testToPartitionAsPath() throws Exception {
        final String catalogUrl = "thrift://localhost:49083";
        String tableUri = "catalog:clicksdb:clicks#ds=20130918;region=us";
        String partitionPath = "ds=20130918/region=us";
        CatalogStorage table = new CatalogStorage(catalogUrl, tableUri);
        Assert.assertEquals(table.toPartitionAsPath(), partitionPath);
        Assert.assertNotNull(table.getConf());
    }

    @Test
    public void testCreateFromURL() throws Exception {
        String url = "thrift://localhost:29083/falcon_db/output_table/ds=2012-04-21-00";
        CatalogStorage storage = new CatalogStorage(url);
        Assert.assertEquals("thrift://localhost:29083", storage.getCatalogUrl());
        Assert.assertEquals("falcon_db", storage.getDatabase());
        Assert.assertEquals("output_table", storage.getTable());
        Assert.assertEquals(Storage.TYPE.TABLE, storage.getType());
        Assert.assertEquals(1, storage.getPartitions().size());
        Assert.assertEquals("2012-04-21-00", storage.getPartitionValue("ds"));
        Assert.assertTrue(storage.hasPartition("ds"));
        Assert.assertNotNull(storage.getConf());
    }
}

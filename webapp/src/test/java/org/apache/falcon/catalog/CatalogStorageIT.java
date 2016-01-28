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

import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedInstanceStatus;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.TimeZone;


/**
 * Tests Hive Meta Store service.
 */
public class CatalogStorageIT {

    private static final String METASTORE_URL = "thrift://localhost:49083";
    private static final String DATABASE_NAME = "CatalogStorageITDB";
    private static final String TABLE_NAME = "CatalogStorageITTable";

    private HCatClient client;
    private Feed feed = new Feed();
    private org.apache.falcon.entity.v0.cluster.Cluster cluster = new org.apache.falcon.entity.v0.cluster.Cluster();
    private DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
    private CatalogStorage storage;

    @BeforeClass
    public void setUp() throws Exception {
        // setup a logged in user
        CurrentUser.authenticate(TestContext.REMOTE_USER);
        client = TestContext.getHCatClient(METASTORE_URL);

        HiveTestUtils.createDatabase(METASTORE_URL, DATABASE_NAME);
        List<String> partitionKeys = new ArrayList<String>();
        partitionKeys.add("ds");
        partitionKeys.add("region");
        HiveTestUtils.createTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionKeys);
        addPartitions();
        addClusterAndFeed();
    }

    private void addClusterAndFeed() throws Exception {
        cluster.setName("testCluster");
        Interfaces interfaces = new Interfaces();
        Interface registry = new Interface();
        registry.setType(Interfacetype.REGISTRY);
        registry.setEndpoint(METASTORE_URL);
        interfaces.getInterfaces().add(registry);
        cluster.setInterfaces(interfaces);

        feed.setName("feed");
        Frequency f = new Frequency("days(1)");
        feed.setFrequency(f);
        feed.setTimezone(TimeZone.getTimeZone("UTC"));
        Clusters fClusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster fCluster = new org.apache.falcon.entity.v0.feed.Cluster();
        fCluster.setType(ClusterType.SOURCE);
        fCluster.setName("testCluster");
        Validity validity = new Validity();
        validity.setStart(format.parse("2013-09-01 00:00 UTC"));
        validity.setEnd(format.parse("2013-09-06 00:00 UTC"));
        fCluster.setValidity(validity);
        fClusters.getClusters().add(fCluster);
        feed.setClusters(fClusters);

        initCatalogService();
    }

    private void initCatalogService() throws Exception {
        CatalogTable table = new CatalogTable();
        String uri = "catalog:" + DATABASE_NAME + ":" + TABLE_NAME + "#ds=${YEAR}${MONTH}${DAY};region=us";
        table.setUri(uri);
        feed.setTable(table);

        storage = new CatalogStorage(cluster, table);
        Configuration configuration = HiveCatalogService.createHiveConf(new Configuration(), storage.getCatalogUrl());
        storage.setConf(configuration);
    }


    @AfterClass
    public void tearDown() throws Exception {
        dropTable(TABLE_NAME);
        dropDatabase();
        TestContext.deleteEntitiesFromStore();
    }

    private void dropTable(String tableName) throws Exception {
        client.dropTable(DATABASE_NAME, tableName, true);
    }

    private void dropDatabase() throws Exception {
        client.dropDatabase(DATABASE_NAME, true, HCatClient.DropDBMode.CASCADE);
    }

    private void addPartitions() throws Exception {
        putPartition("20130901", "us");
        putPartition("20130902", "us");
        putPartition("20130904", "us");
        putPartition("20130905", "us");
    }

    private void putPartition(String date, String region) throws HCatException {
        Map<String, String> partition = new HashMap<String, String>();
        partition.put("ds", date); //yyyyMMDD
        partition.put("region", region);
        HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(
                DATABASE_NAME, TABLE_NAME, null, partition).build();
        client.addPartition(addPtn);
    }

    @Test
    public void testGetInstanceAvailabilityStatus() throws Exception {
        List<FeedInstanceStatus> instanceStatuses = storage.getListing(feed, cluster.getName(),
                LocationType.DATA, format.parse("2013-09-02 00:00 UTC"), format.parse("2013-09-04 00:00 UTC"));
        Assert.assertEquals(instanceStatuses.size(), 3);
    }

    @Test
    public void testGetListing() throws Exception {
        FeedInstanceStatus.AvailabilityStatus availabilityStatus = storage.getInstanceAvailabilityStatus(
                feed, cluster.getName(),
                LocationType.DATA, format.parse("2013-09-03 00:00 UTC"));
        Assert.assertEquals(availabilityStatus, FeedInstanceStatus.AvailabilityStatus.MISSING);
    }

}

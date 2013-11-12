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

import org.apache.falcon.entity.parser.ClusterEntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.FeedEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;

/**
 * Test for storage factory methods in feed helper.
 */
public class StorageFactoryTest {

    private static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";

    private static final String FS_FEED_UNIFORM = "/config/feed/feed-0.1.xml";
    private static final String FS_FEED_OVERRIDE = "/config/feed/feed-0.2.xml";

    private static final String TABLE_FEED_UNIFORM = "/config/feed/hive-table-feed.xml";
    private static final String TABLE_FEED_OVERRIDE = "/config/feed/hive-table-feed-out.xml";

    private static final String OVERRIDE_TBL_LOC = "/testCluster/clicks-summary/ds=${YEAR}-${MONTH}-${DAY}-${HOUR}";

    private final ClusterEntityParser clusterParser =
            (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);
    private final FeedEntityParser feedParser =
            (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);

    private Cluster clusterEntity;
    private Feed fsFeedWithUniformStorage;
    private Feed fsFeedWithOverriddenStorage;
    private Feed tableFeedWithUniformStorage;
    private Feed tableFeedWithOverriddenStorage;

    @BeforeClass
    public void setup() throws Exception {
        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);
        clusterEntity = clusterParser.parse(stream);
        stream.close();
        Interface registry = ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY);
        registry.setEndpoint("thrift://localhost:9083");
        ConfigurationStore.get().publish(EntityType.CLUSTER, clusterEntity);

        stream = this.getClass().getResourceAsStream(FS_FEED_UNIFORM);
        fsFeedWithUniformStorage = feedParser.parse(stream);
        stream.close();

        stream = this.getClass().getResourceAsStream(FS_FEED_OVERRIDE);
        fsFeedWithOverriddenStorage = feedParser.parse(stream);
        stream.close();

        stream = this.getClass().getResourceAsStream(TABLE_FEED_UNIFORM);
        tableFeedWithUniformStorage = feedParser.parse(stream);
        stream.close();

        stream = this.getClass().getResourceAsStream(TABLE_FEED_OVERRIDE);
        tableFeedWithOverriddenStorage = feedParser.parse(stream);
        stream.close();
    }

    @AfterClass
    public void tearDown() throws Exception {
        ConfigurationStore.get().remove(EntityType.CLUSTER, clusterEntity.getName());
    }

    @DataProvider (name = "locationsDataProvider")
    private Object[][] createLocationsDataProvider() {
        return new Object[][] {
            {fsFeedWithUniformStorage, "/projects/falcon/clicks"},
            {fsFeedWithOverriddenStorage, "/testCluster/projects/falcon/clicks"},
        };
    }

    @Test (dataProvider = "locationsDataProvider")
    public void testGetLocations(Feed feed, String dataPath) {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(feed, clusterEntity.getName());
        List<Location> locations = FeedHelper.getLocations(feedCluster, feed);
        for (Location location : locations) {
            if (location.getType() == LocationType.DATA) {
                Assert.assertEquals(location.getPath(), dataPath);
            }
        }
    }

    @DataProvider (name = "tableDataProvider")
    private Object[][] createTableDataProvider() {
        return new Object[][] {
            {tableFeedWithUniformStorage, "catalog:default:clicks#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}"},
            {tableFeedWithOverriddenStorage, "catalog:testCluster:clicks-summary#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}"},
        };
    }

    @Test (dataProvider = "tableDataProvider")
    public void testGetTable(Feed feed, String dataPath) {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(feed, clusterEntity.getName());
        CatalogTable table = FeedHelper.getTable(feedCluster, feed);
        Assert.assertEquals(table.getUri(), dataPath);
    }

    private static final String UNIFORM_TABLE = "${hcatNode}/default/clicks/ds=${YEAR}-${MONTH}-${DAY}-${HOUR}";
    private static final String OVERRIDETBL = "${hcatNode}/default/clicks-summary/ds=${YEAR}-${MONTH}-${DAY}-${HOUR}";


    @DataProvider (name = "uniformFeedStorageDataProvider")
    private Object[][] createUniformFeedStorageDataProvider() {
        return new Object[][] {
            {fsFeedWithUniformStorage, Storage.TYPE.FILESYSTEM, "${nameNode}/projects/falcon/clicks"},
            {fsFeedWithOverriddenStorage, Storage.TYPE.FILESYSTEM, "${nameNode}/projects/falcon/clicks"},
            {tableFeedWithUniformStorage, Storage.TYPE.TABLE, UNIFORM_TABLE},
            {tableFeedWithOverriddenStorage, Storage.TYPE.TABLE, OVERRIDETBL},
        };
    }

    @Test (dataProvider = "uniformFeedStorageDataProvider")
    public void testCreateStorageWithFeed(Feed feed, Storage.TYPE storageType,
                                            String dataLocation) throws Exception {
        Storage storage = FeedHelper.createStorage(feed);
        Assert.assertEquals(storage.getType(), storageType);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), dataLocation);

        if (storageType == Storage.TYPE.TABLE) {
            Assert.assertEquals(((CatalogStorage) storage).getDatabase(), "default");
        }
    }

    @DataProvider (name = "overriddenFeedStorageDataProvider")
    private Object[][] createFeedStorageDataProvider() {
        return new Object[][] {
            {fsFeedWithUniformStorage, Storage.TYPE.FILESYSTEM, "/projects/falcon/clicks"},
            {fsFeedWithOverriddenStorage, Storage.TYPE.FILESYSTEM, "/testCluster/projects/falcon/clicks"},
            {tableFeedWithUniformStorage, Storage.TYPE.TABLE, "/default/clicks/ds=${YEAR}-${MONTH}-${DAY}-${HOUR}"},
            {tableFeedWithOverriddenStorage, Storage.TYPE.TABLE, OVERRIDE_TBL_LOC},
        };
    }

    @Test (dataProvider = "overriddenFeedStorageDataProvider")
    public void testCreateStorageWithFeedAndClusterEntity(Feed feed, Storage.TYPE storageType,
                                                          String dataLocation) throws Exception {
        Storage storage = FeedHelper.createStorage(clusterEntity, feed);
        Assert.assertEquals(storage.getType(), storageType);

        if (storageType == Storage.TYPE.FILESYSTEM) {
            dataLocation = ClusterHelper.getStorageUrl(clusterEntity) + dataLocation;
        } else if (storageType == Storage.TYPE.TABLE) {
            dataLocation =
                    ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).getEndpoint() + dataLocation;
        }

        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), dataLocation);
    }

    @Test (dataProvider = "overriddenFeedStorageDataProvider")
    public void testCreateStorageWithFeedAndClusterName(Feed feed, Storage.TYPE storageType,
                                                        String dataLocation) throws Exception {
        Storage storage = FeedHelper.createStorage(clusterEntity.getName(), feed);
        Assert.assertEquals(storage.getType(), storageType);

        if (storageType == Storage.TYPE.FILESYSTEM) {
            dataLocation = ClusterHelper.getStorageUrl(clusterEntity) + dataLocation;
        } else if (storageType == Storage.TYPE.TABLE) {
            dataLocation =
                    ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).getEndpoint() + dataLocation;
        }

        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), dataLocation);
    }

    @Test (dataProvider = "overriddenFeedStorageDataProvider")
    public void testCreateStorageWithFeedAndFeedCluster(Feed feed, Storage.TYPE storageType,
                                                        String dataLocation) throws Exception {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(feed, clusterEntity.getName());
        Storage storage = FeedHelper.createStorage(feedCluster, feed);
        Assert.assertEquals(storage.getType(), storageType);

        if (storageType == Storage.TYPE.FILESYSTEM) {
            dataLocation = ClusterHelper.getStorageUrl(clusterEntity) + dataLocation;
        } else if (storageType == Storage.TYPE.TABLE) {
            dataLocation =
                    ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).getEndpoint() + dataLocation;
        }

        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), dataLocation);
    }

    @Test (dataProvider = "overriddenFeedStorageDataProvider")
    public void testCreateStorageWithAll(Feed feed, Storage.TYPE storageType,
                                         String dataLocation) throws Exception {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(feed, clusterEntity.getName());
        Storage storage = FeedHelper.createStorage(feedCluster, feed, clusterEntity);
        Assert.assertEquals(storage.getType(), storageType);

        if (storageType == Storage.TYPE.FILESYSTEM) {
            dataLocation = ClusterHelper.getStorageUrl(clusterEntity) + dataLocation;
        } else if (storageType == Storage.TYPE.TABLE) {
            dataLocation =
                    ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).getEndpoint() + dataLocation;
        }

        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), dataLocation);
    }

    @Test (dataProvider = "overriddenFeedStorageDataProvider")
    public void testCreateReadOnlyStorage(Feed feed, Storage.TYPE storageType,
                                          String dataLocation) throws Exception {
        Storage readOnlyStorage = FeedHelper.createReadOnlyStorage(clusterEntity, feed);
        Assert.assertEquals(readOnlyStorage.getType(), storageType);

        if (storageType == Storage.TYPE.FILESYSTEM) {
            dataLocation = ClusterHelper.getReadOnlyStorageUrl(clusterEntity) + dataLocation;
        } else if (storageType == Storage.TYPE.TABLE) {
            dataLocation =
                    ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).getEndpoint() + dataLocation;
        }

        Assert.assertEquals(readOnlyStorage.getUriTemplate(LocationType.DATA), dataLocation);
    }

    @DataProvider (name = "uriTemplateDataProvider")
    private Object[][] createUriTemplateDataProvider() {
        return new Object[][] {
            {Storage.TYPE.FILESYSTEM, "/projects/falcon/clicks"},
            {Storage.TYPE.FILESYSTEM, "/testCluster/projects/falcon/clicks"},
            {Storage.TYPE.TABLE, "/default/clicks/ds=${YEAR}-${MONTH}-${DAY}-${HOUR}"},
            {Storage.TYPE.TABLE, OVERRIDE_TBL_LOC},
        };
    }

    @Test (dataProvider = "uriTemplateDataProvider")
    public void testCreateStorageWithUriTemplate(Storage.TYPE storageType,
                                                 String dataLocation) throws Exception {
        String uriTemplate = null;
        if (storageType == Storage.TYPE.FILESYSTEM) {
            uriTemplate = "DATA=" + ClusterHelper.getStorageUrl(clusterEntity) + dataLocation + "#";
            dataLocation = ClusterHelper.getStorageUrl(clusterEntity) + dataLocation;
        } else if (storageType == Storage.TYPE.TABLE) {
            uriTemplate =
                    ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).getEndpoint() + dataLocation;
            dataLocation = uriTemplate;
        }

        Storage storage = FeedHelper.createStorage(storageType.name(), uriTemplate);
        Assert.assertEquals(storage.getType(), storageType);
        Assert.assertEquals(storage.getUriTemplate(LocationType.DATA), dataLocation);
    }

    @DataProvider (name = "storageTypeDataProvider")
    private Object[][] createStorageTypeDataProvider() {
        return new Object[][] {
            {fsFeedWithUniformStorage, Storage.TYPE.FILESYSTEM},
            {fsFeedWithOverriddenStorage, Storage.TYPE.FILESYSTEM},
            {tableFeedWithUniformStorage, Storage.TYPE.TABLE},
            {tableFeedWithOverriddenStorage, Storage.TYPE.TABLE},
        };
    }

    @Test (dataProvider = "storageTypeDataProvider")
    public void testGetStorageTypeWithFeed(Feed feed, Storage.TYPE expectedStorageType) throws Exception {
        Storage.TYPE actualStorageType = FeedHelper.getStorageType(feed);
        Assert.assertEquals(actualStorageType, expectedStorageType);

        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(feed, clusterEntity.getName());
        actualStorageType = FeedHelper.getStorageType(feed, feedCluster);
        Assert.assertEquals(actualStorageType, expectedStorageType);

        actualStorageType = FeedHelper.getStorageType(feed, clusterEntity);
        Assert.assertEquals(actualStorageType, expectedStorageType);
    }
}

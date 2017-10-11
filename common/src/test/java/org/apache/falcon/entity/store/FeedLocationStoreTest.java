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

package org.apache.falcon.entity.store;

import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.FalconRadixUtils;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;


/**
 * Tests for FeedLocationStore.
 */
public class FeedLocationStoreTest extends AbstractTestBase {
    private ConfigurationStore store;

    @BeforeClass
    public void initConfigStore() throws Exception {
        String configPath = new URI(StartupProperties.get().getProperty("config.store.uri")).getPath();
        String location = configPath + "-" + getClass().getName();
        StartupProperties.get().setProperty("config.store.uri", location);
        FileUtils.deleteDirectory(new File(location));

        cleanupStore();
        String listeners = StartupProperties.get().getProperty("configstore.listeners");
        listeners = listeners.replace("org.apache.falcon.service.SharedLibraryHostingService", "");
        listeners = listeners.replace("org.apache.falcon.service.EntitySLAMonitoringService", "");
        StartupProperties.get().setProperty("configstore.listeners", listeners);
        store = ConfigurationStore.get();
        store.init();
        CurrentUser.authenticate(FalconTestUtil.TEST_USER_2);

    }
    @BeforeMethod
    public void setUp() throws FalconException{
        cleanupStore();
        createClusters();
    }

    @AfterMethod
    public void print() {
        System.out.printf("%s", FeedLocationStore.get().store);
    }

    @Test
    public void testOnAddSameLocation() throws FalconException{
        Feed f1 = createFeed("f1SameLocations");
        int initialSize = FeedLocationStore.get().store.getSize();
        f1.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/data/hourly/2014/09/09/09"));
        f1.getLocations().getLocations().add(createLocation(LocationType.STATS,
                "/projects/cas/stats/hourly/2014/09/09/09"));

        Feed f2 = createFeed("f2SameLocations");
        f2.getLocations().getLocations().add(createLocation(LocationType.STATS,
                "/projects/cas/data/hourly/2014/09/09/09"));
        f2.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/stats/hourly/2014/09/09/09"));

        store.publish(EntityType.FEED, f1);
        store.publish(EntityType.FEED, f2);
        int finalSize = FeedLocationStore.get().store.getSize();
        Assert.assertEquals(finalSize - initialSize, 8);
    }

    @Test
    public void testOnUpdate() throws FalconException{
        Feed f1 = createFeed("f1");
        f1.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/data/hourly/2014/09/09/09"));
        store.publish(EntityType.FEED, f1);

        Feed f2 = createFeed("f1");
        f2.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/data/monthly"));
        store.initiateUpdate(f2);
        store.update(EntityType.FEED, f2);
        store.cleanupUpdateInit();
        boolean isArchived = false;
        try {
            Path archivePath = new Path(store.getStorePath(), "archive" + Path.SEPARATOR + "FEED");
            FileStatus [] files= store.getFs().listStatus(archivePath);
            for(FileStatus f:files){
                String name = f.getPath().getName();
                if (name.startsWith(f2.getName())){
                    isArchived= true;
                    break;
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        Assert.assertTrue(isArchived);
    }


    @Test
    public void testOnRemove() throws FalconException{
        int initialSize = FeedLocationStore.get().store.getSize();
        String feedName = "f1ForRemove";
        Feed f1 = createFeed(feedName);
        f1.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/data/hourly/2014/09/09/09"));
        f1.getLocations().getLocations().add(createLocation(LocationType.STATS,
                "/projects/cas/data/hourly/2014/09/09/09"));

        store.publish(EntityType.FEED, f1);
        Assert.assertEquals(FeedLocationStore.get().store.getSize() - initialSize, 4);
        store.remove(EntityType.FEED, feedName);
        boolean isArchived = false;
        try {
            Path archivePath = new Path(store.getStorePath(), "archive" + Path.SEPARATOR + "FEED");
            FileStatus [] files= store.getFs().listStatus(archivePath);
            for(FileStatus f:files){
                String name = f.getPath().getName();
                if (name.startsWith(feedName)){
                    isArchived= true;
                    break;
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        Assert.assertTrue(isArchived);
        Assert.assertEquals(FeedLocationStore.get().store.getSize(), initialSize);
    }


    @Test
    public void testOnChange() throws FalconException{
        Feed f1 = createFeed("f1");
        f1.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/data/hourly/2014/09/09/09"));
        store.publish(EntityType.FEED, f1);

        Feed f2 = createFeed("f1");
        f2.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/projects/cas/data/monthly"));
        store.initiateUpdate(f2);
        store.update(EntityType.FEED, f2);
        store.cleanupUpdateInit();

        Feed f3 = createFeed("f2");
        f3.getLocations().getLocations().add(createLocation(LocationType.STATS,
                "/projects/cas/data/hourly/2014/09/09/09"));
        store.publish(EntityType.FEED, f3);

    }

    @Test
    public void testWithClusterLocations() throws FalconException {
        Feed f = createFeedWithClusterLocations("clusterFeed");
        int initialSize = FeedLocationStore.get().store.getSize();
        store.publish(EntityType.FEED, f);
        Assert.assertEquals(FeedLocationStore.get().store.getSize() - initialSize, 6);
        store.remove(EntityType.FEED, "clusterFeed");
        Assert.assertEquals(FeedLocationStore.get().store.getSize(), initialSize);
    }


    @Test
    public void testFindWithRegularExpression() throws FalconException {
        Feed f = createFeed("findUsingRegexFeed");
        f.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/falcon/test/input/${YEAR}/${MONTH}/${DAY}/${HOUR}"));
        store.publish(EntityType.FEED, f);
        Assert.assertNotNull(FeedLocationStore.get().store.find("/falcon/test/input/2014/12/12/23",
                new FalconRadixUtils.FeedRegexAlgorithm()));
    }

    @Test
    public void testAddCatalogStorageFeeds() throws FalconException {
        //this test ensure that catalog feeds are ignored in FeedLocationStore
        Feed f = createCatalogFeed("catalogFeed");
        store.publish(EntityType.FEED, f);
        Assert.assertTrue(true);
    }

    private Feed createCatalogFeed(String name) {
        Feed f = new Feed();
        f.setName(name);
        f.setClusters(createBlankClusters());
        f.setTable(new CatalogTable());
        return f;
    }

    private Feed createFeed(String name){
        Feed f = new Feed();
        Locations locations = new Locations();
        f.setLocations(locations);
        f.setName(name);
        f.setClusters(createBlankClusters());
        return f;
    }


    private Feed createFeedWithClusterLocations(String name) {
        Feed f = new Feed();
        f.setLocations(new Locations());
        f.getLocations().getLocations().add(createLocation(LocationType.DATA, "/projects/cas/data"));
        f.getLocations().getLocations().add(createLocation(LocationType.STATS, "/projects/cas/stats"));
        f.getLocations().getLocations().add(createLocation(LocationType.META, "/projects/cas/meta"));
        f.setName(name);
        f.setClusters(createClustersWithLocations());
        return f;
    }

    private Location createLocation(LocationType type, String path){
        Location location = new Location();
        location.setPath(path);
        location.setType(type);
        return location;
    }

    protected void cleanupStore() throws FalconException {
        store = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }

    private Clusters createClustersWithLocations() {
        Clusters clusters = new Clusters();
        Cluster cluster1 = new Cluster();
        cluster1.setName("cluster1WithLocations");
        cluster1.setLocations(new Locations());
        cluster1.getLocations().getLocations().add(createLocation(LocationType.DATA, "/projects/cas/cluster1/data"));
        cluster1.getLocations().getLocations().add(createLocation(LocationType.STATS, "/projects/cas/cluster1/stats"));
        cluster1.getLocations().getLocations().add(createLocation(LocationType.META, "/projects/cas/cluster1/meta"));

        Cluster cluster2 = new Cluster();
        cluster2.setName("cluster2WithLocations");
        cluster2.setLocations(new Locations());
        cluster2.getLocations().getLocations().add(createLocation(LocationType.DATA, "/projects/cas/cluster2/data"));
        cluster2.getLocations().getLocations().add(createLocation(LocationType.STATS, "/projects/cas/cluster2/stats"));
        cluster2.getLocations().getLocations().add(createLocation(LocationType.META, "/projects/cas/cluster2/meta"));

        clusters.getClusters().add(cluster1);
        clusters.getClusters().add(cluster2);

        return clusters;
    }

    private Clusters createBlankClusters() {
        Clusters clusters = new Clusters();

        Cluster cluster = new Cluster();
        cluster.setName("blankCluster1");
        clusters.getClusters().add(cluster);

        Cluster cluster2 = new Cluster();
        cluster2.setName("blankCluster2");
        clusters.getClusters().add(cluster2);

        return clusters;
    }

    private void createClusters() throws FalconException {
        String[] clusterNames = {"cluster1WithLocations", "cluster2WithLocations", "blankCluster1", "blankCluster2"};
        for (String name : clusterNames) {
            org.apache.falcon.entity.v0.cluster.Cluster cluster = new org.apache.falcon.entity.v0.cluster.Cluster();
            cluster.setName(name);
            cluster.setColo("default");
            store.publish(EntityType.CLUSTER, cluster);
        }
    }
}

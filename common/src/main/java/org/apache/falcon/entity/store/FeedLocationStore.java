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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.FalconRadixUtils;
import org.apache.falcon.util.RadixTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 *  A <Key, Value> Store to store FeedProperties against Feed's Locations.
 *
 * For example:
 * let's say a feed - <b>MyFeed</b>, is configured for two clusters - cluster1 and cluster2 and has data location as
 * below.
 * /projects/myprocess/data/${MONTH}-${DAY}-${HOUR}
 * /projects/myprocess/meta/${MONTH}-${DAY}-${HOUR}
 *
 * then the key,value store will be like below
 * key1: /projects/myprocess/data/${MONTH}-${DAY}-${HOUR}
 * value1: [FeedProperties("cluster1", LocationType.DATA, "MyFeed"),
 *          FeedProperties("cluster2", LocationType.DATA, "MyFeed")
 *         ]
 *
 * key2: /projects/myprocess/meta/${MONTH}-${DAY}-${HOUR}
 * value2: [FeedProperties("cluster1", LocationType.META, "MyFeed"),
 *          FeedProperties("cluster2", LocationType.META, "MyFeed")
 *         ]
 *
 * It ensures that no two Feeds share the same location.
 * It can also be used for operations like:
 * <ul>
 *     <li>Find if a there is a feed which uses a given path as it's location.</li>
 *     <li>Find name of the feed, given it's location.</li>
 * </ul>
 */
public final class FeedLocationStore implements ConfigurationChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(FeedLocationStore.class);
    protected final FeedPathStore<FeedLookupResult.FeedProperties> store = new
            RadixTree<FeedLookupResult.FeedProperties>();

    private static FeedLocationStore instance = new FeedLocationStore();

    private FeedLocationStore(){
    }

    public static FeedLocationStore get(){
        return instance;
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        if (entity.getEntityType() == EntityType.FEED){
            Feed feed = (Feed) entity;
            List<Cluster> clusters = feed.getClusters().getClusters();
            for(Cluster cluster: clusters) {
                if (DeploymentUtil.getCurrentClusters().contains(cluster.getName())) {
                    List<Location> clusterSpecificLocations = FeedHelper.getLocations(FeedHelper.getCluster(feed,
                            cluster.getName()), feed);
                    if (clusterSpecificLocations != null) {
                        for (Location location : clusterSpecificLocations) {
                            if (location != null && StringUtils.isNotBlank(location.getPath())) {
                                FeedLookupResult.FeedProperties value = new FeedLookupResult.FeedProperties(
                                        feed.getName(), location.getType(), cluster.getName());
                                store.insert(StringUtils.trim(location.getPath()), value);
                                LOG.debug("Inserted location: {} for feed: {} and cluster: {}",
                                        location.getPath(), feed.getName(), cluster.getName());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Delete the key(path) from the store if the feed is deleted.
     * @param entity entity object
     * @throws FalconException
     */
    @Override
    public void onRemove(Entity entity) throws FalconException {
        if (entity.getEntityType() == EntityType.FEED){

            Feed feed = (Feed) entity;
            List<Cluster> clusters = feed.getClusters().getClusters();
            for(Cluster cluster: clusters){
                List<Location> clusterSpecificLocations = FeedHelper.getLocations(FeedHelper.getCluster(feed,
                        cluster.getName()), feed);
                if (clusterSpecificLocations != null) {
                    for(Location location: clusterSpecificLocations){
                        if (location != null && StringUtils.isNotBlank(location.getPath())){
                            FeedLookupResult.FeedProperties value = new FeedLookupResult.FeedProperties(feed.getName(),
                                    location.getType(), cluster.getName());
                            LOG.debug("Delete called for location: {} for feed: {} and cluster: {}",
                                    location.getPath(), feed.getName(), cluster.getName());
                            store.delete(location.getPath(), value);
                            LOG.debug("Deleted location: {} for feed: {} and cluster: {}",
                                    location.getPath(), feed.getName(), cluster.getName());
                        }
                    }
                }
            }
        }

    }

    /**
     * Delete the old path and insert the new Path when the feed is updated.
     * @param oldEntity old entity object
     * @param newEntity updated entity object
     * @throws FalconException if the new path already exists in the store.
     */
    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        onRemove(oldEntity);
        onAdd(newEntity);
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        onAdd(entity);
    }


    public Collection<FeedLookupResult.FeedProperties> reverseLookup(String path) {
        return store.find(path, new FalconRadixUtils.FeedRegexAlgorithm());
    }
}

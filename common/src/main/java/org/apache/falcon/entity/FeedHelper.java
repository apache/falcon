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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.expression.ExpressionHelper;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Feed entity helper methods.
 */
public final class FeedHelper {

    private FeedHelper() {}

    public static Cluster getCluster(Feed feed, String clusterName) {
        for (Cluster cluster : feed.getClusters().getClusters()) {
            if (cluster.getName().equals(clusterName)) {
                return cluster;
            }
        }
        return null;
    }

    public static Storage createStorage(Feed feed) throws FalconException {

        final Locations feedLocations = feed.getLocations();
        if (feedLocations != null
                && feedLocations.getLocations().size() != 0) {
            return new FileSystemStorage(feed);
        }

        try {
            final CatalogTable table = feed.getTable();
            if (table != null) {
                return new CatalogStorage(feed);
            }
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage createStorage(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                        Feed feed) throws FalconException {
        return createStorage(getCluster(feed, clusterEntity.getName()), feed, clusterEntity);
    }

    public static Storage createStorage(String clusterName, Feed feed)
        throws FalconException {

        return createStorage(getCluster(feed, clusterName), feed);
    }

    public static Storage createStorage(Cluster cluster, Feed feed)
        throws FalconException {

        final org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                EntityUtil.getEntity(EntityType.CLUSTER, cluster.getName());

        return createStorage(cluster, feed, clusterEntity);
    }

    public static Storage createStorage(Cluster cluster, Feed feed,
                                        org.apache.falcon.entity.v0.cluster.Cluster clusterEntity)
        throws FalconException {

        final List<Location> locations = getLocations(cluster, feed);
        if (locations != null) {
            return new FileSystemStorage(ClusterHelper.getStorageUrl(clusterEntity), locations);
        }

        try {
            final CatalogTable table = getTable(cluster, feed);
            if (table != null) {
                return new CatalogStorage(clusterEntity, table);
            }
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    /**
     * Factory method to dole out a storage instance used for replication source.
     *
     * @param clusterEntity cluster entity
     * @param feed feed entity
     * @return an implementation of Storage
     * @throws FalconException
     */
    public static Storage createReadOnlyStorage(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                                Feed feed) throws FalconException {
        Cluster feedCluster = getCluster(feed, clusterEntity.getName());
        final List<Location> locations = getLocations(feedCluster, feed);
        if (locations != null) {
            return new FileSystemStorage(ClusterHelper.getReadOnlyStorageUrl(clusterEntity), locations);
        }

        try {
            final CatalogTable table = getTable(feedCluster, feed);
            if (table != null) {
                return new CatalogStorage(clusterEntity, table);
            }
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage createStorage(String type, String storageUriTemplate)
        throws URISyntaxException {

        Storage.TYPE storageType = Storage.TYPE.valueOf(type);
        if (storageType == Storage.TYPE.FILESYSTEM) {
            return new FileSystemStorage(storageUriTemplate);
        } else if (storageType == Storage.TYPE.TABLE) {
            return new CatalogStorage(storageUriTemplate);
        }

        throw new IllegalArgumentException("Bad type: " + type);
    }

    public static Storage.TYPE getStorageType(Feed feed) throws FalconException {
        final Locations feedLocations = feed.getLocations();
        if (feedLocations != null
                && feedLocations.getLocations().size() != 0) {
            return Storage.TYPE.FILESYSTEM;
        }

        final CatalogTable table = feed.getTable();
        if (table != null) {
            return Storage.TYPE.TABLE;
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage.TYPE getStorageType(Feed feed,
                                              Cluster cluster) throws FalconException {
        final List<Location> locations = getLocations(cluster, feed);
        if (locations != null) {
            return Storage.TYPE.FILESYSTEM;
        }

        final CatalogTable table = getTable(cluster, feed);
        if (table != null) {
            return Storage.TYPE.TABLE;
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage.TYPE getStorageType(Feed feed,
                                              org.apache.falcon.entity.v0.cluster.Cluster clusterEntity)
        throws FalconException {
        Cluster feedCluster = getCluster(feed, clusterEntity.getName());
        return getStorageType(feed, feedCluster);
    }

    protected static List<Location> getLocations(Cluster cluster, Feed feed) {
        // check if locations are overridden in cluster
        final Locations clusterLocations = cluster.getLocations();
        if (clusterLocations != null
                && clusterLocations.getLocations().size() != 0) {
            return clusterLocations.getLocations();
        }

        final Locations feedLocations = feed.getLocations();
        return feedLocations == null ? null : feedLocations.getLocations();
    }

    protected static CatalogTable getTable(Cluster cluster, Feed feed) {
        // check if table is overridden in cluster
        if (cluster.getTable() != null) {
            return cluster.getTable();
        }

        return feed.getTable();
    }

    public static String normalizePartitionExpression(String part1, String part2) {
        String partExp = StringUtils.stripToEmpty(part1) + "/" + StringUtils.stripToEmpty(part2);
        partExp = partExp.replaceAll("//+", "/");
        partExp = StringUtils.stripStart(partExp, "/");
        partExp = StringUtils.stripEnd(partExp, "/");
        return partExp;
    }

    public static String normalizePartitionExpression(String partition) {
        return normalizePartitionExpression(partition, null);
    }

    private static Properties loadClusterProperties(org.apache.falcon.entity.v0.cluster.Cluster cluster) {
        Properties properties = new Properties();
        Map<String, String> clusterVars = new HashMap<String, String>();
        clusterVars.put("colo", cluster.getColo());
        clusterVars.put("name", cluster.getName());
        if (cluster.getProperties() != null) {
            for (Property property : cluster.getProperties().getProperties()) {
                clusterVars.put(property.getName(), property.getValue());
            }
        }
        properties.put("cluster", clusterVars);
        return properties;
    }

    public static String evaluateClusterExp(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity, String exp)
        throws FalconException {

        Properties properties = loadClusterProperties(clusterEntity);
        ExpressionHelper expHelp = ExpressionHelper.get();
        expHelp.setPropertiesForVariable(properties);
        return expHelp.evaluateFullExpression(exp, String.class);
    }

    public static String getStagingDir(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                       Feed feed, CatalogStorage storage, Tag tag) {
        String workflowName = EntityUtil.getWorkflowName(
                tag, Arrays.asList(clusterEntity.getName()), feed).toString();
        return ClusterHelper.getCompleteLocation(clusterEntity, "staging") + "/"
                + workflowName  + "/"
                + storage.getDatabase() + "/"
                + storage.getTable();
    }
}

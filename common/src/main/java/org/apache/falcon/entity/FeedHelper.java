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
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.util.BuildProperties;
import org.apache.hadoop.fs.Path;

import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;

/**
 * Feed entity helper methods.
 */
public final class FeedHelper {

    private static final String FORMAT = "yyyyMMddHHmm";

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

    public static List<Location> getLocations(Cluster cluster, Feed feed) {
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

    public static Properties getClusterProperties(org.apache.falcon.entity.v0.cluster.Cluster cluster) {
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

        Properties properties = getClusterProperties(clusterEntity);
        ExpressionHelper expHelp = ExpressionHelper.get();
        expHelp.setPropertiesForVariable(properties);
        return expHelp.evaluateFullExpression(exp, String.class);
    }

    public static String getStagingPath(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                       Feed feed, CatalogStorage storage, Tag tag, String suffix) {
        String stagingDirPath = getStagingDir(clusterEntity, feed, storage, tag);

        String datedPartitionKey = storage.getDatedPartitionKeys().get(0);
        String datedPartitionKeySuffix = datedPartitionKey + "=${coord:dataOutPartitionValue('output',"
                + "'" + datedPartitionKey + "')}";
        return stagingDirPath + "/"
                + datedPartitionKeySuffix + "/"
                + suffix + "/"
                + "data";
    }

    public static String getStagingDir(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                       Feed feed, CatalogStorage storage, Tag tag) {
        String workflowName = EntityUtil.getWorkflowName(
                tag, Arrays.asList(clusterEntity.getName()), feed).toString();

        // log path is created at scheduling wf and has 777 perms
        return ClusterHelper.getStorageUrl(clusterEntity)
                + EntityUtil.getLogPath(clusterEntity, feed) + "/"
                + workflowName + "/"
                + storage.getDatabase() + "/"
                + storage.getTable();
    }

    public static Properties getUserWorkflowProperties(LifeCycle lifeCycle) {
        Properties props = new Properties();
        props.put("userWorkflowName", lifeCycle.name().toLowerCase() + "-policy");
        props.put("userWorkflowEngine", "falcon");

        String version;
        try {
            version = BuildProperties.get().getProperty("build.version");
        } catch (Exception e) {  // unfortunate that this is only available in prism/webapp
            version = "0.6";
        }
        props.put("userWorkflowVersion", version);
        return props;
    }

    public static Properties getFeedProperties(Feed feed) {
        Properties feedProperties = new Properties();
        if (feed.getProperties() != null) {
            for (org.apache.falcon.entity.v0.feed.Property property : feed.getProperties().getProperties()) {
                feedProperties.put(property.getName(), property.getValue());
            }
        }
        return feedProperties;
    }

    /**
     * Replaces timed variables with corresponding time notations e.g., ${YEAR} with yyyy and so on.
     * @param templatePath - template feed path
     * @return time notations
     */
    public static String getDateFormatInPath(String templatePath) {
        String mask = extractDatePartFromPathMask(templatePath, templatePath);
        //yyyyMMddHHmm
        return mask.replaceAll(FeedDataPath.VARS.YEAR.regex(), "yyyy")
            .replaceAll(FeedDataPath.VARS.MONTH.regex(), "MM")
            .replaceAll(FeedDataPath.VARS.DAY.regex(), "dd")
            .replaceAll(FeedDataPath.VARS.HOUR.regex(), "HH")
            .replaceAll(FeedDataPath.VARS.MINUTE.regex(), "mm");
    }

    /**
     * Extracts the date part of the path and builds a date format mask.
     * @param mask - Path pattern containing ${YEAR}, ${MONTH}...
     * @param inPath - Path from which date part need to be extracted
     * @return - Parts of inPath with non-date-part stripped out.
     *
     * Example: extractDatePartFromPathMask("/data/foo/${YEAR}/${MONTH}", "/data/foo/2012/${MONTH}");
     *     Returns: 2012${MONTH}.
     */
    private static String extractDatePartFromPathMask(String mask, String inPath) {
        String[] elements = FeedDataPath.PATTERN.split(mask);

        String out = inPath;
        for (String element : elements) {
            out = out.replaceFirst(element, "");
        }
        return out;
    }

    private static Map<FeedDataPath.VARS, String> getDatePartMap(String path, String mask) {
        Map<FeedDataPath.VARS, String> map = new TreeMap<FeedDataPath.VARS, String>();
        Matcher matcher = FeedDataPath.DATE_FIELD_PATTERN.matcher(mask);
        int start = 0;
        while (matcher.find(start)) {
            String subMask = mask.substring(matcher.start(), matcher.end());
            String subPath = path.substring(matcher.start(), matcher.end());
            FeedDataPath.VARS var = FeedDataPath.VARS.from(subMask);
            if (!map.containsKey(var)) {
                map.put(var, subPath);
            }
            start = matcher.start() + 1;
        }
        return map;
    }

    /**
     *  Extracts date from the actual data path e.g., /path/2014/05/06 maps to 2014-05-06T00:00Z.
     * @param file - actual data path
     * @param templatePath - template path from feed definition
     * @param dateMask - path mask from getDateFormatInPath()
     * @param timeZone
     * @return date corresponding to the path
     */
    //consider just the first occurrence of the pattern
    public static Date getDate(Path file, String templatePath, String dateMask, String timeZone) {
        String path = extractDatePartFromPathMask(templatePath, file.toString());
        Map<FeedDataPath.VARS, String> map = getDatePartMap(path, dateMask);

        if (map.isEmpty()) {
            return null;
        }

        StringBuilder date = new StringBuilder();
        int ordinal = 0;
        for (Map.Entry<FeedDataPath.VARS, String> entry : map.entrySet()) {
            if (ordinal++ == entry.getKey().ordinal()) {
                date.append(entry.getValue());
            } else {
                return null;
            }
        }

        try {
            DateFormat dateFormat = new SimpleDateFormat(FORMAT.substring(0, date.length()));
            dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
            return dateFormat.parse(date.toString());
        } catch (ParseException e) {
            return null;
        }
    }

    public static FeedInstanceResult getFeedInstanceListing(Entity entityObject,
                                                            Date start, Date end) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entityObject);
        FeedInstanceResult result = new FeedInstanceResult(APIResult.Status.SUCCEEDED, "Success");
        for (String cluster : clusters) {
            Feed feed = (Feed) entityObject;
            Storage storage = createStorage(cluster, feed);
            List<FeedInstanceStatus> feedListing = storage.getListing(feed, cluster, LocationType.DATA, start, end);
            FeedInstanceResult.Instance[] instances = new FeedInstanceResult.Instance[feedListing.size()];
            int index = 0;
            for (FeedInstanceStatus feedStatus : feedListing) {
                FeedInstanceResult.Instance instance = new
                        FeedInstanceResult.Instance(cluster, feedStatus.getInstance(),
                        feedStatus.getStatus().name());
                instance.creationTime = feedStatus.getCreationTime();
                instance.uri = feedStatus.getUri();
                instance.size = feedStatus.getSize();
                instances[index++] = instance;
            }
            result.setInstances(instances);
        }
        return result;
    }
}

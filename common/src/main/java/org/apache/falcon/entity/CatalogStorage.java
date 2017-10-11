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

import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.catalog.AbstractCatalogService;
import org.apache.falcon.catalog.CatalogPartition;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.catalog.HiveCatalogService;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.retention.EvictedInstanceSerDe;
import org.apache.falcon.retention.EvictionHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * A catalog registry implementation of a feed storage.
 */
public class CatalogStorage extends Configured implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogStorage.class);

    // constants to be used while preparing HCatalog partition filter query
    private static final String FILTER_ST_BRACKET = "(";
    private static final String FILTER_END_BRACKET = ")";
    private static final String FILTER_QUOTE = "'";
    private static final String FILTER_AND = " and ";
    private static final String FILTER_OR = " or ";
    private static final String FILTER_LESS_THAN = " < ";
    private static final String FILTER_EQUALS = " = ";

    private final StringBuffer instancePaths = new StringBuffer();
    private final StringBuilder instanceDates = new StringBuilder();

    public static final String PARTITION_SEPARATOR = ";";
    public static final String PARTITION_KEYVAL_SEPARATOR = "=";
    public static final String INPUT_PATH_SEPARATOR = ":";
    public static final String OUTPUT_PATH_SEPARATOR = "/";
    public static final String PARTITION_VALUE_QUOTE = "'";

    public static final String CATALOG_URL = "${hcatNode}";

    private final String catalogUrl;
    private String database;
    private String table;
    private Map<String, String> partitions;

    protected CatalogStorage(Feed feed) throws URISyntaxException {
        this(CATALOG_URL, feed.getTable());
    }

    public CatalogStorage(Cluster cluster, CatalogTable table) throws URISyntaxException {
        this(ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY).getEndpoint(), table);
    }

    protected CatalogStorage(String catalogUrl, CatalogTable table) throws URISyntaxException {
        this(catalogUrl, table.getUri());
    }

    protected CatalogStorage(String catalogUrl, String tableUri) throws URISyntaxException {
        if (catalogUrl == null || catalogUrl.length() == 0) {
            throw new IllegalArgumentException("Catalog Registry URL cannot be null or empty");
        }
        verifyAndUpdateConfiguration(getConf());
        this.catalogUrl = catalogUrl;

        parseFeedUri(tableUri);
    }

    /**
     * Validate URI to conform to catalog:$database:$table#$partitions.
     * scheme=catalog:database=$database:table=$table#$partitions
     * partitions=key=value;key=value
     *
     * @param catalogTableUri table URI to parse and validate
     * @throws URISyntaxException
     */
    private void parseFeedUri(String catalogTableUri) throws URISyntaxException {

        final String processed = catalogTableUri.replaceAll(DOLLAR_EXPR_START_REGEX, DOLLAR_EXPR_START_NORMALIZED)
                                                .replaceAll("}", EXPR_CLOSE_NORMALIZED);
        URI tableUri = new URI(processed);

        if (!"catalog".equals(tableUri.getScheme())) {
            throw new URISyntaxException(tableUri.toString(), "catalog scheme is missing");
        }

        final String schemeSpecificPart = tableUri.getSchemeSpecificPart();
        if (schemeSpecificPart == null) {
            throw new URISyntaxException(tableUri.toString(), "Database and Table are missing");
        }

        String[] paths = schemeSpecificPart.split(INPUT_PATH_SEPARATOR);

        if (paths.length != 2) {
            throw new URISyntaxException(tableUri.toString(), "URI path is not in expected format: database:table");
        }

        database = paths[0];
        table = paths[1];

        if (database == null || database.length() == 0) {
            throw new URISyntaxException(tableUri.toString(), "DB name is missing");
        }
        if (table == null || table.length() == 0) {
            throw new URISyntaxException(tableUri.toString(), "Table name is missing");
        }

        String partRaw = tableUri.getFragment();
        if (partRaw == null || partRaw.length() == 0) {
            throw new URISyntaxException(tableUri.toString(), "Partition details are missing");
        }

        final String rawPartition = partRaw.replaceAll(DOLLAR_EXPR_START_NORMALIZED, DOLLAR_EXPR_START_REGEX)
                                           .replaceAll(EXPR_CLOSE_NORMALIZED, EXPR_CLOSE_REGEX);
        partitions = new LinkedHashMap<String, String>(); // preserve insertion order
        String[] parts = rawPartition.split(PARTITION_SEPARATOR);
        for (String part : parts) {
            if (part == null || part.length() == 0) {
                continue;
            }

            String[] keyVal = part.split(PARTITION_KEYVAL_SEPARATOR);
            if (keyVal.length != 2) {
                throw new URISyntaxException(tableUri.toString(),
                        "Partition key value pair is not specified properly in (" + part + ")");
            }

            partitions.put(keyVal[0], keyVal[1]);
        }
    }

    /**
     * Create an instance from the URI Template that was generated using
     * the getUriTemplate() method.
     *
     * @param uriTemplate the uri template from org.apache.falcon.entity.CatalogStorage#getUriTemplate
     * @throws URISyntaxException
     */
    protected CatalogStorage(String uriTemplate) throws URISyntaxException {
        this(uriTemplate, new Configuration());
    }

    protected CatalogStorage(String uriTemplate, Configuration conf) throws URISyntaxException {
        if (uriTemplate == null || uriTemplate.length() == 0) {
            throw new IllegalArgumentException("URI template cannot be null or empty");
        }

        final String processed = uriTemplate.replaceAll(DOLLAR_EXPR_START_REGEX, DOLLAR_EXPR_START_NORMALIZED)
                                            .replaceAll("}", EXPR_CLOSE_NORMALIZED);
        URI uri = new URI(processed);

        this.catalogUrl = uri.getScheme() + "://" + uri.getAuthority();
        verifyAndUpdateConfiguration(conf);
        parseUriTemplate(uri);
    }

    private void parseUriTemplate(URI uriTemplate) throws URISyntaxException {
        String path = uriTemplate.getPath();
        String[] paths = path.split(OUTPUT_PATH_SEPARATOR);
        if (paths.length != 4) {
            throw new URISyntaxException(uriTemplate.toString(),
                    "URI path is not in expected format: database:table");
        }

        database = paths[1];
        table = paths[2];
        String partRaw = paths[3];

        if (database == null || database.length() == 0) {
            throw new URISyntaxException(uriTemplate.toString(), "DB name is missing");
        }
        if (table == null || table.length() == 0) {
            throw new URISyntaxException(uriTemplate.toString(), "Table name is missing");
        }
        if (partRaw == null || partRaw.length() == 0) {
            throw new URISyntaxException(uriTemplate.toString(), "Partition details are missing");
        }

        String rawPartition = partRaw.replaceAll(DOLLAR_EXPR_START_NORMALIZED, DOLLAR_EXPR_START_REGEX)
                .replaceAll(EXPR_CLOSE_NORMALIZED, EXPR_CLOSE_REGEX);
        partitions = new LinkedHashMap<String, String>();
        String[] parts = rawPartition.split(PARTITION_SEPARATOR);
        for (String part : parts) {
            if (part == null || part.length() == 0) {
                continue;
            }

            String[] keyVal = part.split(PARTITION_KEYVAL_SEPARATOR);
            if (keyVal.length != 2) {
                throw new URISyntaxException(uriTemplate.toString(),
                        "Partition key value pair is not specified properly in (" + part + ")");
            }

            partitions.put(keyVal[0], keyVal[1]);
        }
    }

    public String getCatalogUrl() {
        return catalogUrl;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getPartitions() {
        return partitions;
    }

    /**
     * @param key partition key
     * @return partition value
     */
    public String getPartitionValue(String key) {
        return partitions.get(key);
    }

    /**
     * @param key partition key
     * @return if partitions map includes the key or not
     */
    public boolean hasPartition(String key) {
        return partitions.containsKey(key);
    }

    public List<String> getDatedPartitionKeys() {
        List<String> keys = new ArrayList<String>();

        for (Map.Entry<String, String> entry : getPartitions().entrySet()) {

            Matcher matcher = FeedDataPath.PATTERN.matcher(entry.getValue());
            if (matcher.find()) {
                keys.add(entry.getKey());
            }
        }

        return keys;
    }

    /**
     * Convert the partition map to filter string.
     * Each key value pair is separated by ';'.
     *
     * @return filter string
     */
    public String toPartitionFilter() {
        StringBuilder filter = new StringBuilder();
        filter.append("(");
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (filter.length() > 1) {
                filter.append(PARTITION_SEPARATOR);
            }
            filter.append(entry.getKey());
            filter.append(PARTITION_KEYVAL_SEPARATOR);
            filter.append(PARTITION_VALUE_QUOTE);
            filter.append(entry.getValue());
            filter.append(PARTITION_VALUE_QUOTE);
        }
        filter.append(")");
        return filter.toString();
    }

    /**
     * Convert the partition map to path string.
     * Each key value pair is separated by '/'.
     *
     * @return path string
     */
    public String toPartitionAsPath() {
        StringBuilder partitionFilter = new StringBuilder();

        for (Map.Entry<String, String> entry : getPartitions().entrySet()) {
            partitionFilter.append(entry.getKey())
                    .append(PARTITION_KEYVAL_SEPARATOR)
                    .append(entry.getValue())
                    .append(OUTPUT_PATH_SEPARATOR);
        }

        partitionFilter.setLength(partitionFilter.length() - 1);
        return partitionFilter.toString();
    }

    @Override
    public TYPE getType() {
        return TYPE.TABLE;
    }

    /**
     * LocationType does NOT matter here.
     */
    @Override
    public String getUriTemplate() {
        return getUriTemplate(LocationType.DATA);
    }

    /**
     * LocationType does NOT matter here.
     */
    @Override
    public String getUriTemplate(LocationType locationType) {
        StringBuilder uriTemplate = new StringBuilder();
        uriTemplate.append(catalogUrl);
        uriTemplate.append(OUTPUT_PATH_SEPARATOR);
        uriTemplate.append(database);
        uriTemplate.append(OUTPUT_PATH_SEPARATOR);
        uriTemplate.append(table);
        uriTemplate.append(OUTPUT_PATH_SEPARATOR);
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            uriTemplate.append(entry.getKey());
            uriTemplate.append(PARTITION_KEYVAL_SEPARATOR);
            uriTemplate.append(entry.getValue());
            uriTemplate.append(PARTITION_SEPARATOR);
        }
        uriTemplate.setLength(uriTemplate.length() - 1);

        return uriTemplate.toString();
    }

    @Override
    public boolean isIdentical(Storage toCompareAgainst) throws FalconException {
        if (!(toCompareAgainst instanceof CatalogStorage)) {
            return false;
        }

        CatalogStorage catalogStorage = (CatalogStorage) toCompareAgainst;

        return !(getCatalogUrl() != null && !getCatalogUrl().equals(catalogStorage.getCatalogUrl()))
                && getDatabase().equals(catalogStorage.getDatabase())
                && getTable().equals(catalogStorage.getTable())
                && getPartitions().equals(catalogStorage.getPartitions());
    }

    @Override
    public void validateACL(AccessControlList acl) throws FalconException {
        // This is not supported in Hive today as authorization is not enforced on table and
        // partition listing
    }

    @Override
    public List<FeedInstanceStatus> getListing(Feed feed, String clusterName, LocationType locationType,
                                               Date start, Date end) throws FalconException {
        try {
            List<FeedInstanceStatus> instances = new ArrayList<FeedInstanceStatus>();
            Date feedStart = FeedHelper.getFeedValidityStart(feed, clusterName);
            Date alignedDate = EntityUtil.getNextStartTime(feedStart, feed.getFrequency(),
                    feed.getTimezone(), start);

            while (!end.before(alignedDate)) {
                List<String> partitionValues = getCatalogPartitionValues(alignedDate);
                try {
                    CatalogPartition partition = CatalogServiceFactory.getCatalogService().getPartition(
                            getConf(), getCatalogUrl(), getDatabase(), getTable(), partitionValues);
                    instances.add(getFeedInstanceFromCatalogPartition(partition));
                } catch (FalconException e) {
                    if (e.getMessage().startsWith(HiveCatalogService.PARTITION_DOES_NOT_EXIST)) {
                        // Partition missing
                        FeedInstanceStatus instanceStatus = new FeedInstanceStatus(null);
                        instanceStatus.setInstance(partitionValues.toString());
                        instances.add(instanceStatus);
                    } else {
                        throw e;
                    }
                }
                alignedDate = FeedHelper.getNextFeedInstanceDate(alignedDate, feed);
            }
            return instances;
        } catch (Exception e) {
            LOG.error("Unable to retrieve listing for {}:{} -- {}", locationType, catalogUrl, e.getMessage());
            throw new FalconException("Unable to retrieve listing for (URI " + catalogUrl + ")", e);
        }
    }

    private List<String> getCatalogPartitionValues(Date alignedDate) throws FalconException {
        List<String> partitionValues  = new ArrayList<String>();
        for (Map.Entry<String, String> entry : getPartitions().entrySet()) {
            if (FeedDataPath.PATTERN.matcher(entry.getValue()).find()) {
                ExpressionHelper.setReferenceDate(alignedDate);
                ExpressionHelper expressionHelper = ExpressionHelper.get();
                String instanceValue = expressionHelper.evaluateFullExpression(entry.getValue(), String.class);
                partitionValues.add(instanceValue);
            } else {
                partitionValues.add(entry.getValue());
            }
        }
        return partitionValues;
    }

    private FeedInstanceStatus getFeedInstanceFromCatalogPartition(CatalogPartition partition) {
        FeedInstanceStatus feedInstanceStatus = new FeedInstanceStatus(partition.getLocation());
        feedInstanceStatus.setCreationTime(partition.getCreateTime());
        feedInstanceStatus.setInstance(partition.getValues().toString());
        FeedInstanceStatus.AvailabilityStatus availabilityStatus = FeedInstanceStatus.AvailabilityStatus.MISSING;
        long size = partition.getSize();
        if (size == 0) {
            availabilityStatus = FeedInstanceStatus.AvailabilityStatus.EMPTY;
        } else if (size > 0) {
            availabilityStatus = FeedInstanceStatus.AvailabilityStatus.AVAILABLE;
        }
        feedInstanceStatus.setSize(size);
        feedInstanceStatus.setStatus(availabilityStatus);
        return feedInstanceStatus;
    }

    @Override
    public FeedInstanceStatus.AvailabilityStatus getInstanceAvailabilityStatus(Feed feed, String clusterName,
                                         LocationType locationType, Date instanceTime) throws FalconException {
        List<FeedInstanceStatus> result = getListing(feed, clusterName, locationType, instanceTime, instanceTime);
        if (result.isEmpty()) {
            return FeedInstanceStatus.AvailabilityStatus.MISSING;
        } else {
            return result.get(0).getStatus();
        }
    }

    @Override
    public StringBuilder evict(String retentionLimit, String timeZone, Path logFilePath) throws FalconException {
        LOG.info("Applying retention on {}, Limit: {}, timezone: {}",
                getTable(), retentionLimit, timeZone);

        List<CatalogPartition> toBeDeleted;
        try {
            // get sorted date partition keys and values
            toBeDeleted = discoverPartitionsToDelete(retentionLimit, timeZone);
        } catch (ELException e) {
            throw new FalconException("Couldn't find partitions to be deleted", e);

        }

        if (toBeDeleted.isEmpty()) {
            LOG.info("No partitions to delete.");
        } else {
            final boolean isTableExternal = CatalogServiceFactory.getCatalogService().isTableExternal(
                getConf(), getCatalogUrl(), getDatabase(), getTable());
            try {
                dropPartitions(toBeDeleted, isTableExternal);
            } catch (IOException e) {
                throw new FalconException("Couldn't drop partitions", e);
            }
        }

        try {
            EvictedInstanceSerDe.serializeEvictedInstancePaths(
                    HadoopClientFactory.get().createProxiedFileSystem(logFilePath.toUri(), new Configuration()),
                    logFilePath, instancePaths);
        } catch (IOException e) {
            throw new FalconException("Couldn't record dropped partitions", e);
        }
        return instanceDates;
    }

    private List<CatalogPartition> discoverPartitionsToDelete(String retentionLimit, String timezone)
        throws FalconException, ELException {
        Pair<Date, Date> range = EvictionHelper.getDateRange(retentionLimit);
        ExpressionHelper.setReferenceDate(range.first);
        Map<String, String> partitionsToDelete = new LinkedHashMap<String, String>();
        ExpressionHelper expressionHelper = ExpressionHelper.get();
        for (Map.Entry<String, String> entry : getPartitions().entrySet()) {
            if (FeedDataPath.PATTERN.matcher(entry.getValue()).find()) {
                partitionsToDelete.put(entry.getKey(),
                        expressionHelper.evaluateFullExpression(entry.getValue(), String.class));
            }
        }
        final String filter = createFilter(partitionsToDelete);
        return CatalogServiceFactory.getCatalogService().listPartitionsByFilter(
            getConf(), getCatalogUrl(), getDatabase(), getTable(), filter);
    }

    /**
     * Creates hive partition filter from inputs partition map.
     * @param partitionsMap - ordered map of partition keys and values
     * @return partition filter
     * @throws ELException
     */
    private String createFilter(Map<String, String> partitionsMap) throws ELException {

        /* Construct filter query string. As an example, suppose the dated partition keys
         * are: [year, month, day, hour] and dated partition values are [2014, 02, 24, 10].
         * Then the filter query generated is of the format:
         * "(year < '2014') or (year = '2014' and month < '02') or
         * (year = '2014' and month = '02' and day < '24') or
         * or (year = '2014' and month = '02' and day = '24' and hour < '10')"
         */
        StringBuilder filterBuffer = new StringBuilder();
        List<String> keys = new ArrayList<String>(partitionsMap.keySet());
        for (int curr = 0; curr < partitionsMap.size(); curr++) {
            if (curr > 0) {
                filterBuffer.append(FILTER_OR);
            }
            filterBuffer.append(FILTER_ST_BRACKET);
            for (int prev = 0; prev < curr; prev++) {
                String key = keys.get(prev);
                filterBuffer.append(key)
                        .append(FILTER_EQUALS)
                        .append(FILTER_QUOTE)
                        .append(partitionsMap.get(key))
                        .append(FILTER_QUOTE)
                        .append(FILTER_AND);
            }
            String key = keys.get(curr);
            filterBuffer.append(key)
                    .append(FILTER_LESS_THAN)
                    .append(FILTER_QUOTE)
                    .append(partitionsMap.get(key))
                    .append(FILTER_QUOTE)
                    .append(FILTER_END_BRACKET);
        }

        return filterBuffer.toString();
    }

    private void dropPartitions(List<CatalogPartition> partitionsToDelete, boolean isTableExternal)
        throws FalconException, IOException {
        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        for (CatalogPartition partition : partitionsToDelete) {
            boolean deleted = catalogService.dropPartition(getConf(), getCatalogUrl(), getDatabase(), getTable(),
                    partition.getValues(), true);

            if (!deleted) {
                return;
            }

            if (isTableExternal) { // nuke the dirs if an external table
                final Path path = new Path(partition.getLocation());
                if (!HadoopClientFactory.get().createProxiedFileSystem(path.toUri()).delete(path, true)) {
                    throw new FalconException("Failed to delete location " + path + " for partition "
                            + partition.getValues());
                }
            }

            // replace ',' with ';' since message producer splits instancePaths string by ','
            String partitionInfo = partition.getValues().toString().replace(",", ";");
            LOG.info("Deleted partition: " + partitionInfo);
            instanceDates.append(partitionInfo).append(',');
            instancePaths.append(partition.getLocation()).append(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR);
        }
    }

    @Override
    public String toString() {
        return "CatalogStorage{"
                + "catalogUrl='" + catalogUrl + '\''
                + ", database='" + database + '\''
                + ", table='" + table + '\''
                + ", partitions=" + partitions
                + '}';
    }

    private void verifyAndUpdateConfiguration(Configuration conf) {
        if (conf == null) {
            setConf(new Configuration());
        } else {
            setConf(conf);
        }
    }
}

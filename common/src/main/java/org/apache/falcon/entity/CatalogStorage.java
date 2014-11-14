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
import org.apache.falcon.catalog.CatalogPartition;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Matcher;

/**
 * A catalog registry implementation of a feed storage.
 */
public class CatalogStorage extends Configured implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(EvictionHelper.class);

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

    protected CatalogStorage(Cluster cluster, CatalogTable table) throws URISyntaxException {
        this(ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY).getEndpoint(), table);
    }

    protected CatalogStorage(String catalogUrl, CatalogTable table) throws URISyntaxException {
        this(catalogUrl, table.getUri());
    }

    protected CatalogStorage(String catalogUrl, String tableUri) throws URISyntaxException {
        if (catalogUrl == null || catalogUrl.length() == 0) {
            throw new IllegalArgumentException("Catalog Registry URL cannot be null or empty");
        }

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
        if (uriTemplate == null || uriTemplate.length() == 0) {
            throw new IllegalArgumentException("URI template cannot be null or empty");
        }

        final String processed = uriTemplate.replaceAll(DOLLAR_EXPR_START_REGEX, DOLLAR_EXPR_START_NORMALIZED)
                                            .replaceAll("}", EXPR_CLOSE_NORMALIZED);
        URI uri = new URI(processed);

        this.catalogUrl = uri.getScheme() + "://" + uri.getAuthority();

        parseUriTemplate(uri);
    }

    protected CatalogStorage(String uriTemplate, Configuration conf) throws URISyntaxException {
        this(uriTemplate);
        setConf(conf);
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
    public List<FeedInstanceStatus> getListing(Feed feed, String cluster, LocationType locationType,
                                               Date start, Date end) throws FalconException {
        throw new UnsupportedOperationException("getListing");
    }

    @Override
    public StringBuilder evict(String retentionLimit, String timeZone, Path logFilePath) throws FalconException {
        LOG.info("Applying retention on {}, Limit: {}, timezone: {}",
                getTable(), retentionLimit, timeZone);

        // get sorted date partition keys and values
        List<String> datedPartKeys = new ArrayList<String>();
        List<String> datedPartValues = new ArrayList<String>();
        List<CatalogPartition> toBeDeleted;
        try {
            fillSortedDatedPartitionKVs(datedPartKeys, datedPartValues, retentionLimit, timeZone);
            toBeDeleted = discoverPartitionsToDelete(datedPartKeys, datedPartValues);

        } catch (ELException e) {
            throw new FalconException("Couldn't find partitions to be deleted", e);

        }
        if (toBeDeleted.isEmpty()) {
            LOG.info("No partitions to delete.");
        } else {
            final boolean isTableExternal = CatalogServiceFactory.getCatalogService().isTableExternal(
                getConf(), getCatalogUrl(), getDatabase(), getTable());
            try {
                dropPartitions(toBeDeleted, datedPartKeys, isTableExternal);
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

    private List<CatalogPartition> discoverPartitionsToDelete(List<String> datedPartKeys, List<String> datedPartValues)
        throws FalconException, ELException {

        final String filter = createFilter(datedPartKeys, datedPartValues);
        return CatalogServiceFactory.getCatalogService().listPartitionsByFilter(
            getConf(), getCatalogUrl(), getDatabase(), getTable(), filter);
    }

    private void fillSortedDatedPartitionKVs(List<String> sortedPartKeys, List<String> sortedPartValues,
                                             String retentionLimit, String timeZone) throws ELException {
        Pair<Date, Date> range = EvictionHelper.getDateRange(retentionLimit);

        // sort partition keys and values by the date pattern present in value
        Map<FeedDataPath.VARS, String> sortedPartKeyMap = new TreeMap<FeedDataPath.VARS, String>();
        Map<FeedDataPath.VARS, String> sortedPartValueMap = new TreeMap<FeedDataPath.VARS, String>();
        for (Map.Entry<String, String> entry : getPartitions().entrySet()) {
            String datePattern = entry.getValue();
            String mask = datePattern.replaceAll(FeedDataPath.VARS.YEAR.regex(), "yyyy")
                    .replaceAll(FeedDataPath.VARS.MONTH.regex(), "MM")
                    .replaceAll(FeedDataPath.VARS.DAY.regex(), "dd")
                    .replaceAll(FeedDataPath.VARS.HOUR.regex(), "HH")
                    .replaceAll(FeedDataPath.VARS.MINUTE.regex(), "mm");

            // find the first date pattern present in date mask
            FeedDataPath.VARS vars = FeedDataPath.VARS.presentIn(mask);
            // skip this partition if date mask doesn't contain any date format
            if (vars == null) {
                continue;
            }

            // construct dated partition value as per format
            DateFormat dateFormat = new SimpleDateFormat(mask);
            dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
            String partitionValue = dateFormat.format(range.first);

            // add partition key and value in their sorted maps
            if (!sortedPartKeyMap.containsKey(vars)) {
                sortedPartKeyMap.put(vars, entry.getKey());
            }

            if (!sortedPartValueMap.containsKey(vars)) {
                sortedPartValueMap.put(vars, partitionValue);
            }
        }

        // add map entries to lists of partition keys and values
        sortedPartKeys.addAll(sortedPartKeyMap.values());
        sortedPartValues.addAll(sortedPartValueMap.values());
    }

    private String createFilter(List<String> datedPartKeys, List<String> datedPartValues) throws ELException {
        int numPartitions = datedPartKeys.size();

        /* Construct filter query string. As an example, suppose the dated partition keys
         * are: [year, month, day, hour] and dated partition values are [2014, 02, 24, 10].
         * Then the filter query generated is of the format:
         * "(year < '2014') or (year = '2014' and month < '02') or
         * (year = '2014' and month = '02' and day < '24') or
         * or (year = '2014' and month = '02' and day = '24' and hour < '10')"
         */
        StringBuilder filterBuffer = new StringBuilder();
        for (int curr = 0; curr < numPartitions; curr++) {
            if (curr > 0) {
                filterBuffer.append(FILTER_OR);
            }
            filterBuffer.append(FILTER_ST_BRACKET);
            for (int prev = 0; prev < curr; prev++) {
                filterBuffer.append(datedPartKeys.get(prev))
                        .append(FILTER_EQUALS)
                        .append(FILTER_QUOTE)
                        .append(datedPartValues.get(prev))
                        .append(FILTER_QUOTE)
                        .append(FILTER_AND);
            }
            filterBuffer.append(datedPartKeys.get(curr))
                    .append(FILTER_LESS_THAN)
                    .append(FILTER_QUOTE)
                    .append(datedPartValues.get(curr))
                    .append(FILTER_QUOTE)
                    .append(FILTER_END_BRACKET);
        }

        return filterBuffer.toString();
    }

    private void dropPartitions(List<CatalogPartition> partitionsToDelete, List<String> datedPartKeys,
                                boolean isTableExternal) throws FalconException, IOException {

        // get table partition columns
        List<String> partColumns = CatalogServiceFactory.getCatalogService().getTablePartitionCols(
            getConf(), getCatalogUrl(), getDatabase(), getTable());

        /* In case partition columns are a super-set of dated partitions, there can be multiple
         * partitions that share the same set of date-partition values. All such partitions can
         * be deleted by issuing a single HCatalog dropPartition call per date-partition values.
         * Arrange the partitions grouped by each set of date-partition values.
         */
        Map<Map<String, String>, List<CatalogPartition>> dateToPartitionsMap = new HashMap<
                Map<String, String>, List<CatalogPartition>>();
        for (CatalogPartition partitionToDrop : partitionsToDelete) {
            // create a map of name-values of all columns of this partition
            Map<String, String> partitionsMap = new HashMap<String, String>();
            for (int i = 0; i < partColumns.size(); i++) {
                partitionsMap.put(partColumns.get(i), partitionToDrop.getValues().get(i));
            }

            // create a map of name-values of dated sub-set of this partition
            Map<String, String> datedPartitions = new HashMap<String, String>();
            for (String datedPart : datedPartKeys) {
                datedPartitions.put(datedPart, partitionsMap.get(datedPart));
            }

            // add a map entry of this catalog partition corresponding to its date-partition values
            List<CatalogPartition> catalogPartitions;
            if (dateToPartitionsMap.containsKey(datedPartitions)) {
                catalogPartitions = dateToPartitionsMap.get(datedPartitions);
            } else {
                catalogPartitions = new ArrayList<CatalogPartition>();
            }
            catalogPartitions.add(partitionToDrop);
            dateToPartitionsMap.put(datedPartitions, catalogPartitions);
        }

        // delete each entry within dateToPartitions Map
        for (Map.Entry<Map<String, String>, List<CatalogPartition>> entry : dateToPartitionsMap.entrySet()) {
            dropPartitionInstances(entry.getValue(), entry.getKey(), isTableExternal);
        }
    }

    private void dropPartitionInstances(List<CatalogPartition> partitionsToDrop, Map<String, String> partSpec,
                                        boolean isTableExternal) throws FalconException, IOException {

        boolean deleted = CatalogServiceFactory.getCatalogService().dropPartitions(
            getConf(), getCatalogUrl(), getDatabase(), getTable(), partSpec);

        if (!deleted) {
            return;
        }

        for (CatalogPartition partitionToDrop : partitionsToDrop) {
            if (isTableExternal) { // nuke the dirs if an external table
                final String location = partitionToDrop.getLocation();
                final Path path = new Path(location);
                deleted = HadoopClientFactory.get()
                        .createProxiedFileSystem(path.toUri()) .delete(path, true);
            }
            if (!isTableExternal || deleted) {
                // replace ',' with ';' since message producer splits instancePaths string by ','
                String partitionInfo = partitionToDrop.getValues().toString().replace("," , ";");
                LOG.info("Deleted partition: " + partitionInfo);
                instanceDates.append(partSpec).append(',');
                instancePaths.append(getEvictedPartitionPath(partitionToDrop))
                        .append(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR);
            }
        }
    }

    private String getEvictedPartitionPath(final CatalogPartition partitionToDrop) {
        String uriTemplate = getUriTemplate(); // no need for location type for table
        List<String> values = partitionToDrop.getValues();
        StringBuilder partitionPath = new StringBuilder();
        int index = 0;
        for (String partitionKey : getDatedPartitionKeys()) {
            String dateMask = getPartitionValue(partitionKey);
            String date = values.get(index);

            partitionPath.append(uriTemplate.replace(dateMask, date));
            partitionPath.append(CatalogStorage.PARTITION_SEPARATOR);
            LOG.info("partitionPath: " + partitionPath);
        }
        partitionPath.setLength(partitionPath.length() - 1);

        LOG.info("Return partitionPath: " + partitionPath);
        return partitionPath.toString();
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
}

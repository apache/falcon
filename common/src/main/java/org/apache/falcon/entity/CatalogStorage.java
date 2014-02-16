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
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * A catalog registry implementation of a feed storage.
 */
public class CatalogStorage implements Storage {

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

    public String getDatedPartitionKey() {
        String datedPartitionKey = null;

        for (Map.Entry<String, String> entry : getPartitions().entrySet()) {

            Matcher matcher = FeedDataPath.PATTERN.matcher(entry.getValue());
            if (matcher.find()) {
                datedPartitionKey = entry.getKey();
                break;
            }
        }

        return datedPartitionKey;
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
        CatalogStorage catalogStorage = (CatalogStorage) toCompareAgainst;

        return !(getCatalogUrl() != null && !getCatalogUrl().equals(catalogStorage.getCatalogUrl()))
                && getDatabase().equals(catalogStorage.getDatabase())
                && getTable().equals(catalogStorage.getTable())
                && getPartitions().equals(catalogStorage.getPartitions());
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

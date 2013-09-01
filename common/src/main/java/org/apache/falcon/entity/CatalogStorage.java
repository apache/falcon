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
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.v0.feed.LocationType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * A catalog registry implementation of a feed storage.
 */
public class CatalogStorage implements Storage {

    public static final String PARTITION_SEPARATOR = ";";
    public static final String PARTITION_KEYVAL_SEPARATOR = "=";
    public static final String INPUT_PATH_SEPARATOR = ":";
    public static final String OUTPUT_PATH_SEPARATOR = "/";

    private final String catalogUrl;
    private String database;
    private String table;
    private Map<String, String> partitions;

    protected CatalogStorage(String catalogTable) throws URISyntaxException {
        this("${hcatNode}", catalogTable);
    }

    protected CatalogStorage(String catalogUrl, String tableUri) throws URISyntaxException {
        if (catalogUrl == null || catalogUrl.length() == 0) {
            throw new IllegalArgumentException("Catalog Registry URL cannot be null or empty");
        }

        this.catalogUrl = catalogUrl;

        parse(tableUri);
    }

    /**
     * Validate URI to conform to catalog:$database:$table#$partitions.
     * scheme=catalog:database=$database:table=$table#$partitions
     * partitions=key=value;key=value
     *
     * @param catalogTableUri table URI to parse and validate
     * @throws URISyntaxException
     */
    private void parse(String catalogTableUri) throws URISyntaxException {

        URI tableUri = new URI(catalogTableUri);

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

        partitions = new HashMap<String, String>();
        String[] parts = partRaw.split(PARTITION_SEPARATOR);
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

    @Override
    public TYPE getType() {
        return TYPE.TABLE;
    }

    @Override
    public String getUriTemplate() {
        return getUriTemplate(LocationType.DATA);
    }

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
    public boolean exists() throws FalconException {
        return CatalogServiceFactory.getCatalogService().tableExists(catalogUrl, database, table);
    }

    @Override
    public boolean isIdentical(Storage toCompareAgainst) throws FalconException {
        CatalogStorage catalogStorage = (CatalogStorage) toCompareAgainst;

        return !(getCatalogUrl() != null && !getCatalogUrl().equals(catalogStorage.getCatalogUrl()))
                && getDatabase().equals(catalogStorage.getDatabase())
                && getTable().equals(catalogStorage.getTable())
                && getPartitions().equals(catalogStorage.getPartitions());
    }
}

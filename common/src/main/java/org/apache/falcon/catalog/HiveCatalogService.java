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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatDatabase;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of CatalogService that uses Hive Meta Store (HCatalog)
 * as the backing Catalog registry.
 */
public class HiveCatalogService extends AbstractCatalogService {

    private static final Logger LOG = Logger.getLogger(HiveCatalogService.class);

    private static final ConcurrentHashMap<String, HCatClient> CACHE = new ConcurrentHashMap<String, HCatClient>();

    public static HCatClient get(Cluster cluster) throws FalconException {
        assert cluster != null : "Cluster cant be null";

        String metastoreUrl = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY).getEndpoint();
        return get(metastoreUrl);
    }

    public static synchronized HCatClient get(String metastoreUrl) throws FalconException {

        if (!CACHE.containsKey(metastoreUrl)) {
            HCatClient hCatClient = getHCatClient(metastoreUrl);
            LOG.info("Caching HCatalog client object for " + metastoreUrl);
            CACHE.putIfAbsent(metastoreUrl, hCatClient);
        }

        return CACHE.get(metastoreUrl);
    }

    private static HCatClient getHCatClient(String metastoreUrl) throws FalconException {
        try {
            HiveConf hcatConf = createHiveConf(metastoreUrl);
            return HCatClient.create(hcatConf);
        } catch (HCatException e) {
            throw new FalconException("Exception creating HCatClient: " + e.getMessage(), e);
        }
    }

    private static HiveConf createHiveConf(String metastoreUrl) {
        HiveConf hcatConf = new HiveConf();
        hcatConf.set("hive.metastore.local", "false");
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        return hcatConf;
    }

    public static synchronized HCatClient getProxiedClient(String catalogUrl,
                                                           String metaStorePrincipal) throws FalconException {
        if (!CACHE.containsKey(catalogUrl)) {
            try {
                final HiveConf hcatConf = createHiveConf(catalogUrl);
                if (UserGroupInformation.isSecurityEnabled()) {
                    hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, metaStorePrincipal);
                    hcatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
                }

                LOG.info("Creating and caching HCatalog client object for " + catalogUrl);
                UserGroupInformation currentUser = UserGroupInformation.getLoginUser();
                HCatClient hcatClient = currentUser.doAs(new PrivilegedExceptionAction<HCatClient>() {
                    public HCatClient run() throws Exception {
                        return HCatClient.create(hcatConf);
                    }
                });
                CACHE.putIfAbsent(catalogUrl, hcatClient);
            } catch (IOException e) {
                throw new FalconException("Exception creating Proxied HCatClient: " + e.getMessage(), e);
            } catch (InterruptedException e) {
                throw new FalconException("Exception creating Proxied HCatClient: " + e.getMessage(), e);
            }
        }

        return CACHE.get(catalogUrl);
    }

    @Override
    public boolean isAlive(final String catalogUrl,
                           final String metaStorePrincipal) throws FalconException {
        LOG.info("Checking if the service is alive for: " + catalogUrl);

        try {
            HCatClient client = getProxiedClient(catalogUrl, metaStorePrincipal);
            HCatDatabase database = client.getDatabase("default");
            return database != null;
        } catch (HCatException e) {
            throw new FalconException("Exception checking if the service is alive:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean tableExists(final String catalogUrl, final String database, final String tableName,
                               final String metaStorePrincipal) throws FalconException {
        LOG.info("Checking if the table exists: " + tableName);

        try {
            HCatClient client = getProxiedClient(catalogUrl, metaStorePrincipal);
            HCatTable table = client.getTable(database, tableName);
            return table != null;
        } catch (HCatException e) {
            throw new FalconException("Exception checking if the table exists:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean isTableExternal(String catalogUrl, String database, String tableName)
        throws FalconException {
        LOG.info("Checking if the table is external:" + tableName);

        try {
            HCatClient client = get(catalogUrl);
            HCatTable table = client.getTable(database, tableName);
            return !table.getTabletype().equals("MANAGED_TABLE");
        } catch (HCatException e) {
            throw new FalconException("Exception checking if the table is external:" + e.getMessage(), e);
        }
    }

    @Override
    public List<CatalogPartition> listPartitionsByFilter(String catalogUrl, String database,
                                                         String tableName, String filter)
        throws FalconException {
        LOG.info("List partitions for : " + tableName + ", partition filter: " + filter);

        try {
            List<CatalogPartition> catalogPartitionList = new ArrayList<CatalogPartition>();

            HCatClient client = get(catalogUrl);
            List<HCatPartition> hCatPartitions = client.listPartitionsByFilter(database, tableName, filter);
            for (HCatPartition hCatPartition : hCatPartitions) {
                LOG.info("Partition: " + hCatPartition.getValues());
                CatalogPartition partition = createCatalogPartition(hCatPartition);
                catalogPartitionList.add(partition);
            }

            return catalogPartitionList;
        } catch (HCatException e) {
            throw new FalconException("Exception listing partitions:" + e.getMessage(), e);
        }
    }

    private CatalogPartition createCatalogPartition(HCatPartition hCatPartition) {
        final CatalogPartition catalogPartition = new CatalogPartition();
        catalogPartition.setDatabaseName(hCatPartition.getDatabaseName());
        catalogPartition.setTableName(hCatPartition.getTableName());
        catalogPartition.setValues(hCatPartition.getValues());
        catalogPartition.setInputFormat(hCatPartition.getInputFormat());
        catalogPartition.setOutputFormat(hCatPartition.getOutputFormat());
        catalogPartition.setLocation(hCatPartition.getLocation());
        catalogPartition.setSerdeInfo(hCatPartition.getSerDe());
        catalogPartition.setCreateTime(hCatPartition.getCreateTime());
        catalogPartition.setLastAccessTime(hCatPartition.getLastAccessTime());

        List<String> tableColumns = new ArrayList<String>();
        for (HCatFieldSchema hCatFieldSchema : hCatPartition.getColumns()) {
            tableColumns.add(hCatFieldSchema.getName());
        }
        catalogPartition.setTableColumns(tableColumns);

        return catalogPartition;
    }

    @Override
    public boolean dropPartitions(String catalogUrl, String database,
                                  String tableName, Map<String, String> partitions)
        throws FalconException {
        LOG.info("Dropping partitions for : " + tableName + ", partitions: " + partitions);

        try {
            HCatClient client = get(catalogUrl);
            client.dropPartitions(database, tableName, partitions, true);
        } catch (HCatException e) {
            throw new FalconException("Exception dropping partitions:" + e.getMessage(), e);
        }

        return true;
    }

    @Override
    public CatalogPartition getPartition(String catalogUrl, String database, String tableName,
                                         Map<String, String> partitionSpec) throws FalconException {
        LOG.info("Fetch partition for : " + tableName + ", partition spec: " + partitionSpec);

        try {
            HCatClient client = get(catalogUrl);
            HCatPartition hCatPartition = client.getPartition(database, tableName, partitionSpec);
            return createCatalogPartition(hCatPartition);
        } catch (HCatException e) {
            throw new FalconException("Exception fetching partition:" + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getTablePartitionCols(String catalogUrl, String database,
                                            String tableName) throws FalconException {
        LOG.info("Fetching partition columns of table: " + tableName);

        try {
            HCatClient client = get(catalogUrl);
            HCatTable table = client.getTable(database, tableName);
            List<HCatFieldSchema> partSchema = table.getPartCols();
            List<String> partCols = new ArrayList<String>();
            for (HCatFieldSchema part : partSchema) {
                partCols.add(part.getName());
            }
            return partCols;
        } catch (HCatException e) {
            throw new FalconException("Exception fetching partition columns: " + e.getMessage(), e);
        }
    }
}

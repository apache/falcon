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
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.io.Text;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatDatabase;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An implementation of CatalogService that uses Hive Meta Store (HCatalog)
 * as the backing Catalog registry.
 */
public class HiveCatalogService extends AbstractCatalogService {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogService.class);

    public static HCatClient getHCatClient(String metastoreUrl) throws FalconException {
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
                                                           String metaStoreServicePrincipal)
        throws FalconException {

        try {
            final HiveConf hcatConf = createHiveConf(catalogUrl);
            UserGroupInformation proxyUGI = CurrentUser.getProxyUGI();
            if (UserGroupInformation.isSecurityEnabled()) {
                hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                        metaStoreServicePrincipal);
                hcatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");

                Token<DelegationTokenIdentifier> delegationTokenId = getDelegationToken(
                        hcatConf, metaStoreServicePrincipal);
                proxyUGI.addToken(delegationTokenId);
            }

            LOG.info("Creating and caching HCatalog client object for {}", catalogUrl);
            return proxyUGI.doAs(new PrivilegedExceptionAction<HCatClient>() {
                public HCatClient run() throws Exception {
                    return HCatClient.create(hcatConf);
                }
            });
        } catch (IOException e) {
            throw new FalconException("Exception creating Proxied HCatClient: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new FalconException("Exception creating Proxied HCatClient: " + e.getMessage(),
                    e);
        }
    }

    private static Token<DelegationTokenIdentifier> getDelegationToken(HiveConf hcatConf,
                                                                       String metaStoreServicePrincipal)
        throws IOException {

        HCatClient hcatClient = HCatClient.create(hcatConf);
        String delegationToken = hcatClient.getDelegationToken(
                CurrentUser.getUser(), metaStoreServicePrincipal);
        hcatConf.set("hive.metastore.token.signature", "FalconService");

        Token<DelegationTokenIdentifier> delegationTokenId = new Token<DelegationTokenIdentifier>();
        delegationTokenId.decodeFromUrlString(delegationToken);
        delegationTokenId.setService(new Text("FalconService"));
        return delegationTokenId;
    }

    @Override
    public boolean isAlive(final String catalogUrl,
                           final String metaStorePrincipal) throws FalconException {
        LOG.info("Checking if the service is alive for: {}", catalogUrl);

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
        LOG.info("Checking if the table exists: {}", tableName);

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
        LOG.info("Checking if the table is external: {}", tableName);

        try {
            HCatClient client = getHCatClient(catalogUrl);
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
        LOG.info("List partitions for: {}, partition filter: {}", tableName, filter);

        try {
            List<CatalogPartition> catalogPartitionList = new ArrayList<CatalogPartition>();

            HCatClient client = getHCatClient(catalogUrl);
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
        LOG.info("Dropping partitions for: {}, partitions: {}", tableName, partitions);

        try {
            HCatClient client = getHCatClient(catalogUrl);
            client.dropPartitions(database, tableName, partitions, true);
        } catch (HCatException e) {
            throw new FalconException("Exception dropping partitions:" + e.getMessage(), e);
        }

        return true;
    }

    @Override
    public CatalogPartition getPartition(String catalogUrl, String database, String tableName,
                                         Map<String, String> partitionSpec) throws FalconException {
        LOG.info("Fetch partition for: {}, partition spec: {}", tableName, partitionSpec);

        try {
            HCatClient client = getHCatClient(catalogUrl);
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
            HCatClient client = getHCatClient(catalogUrl);
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

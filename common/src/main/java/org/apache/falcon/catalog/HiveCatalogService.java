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
import org.apache.falcon.security.SecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * An implementation of CatalogService that uses Hive Meta Store (HCatalog)
 * as the backing Catalog registry.
 */
public class HiveCatalogService extends AbstractCatalogService {

    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogService.class);
    public static final String CREATE_TIME = "falcon.create_time";
    public static final String UPDATE_TIME = "falcon.update_time";


    public static HiveConf createHiveConf(Configuration conf,
                                           String metastoreUrl) throws IOException {
        HiveConf hcatConf = new HiveConf(conf, HiveConf.class);

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

    /**
     * This is used from with in an oozie job.
     *
     * @param conf conf object
     * @param metastoreUrl metastore uri
     * @return hive metastore client handle
     * @throws FalconException
     */
    private static HiveMetaStoreClient createClient(Configuration conf,
                                                    String metastoreUrl) throws FalconException {
        try {
            LOG.info("Creating HCatalog client object for metastore {} using conf {}",
                metastoreUrl, conf.toString());
            final Credentials credentials = getCredentials(conf);
            Configuration jobConf = credentials != null ? copyCredentialsToConf(conf, credentials) : conf;
            HiveConf hcatConf = createHiveConf(jobConf, metastoreUrl);

            if (UserGroupInformation.isSecurityEnabled()) {
                hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                    conf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));
                hcatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");

                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                ugi.addCredentials(credentials); // credentials cannot be null
            }

            return new HiveMetaStoreClient(hcatConf);
        } catch (Exception e) {
            throw new FalconException("Exception creating HiveMetaStoreClient: " + e.getMessage(), e);
        }
    }

    private static JobConf copyCredentialsToConf(Configuration conf, Credentials credentials) {
        JobConf jobConf = new JobConf(conf);
        jobConf.setCredentials(credentials);
        return jobConf;
    }

    private static Credentials getCredentials(Configuration conf) throws IOException {
        final String tokenFile = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
        if (tokenFile == null) {
            return null;
        }

        try {
            LOG.info("Adding credentials/delegation tokens from token file={} to conf", tokenFile);
            Credentials credentials = Credentials.readTokenStorageFile(new File(tokenFile), conf);
            LOG.info("credentials numberOfTokens={}, numberOfSecretKeys={}",
                credentials.numberOfTokens(), credentials.numberOfSecretKeys());
            return credentials;
        } catch (IOException e) {
            LOG.warn("error while fetching credentials from {}", tokenFile);
        }

        return null;
    }

    /**
     * This is used from with in falcon namespace.
     *
     * @param conf                      conf
     * @param catalogUrl                metastore uri
     * @return hive metastore client handle
     * @throws FalconException
     */
    private static HiveMetaStoreClient createProxiedClient(Configuration conf,
                                                           String catalogUrl) throws FalconException {

        try {
            final HiveConf hcatConf = createHiveConf(conf, catalogUrl);
            UserGroupInformation proxyUGI = CurrentUser.getProxyUGI();
            addSecureCredentialsAndToken(conf, hcatConf, proxyUGI);

            LOG.info("Creating HCatalog client object for {}", catalogUrl);
            return proxyUGI.doAs(new PrivilegedExceptionAction<HiveMetaStoreClient>() {
                public HiveMetaStoreClient run() throws Exception {
                    return new HiveMetaStoreClient(hcatConf);
                }
            });
        } catch (Exception e) {
            throw new FalconException("Exception creating Proxied HiveMetaStoreClient: " + e.getMessage(), e);
        }
    }

    private static void addSecureCredentialsAndToken(Configuration conf,
                                                     HiveConf hcatConf,
                                                     UserGroupInformation proxyUGI) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            String metaStoreServicePrincipal = conf.get(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
            hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                metaStoreServicePrincipal);
            hcatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");

            Token<DelegationTokenIdentifier> delegationTokenId = getDelegationToken(
                hcatConf, metaStoreServicePrincipal);
            proxyUGI.addToken(delegationTokenId);
        }
    }

    private static Token<DelegationTokenIdentifier> getDelegationToken(HiveConf hcatConf,
                                                                       String metaStoreServicePrincipal)
        throws IOException {

        LOG.debug("Creating delegation tokens for principal={}", metaStoreServicePrincipal);
        HCatClient hcatClient = HCatClient.create(hcatConf);
        String delegationToken = hcatClient.getDelegationToken(
                CurrentUser.getUser(), metaStoreServicePrincipal);
        hcatConf.set("hive.metastore.token.signature", "FalconService");

        Token<DelegationTokenIdentifier> delegationTokenId = new Token<DelegationTokenIdentifier>();
        delegationTokenId.decodeFromUrlString(delegationToken);
        delegationTokenId.setService(new Text("FalconService"));
        LOG.info("Created delegation token={}", delegationToken);
        return delegationTokenId;
    }

    @Override
    public boolean isAlive(Configuration conf, final String catalogUrl) throws FalconException {
        LOG.info("Checking if the service is alive for: {}", catalogUrl);

        try {
            HiveMetaStoreClient client = createProxiedClient(conf, catalogUrl);
            Database database = client.getDatabase("default");
            return database != null;
        } catch (Exception e) {
            throw new FalconException("Exception checking if the service is alive:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean tableExists(Configuration conf, final String catalogUrl, final String database,
                               final String tableName) throws FalconException {
        LOG.info("Checking if the table exists: {}", tableName);

        try {
            HiveMetaStoreClient client = createProxiedClient(conf, catalogUrl);
            Table table = client.getTable(database, tableName);
            return table != null;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (Exception e) {
            throw new FalconException("Exception checking if the table exists:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean isTableExternal(Configuration conf, String catalogUrl, String database,
                                   String tableName) throws FalconException {
        LOG.info("Checking if the table is external: {}", tableName);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            Table table = client.getTable(database, tableName);
            return table.getTableType().equals(TableType.EXTERNAL_TABLE.name());
        } catch (Exception e) {
            throw new FalconException("Exception checking if the table is external:" + e.getMessage(), e);
        }
    }

    @Override
    public List<CatalogPartition> listPartitions(Configuration conf, String catalogUrl,
                                                         String database, String tableName,
                                                         List<String> values) throws FalconException {
        LOG.info("List partitions for: {}, partition filter: {}", tableName, values);

        try {
            List<CatalogPartition> catalogPartitionList = new ArrayList<CatalogPartition>();

            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            List<Partition> hCatPartitions = client.listPartitions(database, tableName, values, (short) -1);
            for (Partition hCatPartition : hCatPartitions) {
                LOG.debug("Partition: " + hCatPartition.getValues());
                CatalogPartition partition = createCatalogPartition(hCatPartition);
                catalogPartitionList.add(partition);
            }

            return catalogPartitionList;
        } catch (Exception e) {
            throw new FalconException("Exception listing partitions:" + e.getMessage(), e);
        }
    }

    @Override
    public List<CatalogPartition> listPartitionsByFilter(Configuration conf, String catalogUrl,
                                                         String database, String tableName,
                                                         String filter) throws FalconException {
        LOG.info("List partitions for: {}, partition filter: {}", tableName, filter);

        try {
            List<CatalogPartition> catalogPartitionList = new ArrayList<CatalogPartition>();

            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            List<Partition> hCatPartitions = client.listPartitionsByFilter(database, tableName, filter, (short) -1);
            for (Partition hCatPartition : hCatPartitions) {
                LOG.info("Partition: " + hCatPartition.getValues());
                CatalogPartition partition = createCatalogPartition(hCatPartition);
                catalogPartitionList.add(partition);
            }

            return catalogPartitionList;
        } catch (Exception e) {
            throw new FalconException("Exception listing partitions:" + e.getMessage(), e);
        }
    }

    private CatalogPartition createCatalogPartition(Partition hCatPartition) {
        final CatalogPartition catalogPartition = new CatalogPartition();
        catalogPartition.setDatabaseName(hCatPartition.getDbName());
        catalogPartition.setTableName(hCatPartition.getTableName());
        catalogPartition.setValues(hCatPartition.getValues());
        catalogPartition.setInputFormat(hCatPartition.getSd().getInputFormat());
        catalogPartition.setOutputFormat(hCatPartition.getSd().getOutputFormat());
        catalogPartition.setLocation(hCatPartition.getSd().getLocation());
        catalogPartition.setSerdeInfo(hCatPartition.getSd().getSerdeInfo().getSerializationLib());
        catalogPartition.setCreateTime(hCatPartition.getCreateTime());
        catalogPartition.setLastAccessTime(hCatPartition.getLastAccessTime());
        return catalogPartition;
    }

    //Drop single partition
    @Override
    public boolean dropPartition(Configuration conf, String catalogUrl,
                                  String database, String tableName,
                                  List<String> partitionValues, boolean deleteData) throws FalconException {
        LOG.info("Dropping partition for: {}, partition: {}", tableName, partitionValues);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            return client.dropPartition(database, tableName, partitionValues, deleteData);
        } catch (Exception e) {
            throw new FalconException("Exception dropping partitions:" + e.getMessage(), e);
        }
    }

    @Override
    public void dropPartitions(Configuration conf, String catalogUrl,
                               String database, String tableName,
                               List<String> partitionValues, boolean deleteData) throws FalconException {
        LOG.info("Dropping partitions for: {}, partitions: {}", tableName, partitionValues);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            List<Partition> partitions = client.listPartitions(database, tableName, partitionValues, (short) -1);
            for (Partition part : partitions) {
                LOG.info("Dropping partition for: {}, partition: {}", tableName, part.getValues());
                client.dropPartition(database, tableName, part.getValues(), deleteData);
            }
        } catch (Exception e) {
            throw new FalconException("Exception dropping partitions:" + e.getMessage(), e);
        }
    }

    @Override
    public CatalogPartition getPartition(Configuration conf, String catalogUrl,
                                         String database, String tableName,
                                         List<String> partitionValues) throws FalconException {
        LOG.info("Fetch partition for: {}, partition spec: {}", tableName, partitionValues);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            Partition hCatPartition = client.getPartition(database, tableName, partitionValues);
            return createCatalogPartition(hCatPartition);
        } catch (Exception e) {
            throw new FalconException("Exception fetching partition:" + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getPartitionColumns(Configuration conf, String catalogUrl, String database,
                                            String tableName) throws FalconException {
        LOG.info("Fetching partition columns of table: " + tableName);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            Table table = client.getTable(database, tableName);
            List<String> partCols = new ArrayList<String>();
            for (FieldSchema part : table.getPartitionKeys()) {
                partCols.add(part.getName());
            }
            return partCols;
        } catch (Exception e) {
            throw new FalconException("Exception fetching partition columns: " + e.getMessage(), e);
        }
    }

    @Override
    public void addPartition(Configuration conf, String catalogUrl, String database,
                             String tableName, List<String> partValues, String location) throws FalconException {
        LOG.info("Adding partition {} for {}.{} with location {}", partValues, database, tableName, location);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            Table table = client.getTable(database, tableName);
            org.apache.hadoop.hive.metastore.api.Partition part = new org.apache.hadoop.hive.metastore.api.Partition();
            part.setDbName(database);
            part.setTableName(tableName);
            part.setValues(partValues);
            part.setSd(table.getSd());
            part.getSd().setLocation(location);
            part.setParameters(table.getParameters());
            if (part.getParameters() == null) {
                part.setParameters(new HashMap<String, String>());
            }
            part.getParameters().put(CREATE_TIME, String.valueOf(System.currentTimeMillis()));
            client.add_partition(part);

        } catch (Exception e) {
            throw new FalconException("Exception adding partition: " + e.getMessage(), e);
        }
    }

    @Override
    public void updatePartition(Configuration conf, String catalogUrl, String database,
                             String tableName, List<String> partValues, String location) throws FalconException {
        LOG.info("Updating partition {} of {}.{} with location {}", partValues, database, tableName, location);

        try {
            HiveMetaStoreClient client = createClient(conf, catalogUrl);
            Table table = client.getTable(database, tableName);
            org.apache.hadoop.hive.metastore.api.Partition part = new org.apache.hadoop.hive.metastore.api.Partition();
            part.setDbName(database);
            part.setTableName(tableName);
            part.setValues(partValues);
            part.setSd(table.getSd());
            part.getSd().setLocation(location);
            part.setParameters(table.getParameters());
            if (part.getParameters() == null) {
                part.setParameters(new HashMap<String, String>());
            }
            part.getParameters().put(UPDATE_TIME, String.valueOf(System.currentTimeMillis()));
            client.alter_partition(database, tableName, part);
        } catch (Exception e) {
            throw new FalconException("Exception updating partition: " + e.getMessage(), e);
        }
    }
}

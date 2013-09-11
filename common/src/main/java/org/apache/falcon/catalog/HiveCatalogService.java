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
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatDatabase;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatException;
import org.apache.log4j.Logger;

import java.util.HashMap;
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
            HiveConf hcatConf = new HiveConf();
            hcatConf.set("hive.metastore.local", "false");
            hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
            hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
            hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                    HCatSemanticAnalyzer.class.getName());
            hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

            hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
            hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");

            return HCatClient.create(hcatConf);
        } catch (HCatException e) {
            throw new FalconException(e);
        }
    }


    @Override
    public boolean isAlive(String catalogBaseUrl) throws FalconException {
        LOG.info("Checking if the service is alive for: " + catalogBaseUrl);

        try {
            HCatClient client = get(catalogBaseUrl);
            client.close();
            HCatDatabase database = client.getDatabase("default");
            return database != null;
        } catch (HCatException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public boolean tableExists(String catalogUrl, String database, String tableName)
        throws FalconException {
        LOG.info("Checking if the table exists: " + tableName);

        try {
            HCatClient client = get(catalogUrl);
            HCatTable table = client.getTable(database, tableName);
            return table != null;
        } catch (HCatException e) {
            throw new FalconException(e);
        }
    }

    @Override
    public Map<String, String> listTableProperties(String catalogUrl, String database,
                                                   String tableName) throws FalconException {
        LOG.info("Returns a list of table properties for:" + tableName);

        try {
            HCatClient client = get(catalogUrl);
            HCatTable table = client.getTable(database, tableName);

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("database", table.getDbName());
            tableProperties.put("tableName", table.getTableName());
            tableProperties.put("tabletype", table.getTabletype());
            tableProperties.put("location", table.getLocation());
            // tableProperties.putAll(table.getTblProps());

            return tableProperties;
        } catch (HCatException e) {
            throw new FalconException(e);
        }
    }
}

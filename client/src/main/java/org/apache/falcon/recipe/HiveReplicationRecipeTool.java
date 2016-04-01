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

package org.apache.falcon.recipe;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.api.ObjectNotFoundException;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;

import java.io.IOException;
import java.util.Properties;

/**
 * Hive Replication recipe tool for Falcon recipes.
 */
public class HiveReplicationRecipeTool implements Recipe {
    private static final String ALL_TABLES = "*";

    @Override
    public void validate(final Properties recipeProperties) throws Exception {
        for (HiveReplicationRecipeToolOptions option : HiveReplicationRecipeToolOptions.values()) {
            if (recipeProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing argument: " + option.getName());
            }
        }

        HCatClient sourceMetastoreClient = null;
        HCatClient targetMetastoreClient = null;
        try {
            // Validate if DB exists - source and target
            sourceMetastoreClient = getHiveMetaStoreClient(
                    recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                            .REPLICATION_SOURCE_METASTORE_URI.getName()),
                    recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                            .REPLICATION_SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL.getName()),
                    recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                            .REPLICATION_SOURCE_HIVE2_KERBEROS_PRINCIPAL.getName()));

            String sourceDbList = recipeProperties.getProperty(
                    HiveReplicationRecipeToolOptions.REPLICATION_SOURCE_DATABASE.getName());

            if (StringUtils.isEmpty(sourceDbList)) {
                throw new Exception("No source DB specified in property file");
            }

            String sourceTableList = recipeProperties.getProperty(
                    HiveReplicationRecipeToolOptions.REPLICATION_SOURCE_TABLE.getName());
            if (StringUtils.isEmpty(sourceTableList)) {
                throw new Exception("No source table specified in property file. For DB replication please specify * "
                        + "for sourceTable");
            }

            String[] srcDbs = sourceDbList.split(",");
            if (srcDbs.length <= 0) {
                throw new Exception("No source DB specified in property file");
            }
            for (String db : srcDbs) {
                if (!dbExists(sourceMetastoreClient, db)) {
                    throw new Exception("Database " + db + " doesn't exist on source cluster");
                }
            }

            if (!sourceTableList.equals(ALL_TABLES)) {
                String[] srcTables = sourceTableList.split(",");
                if (srcTables.length > 0) {
                    for (String table : srcTables) {
                        if (!tableExists(sourceMetastoreClient, srcDbs[0], table)) {
                            throw new Exception("Table " + table + " doesn't exist on source cluster");
                        }
                    }
                }
            }

            targetMetastoreClient = getHiveMetaStoreClient(
                    recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                            .REPLICATION_TARGET_METASTORE_URI.getName()),
                    recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                            .REPLICATION_TARGET_HIVE_METASTORE_KERBEROS_PRINCIPAL.getName()),
                    recipeProperties.getProperty(HiveReplicationRecipeToolOptions
                            .REPLICATION_TARGET_HIVE2_KERBEROS_PRINCIPAL.getName()));
            // Verify db exists on target
            for (String db : srcDbs) {
                if (!dbExists(targetMetastoreClient, db)) {
                    throw new Exception("Database " + db + " doesn't exist on target cluster");
                }
            }
        } finally {
            if (sourceMetastoreClient != null) {
                sourceMetastoreClient.close();
            }
            if (targetMetastoreClient != null) {
                targetMetastoreClient.close();
            }
        }
    }

    @Override
    public Properties getAdditionalSystemProperties(final Properties recipeProperties) {
        Properties additionalProperties = new Properties();
        String recipeName = recipeProperties.getProperty(RecipeToolOptions.RECIPE_NAME.getName());
        // Add recipe name as Hive DR job
        additionalProperties.put(HiveReplicationRecipeToolOptions.HIVE_DR_JOB_NAME.getName(), recipeName);
        additionalProperties.put(HiveReplicationRecipeToolOptions.CLUSTER_FOR_JOB_RUN.getName(),
                recipeProperties.getProperty(RecipeToolOptions.CLUSTER_NAME.getName()));
        additionalProperties.put(HiveReplicationRecipeToolOptions.CLUSTER_FOR_JOB_RUN_WRITE_EP.getName(),
                recipeProperties.getProperty(RecipeToolOptions.CLUSTER_HDFS_WRITE_ENDPOINT.getName()));
        if (StringUtils.isNotEmpty(recipeProperties.getProperty(RecipeToolOptions.RECIPE_NN_PRINCIPAL.getName()))) {
            additionalProperties.put(HiveReplicationRecipeToolOptions.CLUSTER_FOR_JOB_NN_KERBEROS_PRINCIPAL.getName(),
                    recipeProperties.getProperty(RecipeToolOptions.RECIPE_NN_PRINCIPAL.getName()));
        }
        if (StringUtils.isEmpty(
                recipeProperties.getProperty(HiveReplicationRecipeToolOptions.TDE_ENCRYPTION_ENABLED.getName()))) {
            additionalProperties.put(HiveReplicationRecipeToolOptions.TDE_ENCRYPTION_ENABLED.getName(), "false");
        }
        return additionalProperties;
    }

    private HCatClient getHiveMetaStoreClient(String metastoreUrl, String metastorePrincipal,
                                              String hive2Principal) throws Exception {
        try {
            HiveConf hcatConf = createHiveConf(new Configuration(false), metastoreUrl,
                    metastorePrincipal, hive2Principal);
            return HCatClient.create(hcatConf);
        } catch (IOException e) {
            throw new Exception("Exception creating HCatClient: " + e.getMessage(), e);
        }
    }

    private static HiveConf createHiveConf(Configuration conf, String metastoreUrl, String metastorePrincipal,
                                           String hive2Principal) throws IOException {
        HiveConf hcatConf = new HiveConf(conf, HiveConf.class);

        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        if (StringUtils.isNotEmpty(metastorePrincipal)) {
            hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, metastorePrincipal);
            hcatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
            hcatConf.set(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "true");
        }
        if (StringUtils.isNotEmpty(hive2Principal)) {
            hcatConf.set(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname, hive2Principal);
            hcatConf.set(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, "kerberos");
        }

        return hcatConf;
    }

    private static boolean tableExists(HCatClient client, final String database, final String tableName)
        throws Exception {
        try {
            HCatTable table = client.getTable(database, tableName);
            return table != null;
        } catch (ObjectNotFoundException e) {
            System.out.println(e.getMessage());
            return false;
        } catch (HCatException e) {
            throw new Exception("Exception checking if the table exists:" + e.getMessage(), e);
        }
    }

    private static boolean dbExists(HCatClient client, final String database)
        throws Exception {
        try {
            HCatDatabase db = client.getDatabase(database);
            return db != null;
        } catch (ObjectNotFoundException e) {
            System.out.println(e.getMessage());
            return false;
        } catch (HCatException e) {
            throw new Exception("Exception checking if the db exists:" + e.getMessage(), e);
        }
    }
}

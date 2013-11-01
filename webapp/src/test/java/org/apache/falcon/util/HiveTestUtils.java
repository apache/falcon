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

package org.apache.falcon.util;

import org.apache.falcon.catalog.HiveCatalogService;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatCreateDBDesc;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.data.schema.HCatFieldSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive Utility class for integration-tests.
 */
public final class HiveTestUtils {

    private HiveTestUtils() {
    }

    public static void createDatabase(String metaStoreUrl,
                                      String databaseName) throws Exception {
        HCatClient client = HiveCatalogService.get(metaStoreUrl);
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(databaseName)
                .ifNotExists(true).build();
        client.createDatabase(dbDesc);
    }

    public static  void dropDatabase(String metaStoreUrl, String databaseName) throws Exception {
        HCatClient client = HiveCatalogService.get(metaStoreUrl);
        client.dropDatabase(databaseName, true, HCatClient.DropDBMode.CASCADE);
    }

    public static void createTable(String metaStoreUrl, String databaseName,
                                   String tableName) throws Exception {
        HCatClient client = HiveCatalogService.get(metaStoreUrl);
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.INT, "id comment"));
        cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(databaseName, tableName, cols)
                .ifNotExists(true)
                .comments("falcon integration test")
                .build();
        client.createTable(tableDesc);
    }

    public static void createTable(String metaStoreUrl, String databaseName, String tableName,
                                   List<String> partitionKeys) throws Exception {
        HCatClient client = HiveCatalogService.get(metaStoreUrl);
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.INT, "id comment"));
        cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

        List<HCatFieldSchema> partitionSchema = new ArrayList<HCatFieldSchema>();
        for (String partitionKey : partitionKeys) {
            partitionSchema.add(new HCatFieldSchema(partitionKey, HCatFieldSchema.Type.STRING, ""));
        }

        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(databaseName, tableName, cols)
                .ifNotExists(true)
                .comments("falcon integration test")
                .partCols(new ArrayList<HCatFieldSchema>(partitionSchema))
                .build();
        client.createTable(tableDesc);
    }

    public static void createExternalTable(String metaStoreUrl, String databaseName, String tableName,
                                           List<String> partitionKeys, String externalLocation) throws Exception {
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.INT, "id comment"));
        cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

        List<HCatFieldSchema> partitionSchema = new ArrayList<HCatFieldSchema>();
        for (String partitionKey : partitionKeys) {
            partitionSchema.add(new HCatFieldSchema(partitionKey, HCatFieldSchema.Type.STRING, ""));
        }

        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                .create(databaseName, tableName, cols)
                .fileFormat("rcfile")
                .ifNotExists(true)
                .comments("falcon integration test")
                .partCols(new ArrayList<HCatFieldSchema>(partitionSchema))
                .isTableExternal(true)
                .location(externalLocation)
                .build();

        HCatClient client = HiveCatalogService.get(metaStoreUrl);
        client.createTable(tableDesc);
    }

    public static void alterTable(String metaStoreUrl, String databaseName,
                                  String tableName) throws Exception {
        StringBuilder alterTableDdl = new StringBuilder();
        alterTableDdl
                .append(" alter table ")
                .append(tableName)
                .append(" set fileformat ")
                .append(" inputformat 'org.apache.hadoop.mapred.TextInputFormat' ")
                .append(" outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        startSessionState(metaStoreUrl);
        execHiveDDL("use " + databaseName);
        execHiveDDL(alterTableDdl.toString());
    }

    public static void loadData(String metaStoreUrl, String databaseName, String tableName,
                                String path, String partition) throws Exception {
        StringBuilder ddl = new StringBuilder();
        ddl.append(" load data inpath ")
            .append(" '").append(path).append("' ")
            .append(" into table ")
            .append(tableName)
            .append(" partition ").append(" (ds='").append(partition).append("') ");

        startSessionState(metaStoreUrl);
        execHiveDDL("use " + databaseName);
        execHiveDDL(ddl.toString());
    }

    public static void dropTable(String metaStoreUrl, String databaseName,
                                 String tableName) throws Exception {
        HCatClient client = HiveCatalogService.get(metaStoreUrl);
        client.dropTable(databaseName, tableName, true);
    }

    public static void startSessionState(String metaStoreUrl) {
        HiveConf hcatConf = new HiveConf();
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUrl);
        hcatConf.set("hive.metastore.local", "false");
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hcatConf.set("hive.root.logger", "DEBUG,console");
        SessionState.start(hcatConf);
    }

    public static void execHiveDDL(String ddl) throws Exception {
        System.out.println("Executing ddl = " + ddl);

        Driver hiveDriver = new Driver();
        CommandProcessorResponse response = hiveDriver.run(ddl);

        System.out.println("response = " + response);
        System.out.println("response.getResponseCode() = " + response.getResponseCode());
        System.out.println("response.getErrorMessage() = " + response.getErrorMessage());
        System.out.println("response.getSQLState() = " + response.getSQLState());

        if (response.getResponseCode() > 0) {
            throw new Exception(response.getErrorMessage());
        }
    }

    public static HCatPartition getPartition(String metastoreUrl, String databaseName,
                                             String tableName, String partitionKey,
                                             String partitionValue) throws Exception {
        Map<String, String> partitionSpec = new HashMap<String, String>();
        partitionSpec.put(partitionKey, partitionValue);

        return HiveCatalogService.get(metastoreUrl).getPartition(databaseName, tableName, partitionSpec);
    }
}

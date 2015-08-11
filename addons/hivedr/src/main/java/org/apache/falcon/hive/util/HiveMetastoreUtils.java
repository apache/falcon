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

package org.apache.falcon.hive.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.repl.exim.EximReplicationTaskFactory;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Create hive metastore client for user.
 */
public final class HiveMetastoreUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreUtils.class);

    private HiveMetastoreUtils() {}

    public static HCatClient initializeHiveMetaStoreClient(String metastoreUri, String metastorePrincipal,
                                                    String hive2Principal) throws Exception {
        try {
            HiveConf hcatConf = createHiveConf(HiveDRUtils.getDefaultConf(),
                    metastoreUri, metastorePrincipal, hive2Principal);
            HCatClient client = HCatClient.create(hcatConf);
            return client;
        } catch (IOException e) {
            throw new Exception("Exception creating HCatClient: " + e.getMessage(), e);
        }
    }

    private static HiveConf createHiveConf(Configuration conf, String metastoreUrl, String metastorePrincipal,
                                           String hive2Principal) throws IOException {
        JobConf jobConf = new JobConf(conf);
        String delegationToken = HiveDRUtils.getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            Credentials credentials = Credentials.readTokenStorageFile(new File(delegationToken), conf);
            jobConf.setCredentials(credentials);
            UserGroupInformation.getCurrentUser().addCredentials(credentials);
        }

        HiveConf hcatConf = new HiveConf(jobConf, HiveConf.class);

        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUrl);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.HIVE_REPL_TASK_FACTORY.varname, EximReplicationTaskFactory.class.getName());
        if (StringUtils.isNotEmpty(metastorePrincipal)) {
            hcatConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, metastorePrincipal);
            hcatConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
            hcatConf.set(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "true");
            hcatConf.set("hadoop.rpc.protection", "authentication");
        }
        if (StringUtils.isNotEmpty(hive2Principal)) {
            hcatConf.set(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname, hive2Principal);
            hcatConf.set(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, "kerberos");
        }

        return hcatConf;
    }

}

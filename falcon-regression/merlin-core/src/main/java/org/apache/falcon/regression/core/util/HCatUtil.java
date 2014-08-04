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

package org.apache.falcon.regression.core.util;


import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

/**
 * util methods for HCat.
 */
public final class HCatUtil {
    private HCatUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    public static HCatClient getHCatClient(String hCatEndPoint, String hiveMetaStorePrinciple)
        throws HCatException {
        HiveConf hcatConf = new HiveConf();
        hcatConf.set("hive.metastore.local", "false");
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, hCatEndPoint);
        hcatConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, hiveMetaStorePrinciple);
        hcatConf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, MerlinConstants.IS_SECURE);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
            HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        return HCatClient.create(hcatConf);
    }

    @SuppressWarnings("deprecation")
    public static HCatFieldSchema getStringSchema(String fieldName, String comment) throws HCatException {
        return new HCatFieldSchema(fieldName, HCatFieldSchema.Type.STRING, comment);
    }
}

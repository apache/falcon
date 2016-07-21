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

package org.apache.falcon.oozie;

import com.google.common.base.Splitter;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.DatasourceHelper;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.datasource.Credential;
import org.apache.falcon.entity.v0.datasource.Credentialtype;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.datasource.DatasourceType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.sqoop.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.SecurityUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Helper class that implements common functions across Import and Export.
 */

public final class ImportExportCommon {

    static final String ARG_SEPARATOR = " ";

    public static final Logger LOG = LoggerFactory.getLogger(ImportExportCommon.class);

    private static final Set<String> FALCON_IMPORT_SQOOP_ACTIONS = new HashSet<>(
            Arrays.asList(new String[]{ OozieOrchestrationWorkflowBuilder.PREPROCESS_ACTION_NAME,
                                        OozieOrchestrationWorkflowBuilder.USER_ACTION_NAME, }));

    private ImportExportCommon() {
    }

    static StringBuilder buildUserPasswordArg(StringBuilder builder, StringBuilder sqoopOpts, Credential cred)
        throws FalconException {
        builder.append("--username").append(ARG_SEPARATOR)
                .append(cred.getUserName())
                .append(ARG_SEPARATOR);
        if (cred.getType() == Credentialtype.PASSWORD_FILE) {
            builder.append("--password-file");
            builder.append(ARG_SEPARATOR).append(cred.getPasswordFile());
        } else if (cred.getType() == Credentialtype.PASSWORD_ALIAS) {
            try {
                sqoopOpts.append("-D")
                        .append(DatasourceHelper.HADOOP_CREDENTIAL_PROVIDER_FILEPATH)
                        .append("=")
                        .append(DatasourceHelper
                                .buildJceksProviderPath(new URI(cred.getPasswordAlias().getProviderPath())));
            } catch(URISyntaxException uriEx) {
                throw new FalconException(uriEx);
            }
            builder.append("--password-alias");
            builder.append(ARG_SEPARATOR).append(cred.getPasswordAlias().getAlias());
        } else {
            builder.append("--password");
            builder.append(ARG_SEPARATOR).append(cred.getPasswordText());
        }
        return builder;
    }

    public static void addHCatalogProperties(Properties props, Feed entity, Cluster cluster,
                                             WORKFLOWAPP workflow, OozieOrchestrationWorkflowBuilder<Feed> wBuilder,
                                             Path buildPath, ACTION sqoopAction)
        throws FalconException {
        if (FeedHelper.getStorageType(entity, cluster) == Storage.TYPE.TABLE) {
            wBuilder.createHiveConfiguration(cluster, buildPath, "");
            addHCatalogShareLibs(props);
            if (SecurityUtil.isSecurityEnabled()) {
                // add hcatalog credentials for secure mode and add a reference to each action
                wBuilder.addHCatalogCredentials(workflow, cluster,
                        OozieOrchestrationWorkflowBuilder.HIVE_CREDENTIAL_NAME, FALCON_IMPORT_SQOOP_ACTIONS);
            }
            sqoopAction.getJobXml().add("${wf:appPath()}/conf/hive-site.xml");
        }
    }
    private static void addHCatalogShareLibs(Properties props) throws FalconException {
        props.put("oozie.action.sharelib.for.sqoop", "sqoop,hive,hcatalog");
    }

    public static Map<String, String> getPartitionKeyValues(String partitionStr) {
        return Splitter.on(";").withKeyValueSeparator("=").split(partitionStr);
    }

    public static StringBuilder buildDriverArgs(StringBuilder builder, Datasource db)
        throws FalconException {
        if ((db.getType() == DatasourceType.GENERIC) && (db.getDriver() != null)
                && (db.getDriver().getClazz() != null)) {
            builder.append("--driver").append(ImportExportCommon.ARG_SEPARATOR).append(db.getDriver().getClazz());
        }
        return builder;
    }

    public static StringBuilder buildConnectArg(StringBuilder builder, String endPoint) throws FalconException {
        return builder.append("--connect").append(ImportExportCommon.ARG_SEPARATOR)
                .append(endPoint);
    }

    public static StringBuilder buildTableArg(StringBuilder builder, String tableName) throws FalconException {
        return builder.append("--table").append(ImportExportCommon.ARG_SEPARATOR)
                .append(tableName);
    }

    public static void buildConnectionManagerArg(StringBuilder sqoopArgs, Datasource datasource) {
        Map<String, String> dsProps = DatasourceHelper.getDatasourceProperties(datasource);
        if (dsProps.containsKey("connection-manager")) {
            sqoopArgs.append("--connection-manager").append(ImportExportCommon.ARG_SEPARATOR)
                .append(dsProps.get("connection-manager")).append(ImportExportCommon.ARG_SEPARATOR);
        }
    }
}

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

package org.apache.falcon.extensions.mirroring.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.AbstractCatalogService;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.extensions.AbstractExtension;
import org.apache.falcon.extensions.ExtensionProperties;
import org.apache.falcon.security.SecurityUtil;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Hive mirroring extension.
 */
public class HiveMirroringExtension extends AbstractExtension {
    private static final String EXTENSION_NAME = "HIVE-MIRRORING";
    private static final String ALL_TABLES = "*";
    private static final String COMMA_DELIMITER = ",";
    private static final String SECURE_RESOURCE = "-secure";
    private static final String NOT_APPLICABLE = "NA";

    @Override
    public String getName() {
        return EXTENSION_NAME;
    }

    @Override
    public void validate(final Properties extensionProperties) throws FalconException {
        for (HiveMirroringExtensionProperties property : HiveMirroringExtensionProperties.values()) {
            if (extensionProperties.getProperty(property.getName()) == null && property.isRequired()) {
                throw new FalconException("Missing extension property: " + property.getName());
            }
        }

        String srcClusterName = extensionProperties.getProperty(HiveMirroringExtensionProperties.SOURCE_CLUSTER
                .getName());
        Cluster srcCluster = ClusterHelper.getCluster(srcClusterName);
        if (srcCluster == null) {
            throw new FalconException("Cluster entity " + srcClusterName + " not found");
        }
        String srcClusterCatalogUrl = ClusterHelper.getRegistryEndPoint(srcCluster);
        Configuration srcClusterConf = ClusterHelper.getConfiguration(srcCluster);

        // Validate if DB exists - source and target
        String sourceDbList = extensionProperties.getProperty(
                HiveMirroringExtensionProperties.SOURCE_DATABASES.getName());

        if (StringUtils.isBlank(sourceDbList)) {
            throw new FalconException("No source DB specified for Hive mirroring");
        }

        AbstractCatalogService catalogService = CatalogServiceFactory.getCatalogService();
        String[] srcDbs = sourceDbList.split(COMMA_DELIMITER);
        if (srcDbs.length <= 0) {
            throw new FalconException("No source DB specified for Hive mirroring");
        }
        for (String db : srcDbs) {
            if (!catalogService.dbExists(srcClusterConf, srcClusterCatalogUrl, db)) {
                throw new FalconException("Database " + db + " doesn't exist on cluster" + srcCluster.getName());
            }
        }

        String sourceTableList = extensionProperties.getProperty(
                HiveMirroringExtensionProperties.SOURCE_TABLES.getName());
        if (StringUtils.isNotBlank(sourceTableList)) {
            if (!sourceTableList.equals(ALL_TABLES)) {
                String db = srcDbs[0];
                String[] srcTables = sourceTableList.split(COMMA_DELIMITER);
                for (String table : srcTables) {
                    if (!catalogService.tableExists(srcClusterConf, srcClusterCatalogUrl, db, table)) {
                        throw new FalconException("Table " + table + " doesn't exist on cluster"
                                + srcCluster.getName());
                    }
                }
            }
        }

        // Verify db exists on target
        String targetClusterName = extensionProperties.getProperty(HiveMirroringExtensionProperties.TARGET_CLUSTER
                .getName());
        Cluster targetCluster = ClusterHelper.getCluster(targetClusterName);
        if (targetCluster == null) {
            throw new FalconException("Cluster entity " + targetClusterName + " not found");
        }
        String targetClusterCatalogUrl = ClusterHelper.getRegistryEndPoint(targetCluster);
        Configuration targetClusterConf = ClusterHelper.getConfiguration(targetCluster);

        for (String db : srcDbs) {
            if (!catalogService.dbExists(targetClusterConf, targetClusterCatalogUrl, db)) {
                throw new FalconException("Database " + db + " doesn't exist on cluster" + targetCluster.getName());
            }
        }
    }

    @Override
    public Properties getAdditionalProperties(final Properties extensionProperties) throws FalconException {
        Properties additionalProperties = new Properties();

        String jobName = extensionProperties.getProperty(ExtensionProperties.JOB_NAME.getName());
        // Add job name as Hive DR job
        additionalProperties.put(HiveMirroringExtensionProperties.HIVE_MIRRORING_JOB_NAME.getName(),
                jobName);

        // Get the first source DB
        additionalProperties.put(HiveMirroringExtensionProperties.SOURCE_DATABASE.getName(),
                extensionProperties.getProperty(HiveMirroringExtensionProperties.SOURCE_DATABASES
                .getName()).trim().split(",")[0]
        );

        String clusterName = extensionProperties.getProperty(ExtensionProperties.CLUSTER_NAME.getName());
        // Add required properties of cluster where job should run
        additionalProperties.put(HiveMirroringExtensionProperties.CLUSTER_FOR_JOB_RUN.getName(),
                clusterName);
        Cluster jobCluster = ClusterHelper.getCluster(clusterName);
        if (jobCluster == null) {
            throw new FalconException("Cluster entity " + clusterName + " not found");
        }
        additionalProperties.put(HiveMirroringExtensionProperties.CLUSTER_FOR_JOB_RUN_WRITE_EP.getName(),
                ClusterHelper.getStorageUrl(jobCluster));

        // Check if job cluster is secure
        String jobClusterKerberosPrincipal = ClusterHelper.getPropertyValue(jobCluster, SecurityUtil.NN_PRINCIPAL);
        if (StringUtils.isNotBlank(jobClusterKerberosPrincipal)) {
            // Add -secure and update the resource name
            String resourceName = getName().toLowerCase() + SECURE_RESOURCE;
            additionalProperties.put(ExtensionProperties.RESOURCE_NAME.getName(), resourceName);
        } else {
            jobClusterKerberosPrincipal = NOT_APPLICABLE;
        }
        additionalProperties.put(HiveMirroringExtensionProperties.CLUSTER_FOR_JOB_NN_KERBEROS_PRINCIPAL.getName(),
                jobClusterKerberosPrincipal);

        // Properties for src cluster
        String srcClusterName = extensionProperties.getProperty(HiveMirroringExtensionProperties.SOURCE_CLUSTER
                .getName());
        Cluster srcCluster = ClusterHelper.getCluster(srcClusterName);
        if (srcCluster == null) {
            throw new FalconException("Cluster entity " + srcClusterName + " not found");
        }
        additionalProperties.put(HiveMirroringExtensionProperties.SOURCE_METASTORE_URI.getName(),
                ClusterHelper.getRegistryEndPoint(srcCluster));
        additionalProperties.put(HiveMirroringExtensionProperties.SOURCE_NN.getName(),
                ClusterHelper.getStorageUrl(srcCluster));

        String sourceTableList = extensionProperties.getProperty(
                HiveMirroringExtensionProperties.SOURCE_TABLES.getName());
        if (StringUtils.isBlank(sourceTableList)) {
            additionalProperties.put(HiveMirroringExtensionProperties.SOURCE_TABLES.getName(), ALL_TABLES);
        }

        // Check if source cluster is secure
        String srcClusterKerberosPrincipal = ClusterHelper.getPropertyValue(srcCluster, SecurityUtil.NN_PRINCIPAL);
        if (StringUtils.isBlank(srcClusterKerberosPrincipal)) {
            srcClusterKerberosPrincipal = NOT_APPLICABLE;
        }

        String srcHiveMetastorePrincipal = ClusterHelper.getPropertyValue(
                srcCluster, SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
        if (StringUtils.isBlank(srcHiveMetastorePrincipal)) {
            srcHiveMetastorePrincipal = NOT_APPLICABLE;
        }
        additionalProperties.put(HiveMirroringExtensionProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName(),
                srcClusterKerberosPrincipal);
        additionalProperties.put(
                HiveMirroringExtensionProperties.SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL.getName(),
                srcHiveMetastorePrincipal);

        // Properties for target cluster
        String targetClusterName = extensionProperties.getProperty(HiveMirroringExtensionProperties.TARGET_CLUSTER
                .getName());
        Cluster targetCluster = ClusterHelper.getCluster(targetClusterName);
        if (targetCluster == null) {
            throw new FalconException("Cluster entity " + targetClusterName + " not found");
        }
        additionalProperties.put(HiveMirroringExtensionProperties.TARGET_METASTORE_URI.getName(),
                ClusterHelper.getRegistryEndPoint(targetCluster));
        additionalProperties.put(HiveMirroringExtensionProperties.TARGET_NN.getName(),
                ClusterHelper.getStorageUrl(targetCluster));

        // Check if target cluster is secure
        String targetClusterKerberosPrincipal = ClusterHelper.getPropertyValue(
                targetCluster, SecurityUtil.NN_PRINCIPAL);
        if (StringUtils.isBlank(targetClusterKerberosPrincipal)) {
            targetClusterKerberosPrincipal = NOT_APPLICABLE;
        }

        String targetHiveMetastorePrincipal = ClusterHelper.getPropertyValue(targetCluster,
                SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
        if (StringUtils.isBlank(targetHiveMetastorePrincipal)) {
            targetHiveMetastorePrincipal = NOT_APPLICABLE;
        }
        additionalProperties.put(HiveMirroringExtensionProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName(),
                targetClusterKerberosPrincipal);
        additionalProperties.put(
                HiveMirroringExtensionProperties.TARGET_HIVE_METASTORE_KERBEROS_PRINCIPAL.getName(),
                targetHiveMetastorePrincipal);

        // Misc properties
        // Add default properties if not passed
        String maxEvents = extensionProperties.getProperty(HiveMirroringExtensionProperties.MAX_EVENTS.getName());
        if (StringUtils.isBlank(maxEvents)) {
            additionalProperties.put(HiveMirroringExtensionProperties.MAX_EVENTS.getName(), "-1");
        }

        String replicationMaxMaps =
                extensionProperties.getProperty(HiveMirroringExtensionProperties.MAX_MAPS.getName());
        if (StringUtils.isBlank(replicationMaxMaps)) {
            additionalProperties.put(HiveMirroringExtensionProperties.MAX_MAPS.getName(), "5");
        }

        String distcpMaxMaps = extensionProperties.getProperty(
                HiveMirroringExtensionProperties.DISTCP_MAX_MAPS.getName());
        if (StringUtils.isBlank(distcpMaxMaps)) {
            additionalProperties.put(HiveMirroringExtensionProperties.DISTCP_MAX_MAPS.getName(), "1");
        }

        String distcpMapBandwidth = extensionProperties.getProperty(
                HiveMirroringExtensionProperties.MAP_BANDWIDTH_IN_MB.getName());
        if (StringUtils.isBlank(distcpMapBandwidth)) {
            additionalProperties.put(HiveMirroringExtensionProperties.MAP_BANDWIDTH_IN_MB.getName(), "100");
        }

        if (StringUtils.isBlank(
                extensionProperties.getProperty(HiveMirroringExtensionProperties.TDE_ENCRYPTION_ENABLED.getName()))) {
            additionalProperties.put(HiveMirroringExtensionProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        }

        if (StringUtils.isBlank(
                extensionProperties.getProperty(HiveMirroringExtensionProperties.SOURCE_STAGING_PATH.getName()))) {
            additionalProperties.put(HiveMirroringExtensionProperties.SOURCE_STAGING_PATH.getName(), NOT_APPLICABLE);
        }

        if (StringUtils.isBlank(
                extensionProperties.getProperty(HiveMirroringExtensionProperties.TARGET_STAGING_PATH.getName()))) {
            additionalProperties.put(HiveMirroringExtensionProperties.TARGET_STAGING_PATH.getName(), NOT_APPLICABLE);
        }

        return additionalProperties;
    }
}

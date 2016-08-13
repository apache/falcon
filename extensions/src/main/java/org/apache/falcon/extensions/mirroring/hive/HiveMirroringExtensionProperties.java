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

/**
 * Hive mirroring extension properties.
 */

public enum HiveMirroringExtensionProperties {
    SOURCE_CLUSTER("sourceCluster", "Replication source cluster name"),
    SOURCE_METASTORE_URI("sourceMetastoreUri", "Source Hive metastore uri", false),
    SOURCE_HS2_URI("sourceHiveServer2Uri", "Source HS2 uri"),
    SOURCE_HS2_EXTRA_OPTS("sourceHiveServer2ExtraOpts", "Source HS2 extra opts", false),
    SOURCE_DATABASES("sourceDatabases", "List of databases to replicate"),
    SOURCE_DATABASE("sourceDatabase", "Database to verify the setup connection", false),
    SOURCE_TABLES("sourceTables", "List of tables to replicate", false),
    SOURCE_STAGING_PATH("sourceStagingPath", "Location of source staging path", false),
    SOURCE_NN("sourceNN", "Source name node", false),
    SOURCE_NN_KERBEROS_PRINCIPAL("sourceNNKerberosPrincipal", "Source name node kerberos principal", false),
    SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL("sourceHiveMetastoreKerberosPrincipal",
            "Source hive metastore kerberos principal", false),
    SOURCE_HIVE2_KERBEROS_PRINCIPAL("sourceHive2KerberosPrincipal",
            "Source hiveserver2 kerberos principal", false),

    TARGET_CLUSTER("targetCluster", "Target cluster name"),
    TARGET_METASTORE_URI("targetMetastoreUri", "Target Hive metastore uri", false),
    TARGET_HS2_URI("targetHiveServer2Uri", "Target HS2 uri"),
    TARGET_HS2_EXTRA_OPTS("targetHiveServer2ExtraOpts", "Target HS2 extra opts", false),
    TARGET_STAGING_PATH("targetStagingPath", "Location of target staging path", false),
    TARGET_NN("targetNN", "Target name node", false),
    TARGET_NN_KERBEROS_PRINCIPAL("targetNNKerberosPrincipal", "Target name node kerberos principal", false),
    TARGET_HIVE_METASTORE_KERBEROS_PRINCIPAL("targetHiveMetastoreKerberosPrincipal",
            "Target hive metastore kerberos principal", false),
    TARGET_HIVE2_KERBEROS_PRINCIPAL("targetHive2KerberosPrincipal",
            "Target hiveserver2 kerberos principal", false),

    MAX_EVENTS("maxEvents", "Maximum events to replicate", false),
    MAX_MAPS("replicationMaxMaps", "Maximum number of maps used during replication", false),
    DISTCP_MAX_MAPS("distcpMaxMaps", "Maximum number of maps used during distcp", false),
    MAP_BANDWIDTH_IN_MB("distcpMapBandwidth", "Bandwidth in MB/s used by each mapper during replication", false),
    CLUSTER_FOR_JOB_RUN("clusterForJobRun", "Cluster on which replication job runs", false),
    CLUSTER_FOR_JOB_NN_KERBEROS_PRINCIPAL("clusterForJobNNKerberosPrincipal", "Job cluster kerberos principal",
            false),
    CLUSTER_FOR_JOB_RUN_WRITE_EP("clusterForJobRunWriteEP", "Write EP of cluster on which replication job runs", false),
    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Set to true if TDE encryption is enabled", false),
    HIVE_MIRRORING_JOB_NAME("hiveJobName", "Unique hive replication job name", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    HiveMirroringExtensionProperties(String name, String description) {
        this(name, description, true);
    }

    HiveMirroringExtensionProperties(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    @Override
    public String toString() {
        return getName();
    }
}


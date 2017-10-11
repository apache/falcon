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

package org.apache.falcon.hive;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Arguments for workflow execution.
 */
public enum HiveDRArgs {

    // source meta store details
    SOURCE_CLUSTER("sourceCluster", "source cluster"),
    SOURCE_METASTORE_URI("sourceMetastoreUri", "source meta store uri"),
    SOURCE_HS2_URI("sourceHiveServer2Uri", "source HS2 uri"),
    SOURCE_HS2_URI_EXTRA_OPTS("sourceHiveServer2ExtraOpts", "source HS2 extra opts", false),
    SOURCE_DATABASES("sourceDatabases", "comma source databases"),
    SOURCE_DATABASE("sourceDatabase", "First source database"),
    SOURCE_TABLES("sourceTables", "comma source tables"),
    SOURCE_STAGING_PATH("sourceStagingPath", "source staging path for data", false),

    // source hadoop endpoints
    SOURCE_NN("sourceNN", "source name node"),
    // source security kerberos principals
    SOURCE_NN_KERBEROS_PRINCIPAL("sourceNNKerberosPrincipal", "Source name node kerberos principal", false),
    SOURCE_HIVE_METASTORE_KERBEROS_PRINCIPAL("sourceHiveMetastoreKerberosPrincipal",
            "Source hive metastore kerberos principal", false),
    SOURCE_HIVE2_KERBEROS_PRINCIPAL("sourceHive2KerberosPrincipal", "Source hiveserver2 kerberos principal", false),

    TARGET_CLUSTER("targetCluster", "target cluster"),
    // target meta store details
    TARGET_METASTORE_URI("targetMetastoreUri", "source meta store uri"),
    TARGET_HS2_URI("targetHiveServer2Uri", "source meta store uri"),
    TARGET_HS2_URI_EXTRA_OPTS("targetHiveServer2ExtraOpts", "target HS2 extra opts", false),

    TARGET_STAGING_PATH("targetStagingPath", "source staging path for data", false),

    // target hadoop endpoints
    TARGET_NN("targetNN", "target name node"),
    // target security kerberos principals
    TARGET_NN_KERBEROS_PRINCIPAL("targetNNKerberosPrincipal", "Target name node kerberos principal", false),
    TARGET_HIVE_METASTORE_KERBEROS_PRINCIPAL("targetHiveMetastoreKerberosPrincipal",
            "Target hive metastore kerberos principal", false),
    TARGET_HIVE2_KERBEROS_PRINCIPAL("targetHive2KerberosPrincipal", "Target hiveserver2 kerberos principal", false),

    // num events
    MAX_EVENTS("maxEvents", "number of events to process in this run"),

    // tuning params
    REPLICATION_MAX_MAPS("replicationMaxMaps", "number of maps", false),
    DISTCP_MAX_MAPS("distcpMaxMaps", "number of maps", false),

    // Set to true if TDE is enabled
    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Set to true if TDE encryption is enabled", false),

    // Map Bandwidth
    DISTCP_MAP_BANDWIDTH("distcpMapBandwidth", "map bandwidth in mb", false),

    JOB_NAME("hiveJobName", "unique job name"),

    CLUSTER_FOR_JOB_RUN("clusterForJobRun", "cluster where job runs"),
    JOB_CLUSTER_NN("clusterForJobRunWriteEP", "write end point of cluster where job runs"),
    JOB_CLUSTER_NN_KERBEROS_PRINCIPAL("clusterForJobNNKerberosPrincipal",
            "Namenode kerberos principal of cluster on which replication job runs", false),

    KEEP_HISTORY("keepHistory", "Keep history of events file generated", false),
    EXECUTION_STAGE("executionStage", "Flag for workflow stage execution", false),
    COUNTER_LOGDIR("counterLogDir", "Log directory to store counter file", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    HiveDRArgs(String name, String description) {
        this(name, description, true);
    }

    HiveDRArgs(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public Option getOption() {
        return new Option(this.name, true, this.description);
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

    public String getOptionValue(CommandLine cmd) {
        return cmd.getOptionValue(this.name);
    }

    @Override
    public String toString() {
        return getName();
    }
}

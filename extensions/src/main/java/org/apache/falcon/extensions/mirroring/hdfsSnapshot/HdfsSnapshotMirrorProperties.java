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

package org.apache.falcon.extensions.mirroring.hdfsSnapshot;

/**
 * Hdfs Snapshot Extension properties.
 */
public enum HdfsSnapshotMirrorProperties {
    SOURCE_CLUSTER("sourceCluster", "Snapshot replication source cluster", true),
    SOURCE_NN("sourceNN", "Snapshot replication source cluster namenode", false),
    SOURCE_EXEC_URL("sourceExecUrl", "Snapshot replication source execute endpoint", false),
    SOURCE_NN_KERBEROS_PRINCIPAL("sourceNNKerberosPrincipal",
            "Snapshot replication source kerberos principal", false),

    SOURCE_SNAPSHOT_DIR("sourceSnapshotDir", "Location of source snapshot path", true),
    SOURCE_SNAPSHOT_RETENTION_POLICY("sourceSnapshotRetentionPolicy", "Retention policy for source snapshots", false),
    SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT("sourceSnapshotRetentionAgeLimit",
            "Delete source snapshots older than this age", true),
    SOURCE_SNAPSHOT_RETENTION_NUMBER("sourceSnapshotRetentionNumber",
            "Number of latest source snapshots to retain on source", true),

    TARGET_CLUSTER("targetCluster", "Snapshot replication target cluster", true),
    TARGET_NN("targetNN", "Snapshot replication target cluster namenode", false),
    TARGET_EXEC_URL("targetExecUrl", "Snapshot replication target execute endpoint", false),
    TARGET_NN_KERBEROS_PRINCIPAL("targetNNKerberosPrincipal",
            "Snapshot replication target kerberos principal", false),

    TARGET_SNAPSHOT_DIR("targetSnapshotDir", "Target Hive metastore uri", true),
    TARGET_SNAPSHOT_RETENTION_POLICY("targetSnapshotRetentionPolicy", "Retention policy for target snapshots", false),
    TARGET_SNAPSHOT_RETENTION_AGE_LIMIT("targetSnapshotRetentionAgeLimit",
            "Delete target snapshots older than this age", true),
    TARGET_SNAPSHOT_RETENTION_NUMBER("targetSnapshotRetentionNumber",
            "Number of latest target snapshots to retain on source", true),

    MAX_MAPS("maxMaps", "Maximum number of maps used during distcp", false),
    MAP_BANDWIDTH_IN_MB("mapBandwidth", "Bandwidth in MB/s used by each mapper during replication", false),

    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Is TDE encryption enabled on source and target", false),
    SNAPSHOT_JOB_NAME("snapshotJobName", "Name of snapshot based mirror job", false);


    private final String name;
    private final String description;
    private final boolean isRequired;

    HdfsSnapshotMirrorProperties(String name, String description, boolean isRequired) {
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

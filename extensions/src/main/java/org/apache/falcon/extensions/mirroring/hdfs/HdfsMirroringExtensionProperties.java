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

package org.apache.falcon.extensions.mirroring.hdfs;

/**
 * Hdfs Extension properties.
 */
public enum HdfsMirroringExtensionProperties {
    SOURCE_DIR("sourceDir", "Location of source data to replicate"),
    SOURCE_CLUSTER("sourceCluster", "Source cluster"),
    SOURCE_CLUSTER_FS_WRITE_ENDPOINT("sourceClusterFS", "Source cluster end point", false),
    TARGET_DIR("targetDir", "Location on target cluster for replication"),
    TARGET_CLUSTER("targetCluster", "Target cluster"),
    TARGET_CLUSTER_FS_WRITE_ENDPOINT("targetClusterFS", "Target cluster end point", false),
    DISTCP_MAX_MAPS("distcpMaxMaps", "Maximum number of maps used during replication", false),
    DISTCP_MAP_BANDWIDTH_IN_MB("distcpMapBandwidth", "Bandwidth in MB/s used by each mapper during replication",
            false),
    TDE_ENCRYPTION_ENABLED("tdeEncryptionEnabled", "Set to true if TDE encryption is enabled", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    HdfsMirroringExtensionProperties(String name, String description) {
        this(name, description, true);
    }

    HdfsMirroringExtensionProperties(String name, String description, boolean isRequired) {
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

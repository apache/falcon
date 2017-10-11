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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.extensions.AbstractExtension;
import org.apache.falcon.extensions.ExtensionProperties;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.SecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Hdfs snapshot mirroring extension.
 */
public class HdfsSnapshotMirroringExtension extends AbstractExtension {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsSnapshotMirroringExtension.class);
    private static final String EXTENSION_NAME = "HDFS-SNAPSHOT-MIRRORING";
    private static final String DEFAULT_RETENTION_POLICY = "delete";
    public static final String EMPTY_KERBEROS_PRINCIPAL = "NA";

    @Override
    public String getName() {
        return EXTENSION_NAME;
    }

    @Override
    public void validate(final Properties extensionProperties) throws FalconException {
        for (HdfsSnapshotMirrorProperties option : HdfsSnapshotMirrorProperties.values()) {
            if (extensionProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new FalconException("Missing extension property: " + option.getName());
            }
        }

        Cluster sourceCluster = ClusterHelper.getCluster(extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.SOURCE_CLUSTER.getName()));
        if (sourceCluster == null) {
            throw new FalconException("SourceCluster entity "
                    + HdfsSnapshotMirrorProperties.SOURCE_CLUSTER.getName() + " not found");
        }
        Cluster targetCluster = ClusterHelper.getCluster(extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.TARGET_CLUSTER.getName()));
        if (targetCluster == null) {
            throw new FalconException("TargetCluster entity "
                    + HdfsSnapshotMirrorProperties.TARGET_CLUSTER.getName() + " not found");
        }

        Configuration sourceConf = ClusterHelper.getConfiguration(sourceCluster);
        Configuration targetConf = ClusterHelper.getConfiguration(targetCluster);
        DistributedFileSystem sourceFileSystem =
                HadoopClientFactory.get().createDistributedProxiedFileSystem(sourceConf);
        DistributedFileSystem targetFileSystem =
                HadoopClientFactory.get().createDistributedProxiedFileSystem(targetConf);

        Path sourcePath = new Path(extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName()));
        Path targetPath = new Path(extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName()));

        // check if source and target path's exist and are snapshot-able
        try {
            if (sourceFileSystem.exists(sourcePath)) {
                if (!isDirSnapshotable(sourceFileSystem, sourcePath)) {
                    throw new FalconException(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName()
                            + " " + sourcePath.toString() + " does not allow snapshots.");
                }
            } else {
                throw new FalconException(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName()
                        + " " + sourcePath.toString() + " does not exist.");
            }
            if (targetFileSystem.exists(targetPath)) {
                if (!isDirSnapshotable(targetFileSystem, targetPath)) {
                    throw new FalconException(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName()
                            + " " + targetPath.toString() + " does not allow snapshots.");
                }
            } else {
                throw new FalconException(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName()
                        + " " + targetPath.toString() + " does not exist.");
            }
        } catch (IOException e) {
            throw new FalconException(e.getMessage(), e);
        }


    }

    private static boolean isDirSnapshotable(DistributedFileSystem hdfs, Path path) throws FalconException {
        try {
            LOG.debug("HDFS Snapshot extension validating if dir {} is snapshotable.", path.toString());
            SnapshottableDirectoryStatus[] snapshotableDirs = hdfs.getSnapshottableDirListing();
            if (snapshotableDirs != null && snapshotableDirs.length > 0) {
                for (SnapshottableDirectoryStatus dir : snapshotableDirs) {
                    if (dir.getFullPath().toString().equals(path.toString())) {
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            LOG.error("Unable to verify if dir {} is snapshot-able. {}", path.toString(), e.getMessage());
            throw new FalconException("Unable to verify if dir " + path.toString() + " is snapshot-able", e);
        }
    }

    @Override
    public Properties getAdditionalProperties(final Properties extensionProperties) throws FalconException {
        Properties additionalProperties = new Properties();

        // Add default properties if not passed
        String distcpMaxMaps = extensionProperties.getProperty(HdfsSnapshotMirrorProperties.MAX_MAPS.getName());
        if (StringUtils.isBlank(distcpMaxMaps)) {
            additionalProperties.put(HdfsSnapshotMirrorProperties.MAX_MAPS.getName(), "1");
        }

        String distcpMapBandwidth = extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.MAP_BANDWIDTH_IN_MB.getName());
        if (StringUtils.isBlank(distcpMapBandwidth)) {
            additionalProperties.put(HdfsSnapshotMirrorProperties.MAP_BANDWIDTH_IN_MB.getName(), "100");
        }

        String tdeEnabled = extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.TDE_ENCRYPTION_ENABLED.getName());
        if (StringUtils.isNotBlank(tdeEnabled) && Boolean.parseBoolean(tdeEnabled)) {
            additionalProperties.put(HdfsSnapshotMirrorProperties.TDE_ENCRYPTION_ENABLED.getName(), "true");
        } else {
            additionalProperties.put(HdfsSnapshotMirrorProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        }

        // Add sourceCluster properties
        Cluster sourceCluster = ClusterHelper.getCluster(extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.SOURCE_CLUSTER.getName()));
        if (sourceCluster == null) {
            LOG.error("Cluster entity {} not found", HdfsSnapshotMirrorProperties.SOURCE_CLUSTER.getName());
            throw new FalconException("Cluster entity "
                    + HdfsSnapshotMirrorProperties.SOURCE_CLUSTER.getName() + " not found");
        }
        additionalProperties.put(HdfsSnapshotMirrorProperties.SOURCE_NN.getName(),
                ClusterHelper.getStorageUrl(sourceCluster));
        additionalProperties.put(HdfsSnapshotMirrorProperties.SOURCE_EXEC_URL.getName(),
                ClusterHelper.getMREndPoint(sourceCluster));
        String sourceKerberosPrincipal = ClusterHelper.getPropertyValue(sourceCluster, SecurityUtil.NN_PRINCIPAL);
        if (StringUtils.isBlank(sourceKerberosPrincipal)) {
            sourceKerberosPrincipal = EMPTY_KERBEROS_PRINCIPAL;
        }
        additionalProperties.put(HdfsSnapshotMirrorProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName(),
                sourceKerberosPrincipal);

        String sourceRetentionPolicy = extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_POLICY.getName());
        if (StringUtils.isBlank(sourceRetentionPolicy)) {
            sourceRetentionPolicy = DEFAULT_RETENTION_POLICY;
        }
        validateRetentionPolicy(sourceRetentionPolicy);
        additionalProperties.put(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_POLICY.getName(),
                sourceRetentionPolicy);

        // Add targetCluster properties
        Cluster targetCluster = ClusterHelper.getCluster(extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.TARGET_CLUSTER.getName()));
        if (targetCluster == null) {
            LOG.error("Cluster entity {} not found", HdfsSnapshotMirrorProperties.TARGET_CLUSTER.getName());
            throw new FalconException("Cluster entity "
                    + HdfsSnapshotMirrorProperties.TARGET_CLUSTER.getName() + " not found");
        }
        additionalProperties.put(HdfsSnapshotMirrorProperties.TARGET_NN.getName(),
                ClusterHelper.getStorageUrl(targetCluster));
        additionalProperties.put(HdfsSnapshotMirrorProperties.TARGET_EXEC_URL.getName(),
                ClusterHelper.getMREndPoint(targetCluster));
        String targetKerberosPrincipal = ClusterHelper.getPropertyValue(targetCluster, SecurityUtil.NN_PRINCIPAL);
        if (StringUtils.isBlank(targetKerberosPrincipal)) {
            targetKerberosPrincipal = EMPTY_KERBEROS_PRINCIPAL;
        }
        additionalProperties.put(HdfsSnapshotMirrorProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName(),
                targetKerberosPrincipal);

        String targetRetentionPolicy = extensionProperties.getProperty(
                HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_POLICY.getName());
        if (StringUtils.isBlank(targetRetentionPolicy)) {
            targetRetentionPolicy = DEFAULT_RETENTION_POLICY;
        }
        validateRetentionPolicy(targetRetentionPolicy);
        additionalProperties.put(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_POLICY.getName(),
                targetRetentionPolicy);

        // Add jobName and jobCluster properties.
        String jobName = extensionProperties.getProperty(ExtensionProperties.JOB_NAME.getName());
        if (StringUtils.isBlank(jobName)) {
            throw new FalconException("Property "
                    + ExtensionProperties.JOB_NAME.getName() + " cannot be null");
        }
        additionalProperties.put(HdfsSnapshotMirrorProperties.SNAPSHOT_JOB_NAME.getName(), jobName);

        String jobClusterName = extensionProperties.getProperty(ExtensionProperties.CLUSTER_NAME.getName());
        Cluster jobCluster = ClusterHelper.getCluster(jobClusterName);
        if (jobCluster == null) {
            LOG.error("Cluster entity {} not found", ExtensionProperties.CLUSTER_NAME.getName());
            throw new FalconException("Cluster entity "
                    + ExtensionProperties.CLUSTER_NAME.getName() + " not found");
        }

        addAdditionalDistCPProperties(extensionProperties, additionalProperties);
        return additionalProperties;
    }

    public static void validateRetentionPolicy(String retentionPolicy) throws FalconException {
        if (!retentionPolicy.equalsIgnoreCase("delete")) {
            throw new FalconException("Retention policy \"" + retentionPolicy + "\" is invalid");
        }
    }
}

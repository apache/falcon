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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.extensions.AbstractExtension;
import org.apache.falcon.util.FSDRUtils;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Hdfs mirroring extension.
 */
public class HdfsMirroringExtension extends AbstractExtension {

    private static final String COMMA_SEPARATOR = ",";
    private static final String EXTENSION_NAME = "HDFS-MIRRORING";

    @Override
    public String getName() {
        return EXTENSION_NAME;
    }

    @Override
    public void validate(final Properties extensionProperties) throws FalconException {
        for (HdfsMirroringExtensionProperties option : HdfsMirroringExtensionProperties.values()) {
            if (extensionProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new FalconException("Missing extension property: " + option.getName());
            }
        }

        String srcPaths = extensionProperties.getProperty(HdfsMirroringExtensionProperties
                .SOURCE_DIR.getName());
        if (!isHCFSPath(srcPaths)) {
            if (extensionProperties.getProperty(HdfsMirroringExtensionProperties.SOURCE_CLUSTER.getName()) == null) {
                throw new FalconException("Missing extension property: " + HdfsMirroringExtensionProperties.
                        SOURCE_CLUSTER.getName());
            }
        }
        String targetDir = extensionProperties.getProperty(HdfsMirroringExtensionProperties
                .TARGET_DIR.getName());

        if (!FSDRUtils.isHCFS(new Path(targetDir.trim()))) {
            if (extensionProperties.getProperty(HdfsMirroringExtensionProperties.TARGET_CLUSTER.getName()) == null) {
                throw new FalconException("Missing extension property: " + HdfsMirroringExtensionProperties.
                        TARGET_CLUSTER.getName());
            }
        }
    }

    @Override
    public Properties getAdditionalProperties(final Properties extensionProperties) throws FalconException {
        Properties additionalProperties = new Properties();
        boolean isHCFSDR = false;

        // Add default properties if not passed
        String distcpMaxMaps = extensionProperties.getProperty(
                HdfsMirroringExtensionProperties.DISTCP_MAX_MAPS.getName());
        if (StringUtils.isBlank(distcpMaxMaps)) {
            additionalProperties.put(HdfsMirroringExtensionProperties.DISTCP_MAX_MAPS.getName(), "1");
        }

        String distcpMapBandwidth = extensionProperties.getProperty(
                HdfsMirroringExtensionProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName());
        if (StringUtils.isBlank(distcpMapBandwidth)) {
            additionalProperties.put(HdfsMirroringExtensionProperties.DISTCP_MAP_BANDWIDTH_IN_MB.getName(), "100");
        }

        String srcPaths = extensionProperties.getProperty(HdfsMirroringExtensionProperties
                .SOURCE_DIR.getName());
        String sourceClusterFS = "";
        if (isHCFSPath(srcPaths)) {
            // Make sure path is fully qualified
            // For HCFS only one path
            URI pathUri = new Path(srcPaths).toUri();
            if (pathUri.getAuthority() == null) {
                throw new FalconException("getAdditionalProperties: " + srcPaths + " is not fully qualified path");
            }
            isHCFSDR = true;
        } else {
            StringBuilder absoluteSrcPaths = new StringBuilder();
            String sourceClusterName = extensionProperties.getProperty(
                    HdfsMirroringExtensionProperties.SOURCE_CLUSTER.getName());

            // Since source cluster get read interface
            Cluster srcCluster = ClusterHelper.getCluster(sourceClusterName.trim());
            if (srcCluster == null) {
                throw new FalconException("Source Cluster entity " + sourceClusterName + " not found");
            }
            String srcClusterEndPoint = ClusterHelper.getReadOnlyStorageUrl(srcCluster);

            // Construct fully qualified hdfs src path
            String[] paths = srcPaths.split(COMMA_SEPARATOR);
            URI pathUri;
            for (String path : paths) {
                try {
                    pathUri = new URI(path.trim());
                } catch (URISyntaxException e) {
                    throw new FalconException(e);
                }
                String authority = pathUri.getAuthority();
                StringBuilder srcpath;
                if (authority == null) {
                    srcpath = new StringBuilder(srcClusterEndPoint);
                } else {
                    srcpath = new StringBuilder();
                }
                srcpath.append(path.trim());
                srcpath.append(COMMA_SEPARATOR);
                absoluteSrcPaths.append(srcpath);
            }
            additionalProperties.put(HdfsMirroringExtensionProperties.SOURCE_DIR.getName(),
                    StringUtils.removeEnd(absoluteSrcPaths.toString(), COMMA_SEPARATOR));
            sourceClusterFS = ClusterHelper.getReadOnlyStorageUrl(srcCluster);
        }


        String targetDir = extensionProperties.getProperty(HdfsMirroringExtensionProperties
                .TARGET_DIR.getName());
        String targetClusterFS = "";
        if (FSDRUtils.isHCFS(new Path(targetDir.trim()))) {
            // Make sure path is fully qualified
            URI pathUri = new Path(targetDir).toUri();
            if (pathUri.getAuthority() == null) {
                throw new FalconException("getAdditionalProperties: " + targetDir + " is not fully qualified path");
            }
            isHCFSDR = true;
        } else {
            String targetClusterName = extensionProperties.getProperty(
                    HdfsMirroringExtensionProperties.TARGET_CLUSTER.getName());

            Cluster targetCluster = ClusterHelper.getCluster(targetClusterName.trim());
            if (targetCluster == null) {
                throw new FalconException("Target Cluster entity " + targetClusterName + " not found");
            }

            targetClusterFS = ClusterHelper.getStorageUrl(targetCluster);

            // Construct fully qualified hdfs target path
            URI pathUri;
            try {
                pathUri = new URI(targetDir.trim());
            } catch (URISyntaxException e) {
                throw new FalconException(e);
            }

            StringBuilder targetPath;
            String authority = pathUri.getAuthority();
            if (authority == null) {
                targetPath = new StringBuilder(targetClusterFS);
            } else {
                targetPath = new StringBuilder();
            }
            targetPath.append(targetDir.trim());

            additionalProperties.put(HdfsMirroringExtensionProperties.TARGET_DIR.getName(),
                    targetPath.toString());
        }

        // Oozie doesn't take null or empty string for arg in the WF. For HCFS pass the source FS as its not used
        if (isHCFSDR) {
            if (StringUtils.isBlank(sourceClusterFS)) {
                sourceClusterFS = targetClusterFS;
            } else if (StringUtils.isBlank(targetClusterFS)) {
                targetClusterFS = sourceClusterFS;
            }
        }
        // Add sourceClusterFS
        additionalProperties.put(HdfsMirroringExtensionProperties.SOURCE_CLUSTER_FS_READ_ENDPOINT.getName(),
                sourceClusterFS);
        // Add targetClusterFS
        additionalProperties.put(HdfsMirroringExtensionProperties.TARGET_CLUSTER_FS_WRITE_ENDPOINT.getName(),
                targetClusterFS);

        if (StringUtils.isBlank(
                extensionProperties.getProperty(HdfsMirroringExtensionProperties.TDE_ENCRYPTION_ENABLED.getName()))) {
            additionalProperties.put(HdfsMirroringExtensionProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        }

        addAdditionalDistCPProperties(extensionProperties, additionalProperties);
        return additionalProperties;
    }

    private static boolean isHCFSPath(String srcPaths) throws FalconException {
        if (StringUtils.isNotBlank(srcPaths)) {
            String[] paths = srcPaths.split(COMMA_SEPARATOR);

            // We expect all paths to be of same type, hence verify the first path
            for (String path : paths) {
                return FSDRUtils.isHCFS(new Path(path.trim()));
            }
        }

        return false;
    }
}

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
    }

    @Override
    public Properties getAdditionalProperties(final Properties extensionProperties) throws FalconException {
        Properties additionalProperties = new Properties();

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

        // Construct fully qualified hdfs src path
        String srcPaths = extensionProperties.getProperty(HdfsMirroringExtensionProperties
                .SOURCE_DIR.getName());
        StringBuilder absoluteSrcPaths = new StringBuilder();
        String sourceClusterName = extensionProperties.getProperty(
                HdfsMirroringExtensionProperties.SOURCE_CLUSTER.getName());

        // Since source cluster get read interface
        Cluster srcCluster = ClusterHelper.getCluster(sourceClusterName);
        if (srcCluster == null) {
            throw new FalconException("Cluster entity " + sourceClusterName + " not found");
        }
        String srcClusterEndPoint = ClusterHelper.getReadOnlyStorageUrl(srcCluster);

        if (StringUtils.isNotBlank(srcPaths)) {
            String[] paths = srcPaths.split(COMMA_SEPARATOR);

            for (String path : paths) {
                StringBuilder srcpath = new StringBuilder(srcClusterEndPoint);
                srcpath.append(path.trim());
                srcpath.append(COMMA_SEPARATOR);
                absoluteSrcPaths.append(srcpath);
            }
        }

        additionalProperties.put(HdfsMirroringExtensionProperties.SOURCE_DIR.getName(),
                StringUtils.removeEnd(absoluteSrcPaths.toString(), COMMA_SEPARATOR));

        // add sourceClusterFS and targetClusterFS
        additionalProperties.put(HdfsMirroringExtensionProperties.SOURCE_CLUSTER_FS_WRITE_ENDPOINT.getName(),
                ClusterHelper.getStorageUrl(srcCluster));

        String targetClusterName = extensionProperties.getProperty(
                HdfsMirroringExtensionProperties.TARGET_CLUSTER.getName());

        Cluster targetCluster = ClusterHelper.getCluster(targetClusterName);
        if (targetCluster == null) {
            throw new FalconException("Cluster entity " + targetClusterName + " not found");
        }
        additionalProperties.put(HdfsMirroringExtensionProperties.TARGET_CLUSTER_FS_WRITE_ENDPOINT.getName(),
                ClusterHelper.getStorageUrl(targetCluster));

        if (StringUtils.isBlank(
                extensionProperties.getProperty(HdfsMirroringExtensionProperties.TDE_ENCRYPTION_ENABLED.getName()))) {
            additionalProperties.put(HdfsMirroringExtensionProperties.TDE_ENCRYPTION_ENABLED.getName(), "false");
        }

        addAdditionalDistCPProperties(extensionProperties, additionalProperties);
        return additionalProperties;
    }

}

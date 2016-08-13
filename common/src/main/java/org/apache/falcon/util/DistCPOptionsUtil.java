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

package org.apache.falcon.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * Utility to set DistCp options.
 */
public final class DistCPOptionsUtil {
    private static final String TDE_ENCRYPTION_ENABLED = "tdeEncryptionEnabled";

    private DistCPOptionsUtil() {}

    public static DistCpOptions getDistCpOptions(CommandLine cmd,
                                                 List<Path> sourcePaths,
                                                 Path targetPath,
                                                 boolean isSnapshot,
                                                 Configuration conf) throws FalconException, IOException {
        DistCpOptions distcpOptions = new DistCpOptions(sourcePaths, targetPath);
        distcpOptions.setBlocking(true);

        distcpOptions.setMaxMaps(Integer.parseInt(cmd.getOptionValue("maxMaps")));
        distcpOptions.setMapBandwidth(Integer.parseInt(cmd.getOptionValue("mapBandwidth")));

        String tdeEncryptionEnabled = cmd.getOptionValue(TDE_ENCRYPTION_ENABLED);
        if (StringUtils.isNotBlank(tdeEncryptionEnabled)
                && tdeEncryptionEnabled.equalsIgnoreCase(Boolean.TRUE.toString())) {
            distcpOptions.setSyncFolder(true);
            distcpOptions.setSkipCRC(true);
        } else {
            if (!isSnapshot) {
                String overwrite = cmd.getOptionValue(ReplicationDistCpOption.DISTCP_OPTION_OVERWRITE.getName());
                if (StringUtils.isNotEmpty(overwrite) && overwrite.equalsIgnoreCase(Boolean.TRUE.toString())) {
                    distcpOptions.setOverwrite(Boolean.parseBoolean(overwrite));
                } else {
                    distcpOptions.setSyncFolder(true);
                }
            }

            String skipChecksum = cmd.getOptionValue(ReplicationDistCpOption.DISTCP_OPTION_SKIP_CHECKSUM.getName());
            if (StringUtils.isNotEmpty(skipChecksum)) {
                distcpOptions.setSkipCRC(Boolean.parseBoolean(skipChecksum));
            }
        }

        if (isSnapshot) {
            // Settings needed for Snapshot distCp.
            distcpOptions.setSyncFolder(true);
            distcpOptions.setDeleteMissing(true);
        } else {
            // Removing deleted files by default - FALCON-1844
            String removeDeletedFiles = cmd.getOptionValue(
                    ReplicationDistCpOption.DISTCP_OPTION_REMOVE_DELETED_FILES.getName(), "true");
            boolean deleteMissing = Boolean.parseBoolean(removeDeletedFiles);
            distcpOptions.setDeleteMissing(deleteMissing);
            if (deleteMissing) {
                // DistCP will fail with InvalidInputException if deleteMissing is set to true and
                // if targetPath does not exist. Create targetPath to avoid failures.
                FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(targetPath.toUri(), conf);
                if (!fs.exists(targetPath)) {
                    fs.mkdirs(targetPath);
                }
            }
        }

        String ignoreErrors = cmd.getOptionValue(ReplicationDistCpOption.DISTCP_OPTION_IGNORE_ERRORS.getName());
        if (StringUtils.isNotBlank(ignoreErrors)) {
            distcpOptions.setIgnoreFailures(Boolean.parseBoolean(ignoreErrors));
        }

        String preserveBlockSize = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getName());
        if (StringUtils.isNotBlank(preserveBlockSize) && Boolean.parseBoolean(preserveBlockSize)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
        }

        String preserveReplicationCount = cmd.getOptionValue(ReplicationDistCpOption
                .DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER.getName());
        if (StringUtils.isNotBlank(preserveReplicationCount) && Boolean.parseBoolean(preserveReplicationCount)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.REPLICATION);
        }

        String preservePermission = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_PERMISSIONS.getName());
        if (StringUtils.isNotBlank(preservePermission) && Boolean.parseBoolean(preservePermission)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.PERMISSION);
        }

        String preserveUser = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_USER.getName());
        if (StringUtils.isNotBlank(preserveUser) && Boolean.parseBoolean(preserveUser)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.USER);
        }

        String preserveGroup = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_GROUP.getName());
        if (StringUtils.isNotBlank(preserveGroup) && Boolean.parseBoolean(preserveGroup)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.GROUP);
        }

        String preserveChecksumType = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE.getName());
        if (StringUtils.isNotBlank(preserveChecksumType) && Boolean.parseBoolean(preserveChecksumType)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE);
        }

        String preserveAcl = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName());
        if (StringUtils.isNotBlank(preserveAcl) && Boolean.parseBoolean(preserveAcl)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.ACL);
        }

        String preserveXattr = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName());
        if (StringUtils.isNotBlank(preserveXattr) && Boolean.parseBoolean(preserveXattr)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.XATTR);
        }

        String preserveTimes = cmd.getOptionValue(
                ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_TIMES.getName());
        if (StringUtils.isNotBlank(preserveTimes) && Boolean.parseBoolean(preserveTimes)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.TIMES);
        }

        return distcpOptions;
    }
}

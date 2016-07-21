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

package org.apache.falcon.snapshots.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.extensions.mirroring.hdfsSnapshot.HdfsSnapshotMirrorProperties;
import org.apache.falcon.extensions.mirroring.hdfsSnapshot.HdfsSnapshotMirroringExtension;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Util class for HDFS snapshot based mirroring.
 */
public final class HdfsSnapshotUtil {

    public static final String SNAPSHOT_PREFIX = "falcon-snapshot-";
    public static final String SNAPSHOT_DIR_PREFIX = ".snapshot";

    private HdfsSnapshotUtil() {}

    public static DistributedFileSystem getSourceFileSystem(CommandLine cmd,
                                                            Configuration conf) throws FalconException {
        String sourceStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_NN.getName());
        String sourceExecuteEndpoint = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_EXEC_URL.getName());
        String sourcePrincipal = parseKerberosPrincipal(cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName()));

        Configuration sourceConf = ClusterHelper.getConfiguration(conf, sourceStorageUrl,
                sourceExecuteEndpoint, sourcePrincipal);
        return HadoopClientFactory.get().createDistributedProxiedFileSystem(sourceConf);
    }

    public static DistributedFileSystem getTargetFileSystem(CommandLine cmd,
                                                            Configuration conf) throws FalconException {
        String targetStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_NN.getName());
        String taregtExecuteEndpoint = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_EXEC_URL.getName());
        String targetPrincipal = parseKerberosPrincipal(cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName()));

        Configuration targetConf = ClusterHelper.getConfiguration(conf, targetStorageUrl,
                taregtExecuteEndpoint, targetPrincipal);
        return HadoopClientFactory.get().createDistributedProxiedFileSystem(targetConf);
    }

    public static String parseKerberosPrincipal(String principal) {
        if (StringUtils.isEmpty(principal) ||
                principal.equals(HdfsSnapshotMirroringExtension.EMPTY_KERBEROS_PRINCIPAL)) {
            return null;
        }
        return principal;
    }
}

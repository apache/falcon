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
package org.apache.falcon.replication;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.Storage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test class for FeedReplicator.
 */
public class FeedReplicatorTest {

    private String defaultPath = "jail://FeedReplicatorTest:00/tmp";

    @Test
    public void testArguments() throws Exception {
        /*
         * <arg>-update</arg>
         * <arg>-blocking</arg><arg>true</arg>
         * <arg>-maxMaps</arg><arg>3</arg>
         * <arg>-mapBandwidth</arg><arg>4</arg>
         * <arg>-sourcePaths</arg><arg>${distcpSourcePaths}</arg>
         * <arg>-targetPath</arg><arg>${distcpTargetPaths}</arg>
         */

        // creates jailed cluster in which DistCpOtions command can be tested.
        EmbeddedCluster cluster =  EmbeddedCluster.newCluster("FeedReplicatorTest");

        final String[] args = {
            "true",
            "-maxMaps", "3",
            "-mapBandwidth", "4",
            "-sourcePaths", defaultPath,
            "-targetPath", defaultPath,
            "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
        };

        FeedReplicator replicator = new FeedReplicator();
        CommandLine cmd = replicator.getCommand(args);
        replicator.setConf(cluster.getConf());
        DistCpOptions options = replicator.getDistCpOptions(cmd, false);

        List<Path> srcPaths = new ArrayList<Path>();
        srcPaths.add(new Path(defaultPath));
        validateMandatoryArguments(options, srcPaths, true);
        Assert.assertTrue(options.shouldDeleteMissing());
    }

    @Test
    public void testOptionalArguments() throws Exception {
        /*
         * <arg>-update</arg>
         * <arg>-blocking</arg><arg>true</arg>
         * <arg>-maxMaps</arg><arg>3</arg>
         * <arg>-mapBandwidthKB</arg><arg>4</arg>
         * <arg>-sourcePaths</arg><arg>${distcpSourcePaths}</arg>
         * <arg>-targetPath</arg><arg>${distcpTargetPaths}</arg>
         * <arg>-overwrite</arg><arg>true</arg>
         * <arg>-ignoreErrors</arg><arg>false</arg>
         * <arg>-skipChecksum</arg><arg>false</arg>
         * <arg>-removeDeletedFiles</arg><arg>true</arg>
         * <arg>-preserveBlockSize</arg><arg>false</arg>
         * <arg>-preserveReplicationCount</arg><arg>true</arg>
         * <arg>-preservePermission</arg><arg>false</arg>
         * <arg>-preserveUser</arg><arg>true</arg>
         * <arg>-preserveGroup</arg><arg>false</arg>
         * <arg>-preserveChecksumType</arg><arg>false</arg>
         * <arg>-preserveAcl</arg><arg>true</arg>
         * <arg>-preserveXattr</arg><arg>false</arg>
         * <arg>-preserveTimes</arg><arg>false</arg>
         */
        final String[] optionalArgs = {
            "true",
            "-maxMaps", "3",
            "-mapBandwidth", "4",
            "-sourcePaths", defaultPath,
            "-targetPath", defaultPath,
            "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
            "-overwrite", "true",
            "-ignoreErrors", "false",
            "-skipChecksum", "false",
            "-removeDeletedFiles", "false",
            "-preserveBlockSize", "false",
            "-preserveReplicationNumber", "true",
            "-preservePermission", "false",
            "-preserveUser", "true",
            "-preserveGroup", "false",
            "-preserveChecksumType", "false",
            "-preserveAcl", "true",
            "-preserveXattr", "false",
            "-preserveTimes", "false",
        };

        FeedReplicator replicator = new FeedReplicator();
        CommandLine cmd = replicator.getCommand(optionalArgs);
        DistCpOptions options = replicator.getDistCpOptions(cmd, false);

        List<Path> srcPaths = new ArrayList<Path>();
        srcPaths.add(new Path(defaultPath));
        validateMandatoryArguments(options, srcPaths, false);
        validateOptionalArguments(options);
    }

    @Test
    public void testIncludePath() throws Exception {
        // Set the include Path so that CustomReplicator is used and the source and targetPaths are modified.
        String includePath = defaultPath + "/test-colo";
        // creates jailed cluster in which DistCpOtions command can be tested.
        EmbeddedCluster cluster = EmbeddedCluster.newCluster("FeedReplicatorTest");

        final String[] args = {
            "true",
            "-maxMaps", "3",
            "-mapBandwidth", "4",
            "-sourcePaths", defaultPath,
            "-targetPath", defaultPath,
            "-falconFeedStorageType", Storage.TYPE.FILESYSTEM.name(),
        };

        FeedReplicator replicator = new FeedReplicator();
        CommandLine cmd = replicator.getCommand(args);
        Configuration conf = cluster.getConf();
        conf.set("falcon.include.path", includePath);
        replicator.setConf(conf);
        DistCpOptions options = replicator.getDistCpOptions(cmd, true);
        Assert.assertEquals(options.getTargetPath().toString(), includePath);
        Assert.assertEquals(options.getSourcePaths().get(0).toString(), includePath);
    }

    private void validateMandatoryArguments(DistCpOptions options, List<Path> srcPaths, boolean shouldSyncFolder) {
        Assert.assertEquals(options.getMaxMaps(), 3);
        Assert.assertEquals(options.getMapBandwidth(), 4);
        Assert.assertEquals(options.getSourcePaths(), srcPaths);
        Assert.assertEquals(options.getTargetPath(), new Path(defaultPath));
        Assert.assertEquals(options.shouldSyncFolder(), shouldSyncFolder);
    }

    private void validateOptionalArguments(DistCpOptions options) {
        Assert.assertTrue(options.shouldOverwrite());
        Assert.assertFalse(options.shouldIgnoreFailures());
        Assert.assertFalse(options.shouldSkipCRC());
        Assert.assertFalse(options.shouldDeleteMissing());
        Assert.assertFalse(options.shouldPreserve(DistCpOptions.FileAttribute.BLOCKSIZE));
        Assert.assertTrue(options.shouldPreserve(DistCpOptions.FileAttribute.REPLICATION));
        Assert.assertFalse(options.shouldPreserve(DistCpOptions.FileAttribute.PERMISSION));
        Assert.assertTrue(options.shouldPreserve(DistCpOptions.FileAttribute.USER));
        Assert.assertFalse(options.shouldPreserve(DistCpOptions.FileAttribute.GROUP));
        Assert.assertFalse(options.shouldPreserve(DistCpOptions.FileAttribute.CHECKSUMTYPE));
        Assert.assertTrue(options.shouldPreserve(DistCpOptions.FileAttribute.ACL));
        Assert.assertFalse(options.shouldPreserve(DistCpOptions.FileAttribute.XATTR));
        Assert.assertFalse(options.shouldPreserve(DistCpOptions.FileAttribute.TIMES));
    }
}

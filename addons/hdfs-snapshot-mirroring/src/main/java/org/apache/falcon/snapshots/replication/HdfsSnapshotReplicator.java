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

package org.apache.falcon.snapshots.replication;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.extensions.mirroring.hdfsSnapshot.HdfsSnapshotMirrorProperties;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.snapshots.util.HdfsSnapshotUtil;
import org.apache.falcon.util.DistCPOptionsUtil;
import org.apache.falcon.util.ReplicationDistCpOption;
import org.apache.falcon.workflow.util.OozieActionConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * HDFS snapshot generator and snapshot based replicator.
 */
public class HdfsSnapshotReplicator extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsSnapshotReplicator.class);
    protected CommandLine cmd;

    public static void main(String[] args) throws Exception {
        Configuration conf = OozieActionConfigurationHelper.createActionConf();
        int ret = ToolRunner.run(conf, new HdfsSnapshotReplicator(), args);
        if (ret != 0) {
            throw new Exception("Unable to perform Snapshot based replication action args: " + Arrays.toString(args));
        }
    }

    @Override
    public int run(String[] args) throws FalconException {
        cmd = getCommand(args);

        String sourceStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_NN.getName());
        String targetStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_NN.getName());

        // Always add to getConf() so that configuration set by oozie action is
        // available when creating DistributedFileSystem.
        DistributedFileSystem sourceFs = HdfsSnapshotUtil.getSourceFileSystem(cmd,
                new Configuration(getConf()));
        DistributedFileSystem targetFs = HdfsSnapshotUtil.getTargetFileSystem(cmd,
                new Configuration(getConf()));

        String currentSnapshotName = HdfsSnapshotUtil.SNAPSHOT_PREFIX
                + cmd.getOptionValue(HdfsSnapshotMirrorProperties.SNAPSHOT_JOB_NAME.getName())
                + "-" + System.currentTimeMillis();
        String sourceDir = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName());
        String targetDir = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName());

        // Generate snapshot on source.
        createSnapshotInFileSystem(sourceDir, currentSnapshotName, sourceFs);

        // Find most recently recplicated snapshot. If it exists, distCp using the snapshots.
        // If not, do regular distcp as this is the first time job is running.
        invokeCopy(sourceStorageUrl, targetStorageUrl, sourceFs, targetFs,
                sourceDir, targetDir, currentSnapshotName);

        // Generate snapshot on target if distCp succeeds.
        createSnapshotInFileSystem(targetDir, currentSnapshotName, targetFs);

        LOG.info("Completed HDFS Snapshot Replication.");
        return 0;
    }

    private static void createSnapshotInFileSystem(String dirName, String snapshotName,
                                                   FileSystem fs) throws FalconException {
        try {
            LOG.info("Creating snapshot {} in directory {}", snapshotName, dirName);
            fs.createSnapshot(new Path(dirName), snapshotName);
        } catch (IOException e) {
            LOG.warn("Unable to create snapshot {} in filesystem {}. Exception is {}",
                    snapshotName, fs.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY), e.getMessage());
            throw new FalconException("Unable to create snapshot " + snapshotName, e);
        }
    }

    protected void invokeCopy(String sourceStorageUrl, String targetStorageUrl,
                              DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                              String sourceDir, String targetDir,
                              String currentSnapshotName) throws FalconException {
        try {
            Configuration jobConf = this.getConf();
            DistCpOptions options = getDistCpOptions(sourceStorageUrl, targetStorageUrl,
                    sourceFs, targetFs, sourceDir, targetDir, currentSnapshotName);
            DistCp distCp = new DistCp(jobConf, options);
            LOG.info("Started Snapshot based DistCp from {} to {} ", getStagingUri(sourceStorageUrl, sourceDir),
                    getStagingUri(targetStorageUrl, targetDir));
            Job distcpJob = distCp.execute();
            LOG.info("Distp Hadoop job: {}", distcpJob.getJobID().toString());
            LOG.info("Completed Snapshot based DistCp");

        } catch (FalconException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FalconException("Unable to replicate HDFS directory using snapshots.", e);
        }
    }

    private DistCpOptions getDistCpOptions(String sourceStorageUrl, String targetStorageUrl,
                                           DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                           String sourceDir, String targetDir,
                                           String currentSnapshotName) throws FalconException, IOException {

        List<Path> sourceUris = new ArrayList<>();
        sourceUris.add(new Path(getStagingUri(sourceStorageUrl, sourceDir)));


        DistCpOptions distcpOptions = DistCPOptionsUtil.getDistCpOptions(cmd, sourceUris,
                new Path(getStagingUri(targetStorageUrl, targetDir)), true, null);

        // Use snapshot diff if two snapshots exist. Else treat it as simple distCp.
        // get latest replicated snapshot.
        String replicatedSnapshotName = findLatestReplicatedSnapshot(sourceFs, targetFs, sourceDir, targetDir);
        if (StringUtils.isNotBlank(replicatedSnapshotName)) {
            distcpOptions.setUseDiff(true, replicatedSnapshotName, currentSnapshotName);
        }

        return distcpOptions;
    }

    private String findLatestReplicatedSnapshot(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                                                String sourceDir, String targetDir) throws FalconException {
        try {
            FileStatus[] sourceSnapshots = sourceFs.listStatus(new Path(getSnapshotDir(sourceDir)));
            Set<String> sourceSnapshotNames = new HashSet<>();
            for (FileStatus snapshot : sourceSnapshots) {
                sourceSnapshotNames.add(snapshot.getPath().getName());
            }

            FileStatus[] targetSnapshots = targetFs.listStatus(new Path(getSnapshotDir(targetDir)));
            if (targetSnapshots.length > 0) {
                //sort target snapshots in desc order of creation time.
                Arrays.sort(targetSnapshots, new Comparator<FileStatus>() {
                    @Override
                    public int compare(FileStatus f1, FileStatus f2) {
                        return Long.compare(f2.getModificationTime(), f1.getModificationTime());
                    }
                });

                // get most recent snapshot name that exists in source.
                for (FileStatus targetSnapshot : targetSnapshots) {
                    String name = targetSnapshot.getPath().getName();
                    if (sourceSnapshotNames.contains(name)) {
                        return name;
                    }
                }
                // If control reaches here,
                // there are snapshots on target, but none are replicated from source. Return null.
            } // No target snapshots, return null
            return null;
        } catch (IOException e) {
            LOG.error("Unable to find latest snapshot on targetDir {} {}", targetDir, e.getMessage());
            throw new FalconException("Unable to find latest snapshot on targetDir " + targetDir, e);
        }
    }

    private String getStagingUri(String storageUrl, String dir) {
        storageUrl = StringUtils.removeEnd(storageUrl, Path.SEPARATOR);
        return storageUrl + Path.SEPARATOR + dir;
    }

    private String getSnapshotDir(String dirName) {
        dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
        return dirName + Path.SEPARATOR + HdfsSnapshotUtil.SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
    }

    protected CommandLine getCommand(String[] args) throws FalconException {
        Options options = new Options();

        Option opt = new Option(HdfsSnapshotMirrorProperties.MAX_MAPS.getName(),
                true, "max number of maps to use for distcp");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.MAP_BANDWIDTH_IN_MB.getName(),
                true, "Bandwidth in MB/s used by each mapper during replication");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_NN.getName(), true, "Source NN");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_EXEC_URL.getName(),
                true, "Replication instance job Exec Url");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName(),
                true, "Replication instance job NN Kerberos Principal");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName(),
                true, "Source snapshot-able dir to replicate");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_NN.getName(), true, "Target NN");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_EXEC_URL.getName(),
                true, "Replication instance target Exec Url");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName(),
                true, "Replication instance target NN Kerberos Principal");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName(),
                true, "Target snapshot-able dir to replicate");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HdfsSnapshotMirrorProperties.TDE_ENCRYPTION_ENABLED.getName(),
                true, "Is TDE encryption enabled on dirs being replicated?");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(HdfsSnapshotMirrorProperties.SNAPSHOT_JOB_NAME.getName(),
                true, "Replication instance job name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_OVERWRITE.getName(), true, "option to force overwrite");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_IGNORE_ERRORS.getName(), true, "abort on error");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_SKIP_CHECKSUM.getName(), true, "skip checksums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_REMOVE_DELETED_FILES.getName(), true,
                "remove deleted files - should there be files in the target directory that"
                        + "were removed from the source directory"
        );
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getName(), true,
                "preserve block size");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER.getName(), true,
                "preserve replication count");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_PERMISSIONS.getName(), true,
                "preserve permissions");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_USER.getName(), true,
                "preserve user");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_GROUP.getName(), true,
                "preserve group");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE.getName(), true,
                "preserve checksum type");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_ACL.getName(), true,
                "preserve ACL");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_XATTR.getName(), true,
                "preserve XATTR");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ReplicationDistCpOption.DISTCP_OPTION_PRESERVE_TIMES.getName(), true,
                "preserve access and modification times");
        opt.setRequired(false);
        options.addOption(opt);

        try {
            return new GnuParser().parse(options, args);
        } catch (ParseException pe) {
            LOG.info("Unabel to parse commad line arguments for HdfsSnapshotReplicator " + pe.getMessage());
            throw new FalconException(pe.getMessage());
        }
    }

}

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

package org.apache.falcon.replication.snapshots;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.extensions.mirroring.hdfsSnapshot.HdfsSnapshotMirrorProperties;
import org.apache.falcon.hadoop.HadoopClientFactory;
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
        ToolRunner.run(new Configuration(), new HdfsSnapshotReplicator(), args);
    }

    @Override
    public int run(String[] args) throws FalconException {
        cmd = getCommand(args);

        String sourceStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_NN.getName());
        String sourceExecuteEndpoint = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_EXEC_URL.getName());
        String sourcePrincipal = cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.SOURCE_NN_KERBEROS_PRINCIPAL.getName());

        String targetStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_NN.getName());
        String taregtExecuteEndpoint = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_EXEC_URL.getName());
        String targetPrincipal = cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.TARGET_NN_KERBEROS_PRINCIPAL.getName());

        String jobStorageUrl = cmd.getOptionValue(HdfsSnapshotMirrorProperties.JOB_NN.getName());
        String jobExecuteEndpoint = cmd.getOptionValue(HdfsSnapshotMirrorProperties.JOB_EXEC_URL.getName());
        String jobPrincipal = cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.JOB_NN_KERBEROS_PRINCIPAL.getName());


        Configuration sourceConf = ClusterHelper.getConfiguration(sourceStorageUrl,
                sourceExecuteEndpoint, sourcePrincipal);
        Configuration targetConf = ClusterHelper.getConfiguration(targetStorageUrl,
                taregtExecuteEndpoint, targetPrincipal);
        Configuration jobConf = ClusterHelper.getConfiguration(jobStorageUrl, jobExecuteEndpoint, jobPrincipal);

        DistributedFileSystem sourceFs = HadoopClientFactory.get().createDistributedProxiedFileSystem(sourceConf);
        DistributedFileSystem targetFs = HadoopClientFactory.get().createDistributedProxiedFileSystem(targetConf);

        String currentSnapshotName = "falcon-snapshot-"
                + cmd.getOptionValue(HdfsSnapshotMirrorProperties.SNAPSHOT_JOB_NAME.getName())
                + "-" + System.currentTimeMillis();
        String sourceDir = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName());
        String targetDir = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName());

        // Generate snapshot on source.
        createSnapshotInFileSystem(sourceDir, currentSnapshotName, sourceFs);

        // Find most recently recplicated snapshot. If it exists, distCp using the snapshots.
        // If not, do regular distcp as this is the first time job is running.
        invokeCopy(sourceStorageUrl, targetStorageUrl, sourceFs, targetFs,
                sourceDir, targetDir, jobConf, currentSnapshotName);

        // Generate snapshot on target if distCp succeeds.
        createSnapshotInFileSystem(targetDir, currentSnapshotName, targetFs);

        LOG.info("Completed HDFS Snapshot Replication.");
        return 0;
    }

    private void createSnapshotInFileSystem(String dirName, String snapshotName,
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

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    protected void invokeCopy(String sourceStorageUrl, String targetStorageUrl,
                              DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
                              String sourceDir, String targetDir,
                              Configuration jobConf, String currentSnapshotName) throws FalconException {
        try {
            DistCpOptions options = getDistCpOptions(sourceStorageUrl, targetStorageUrl,
                    sourceFs, targetFs, sourceDir, targetDir, currentSnapshotName);
            DistCp distCp = new DistCp(jobConf, options);
            LOG.info("Started Snapshot based DistCp from " + getStagingUri(sourceStorageUrl, sourceDir)
                    + " to " + getStagingUri(targetStorageUrl, targetDir));
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
                                           String currentSnapshotName) throws FalconException {

        List<Path> sourceUris=new ArrayList<Path>();
        sourceUris.add(new Path(getStagingUri(sourceStorageUrl, sourceDir)));

        DistCpOptions distcpOptions = new DistCpOptions(sourceUris,
                new Path(getStagingUri(targetStorageUrl, targetDir)));

        // Settings needed for Snapshot distCp.
        distcpOptions.setSyncFolder(true);
        distcpOptions.setDeleteMissing(true);

        // Use snapshot diff if two snapshots exist. Else treat it as simple distCp.
        // get latest replicated snapshot.
        String replicatedSnapshotName = findLatestReplicatedSnapshot(sourceFs, targetFs, sourceDir, targetDir);
        if (StringUtils.isNotBlank(replicatedSnapshotName)) {
            distcpOptions.setUseDiff(true, replicatedSnapshotName, currentSnapshotName);
        }

        if (Boolean.valueOf(cmd.getOptionValue(HdfsSnapshotMirrorProperties.TDE_ENCRYPTION_ENABLED.getName()))) {
            // skipCRCCheck and update enabled
            distcpOptions.setSkipCRC(true);
        }

        distcpOptions.setBlocking(true);
        distcpOptions.setMaxMaps(
                Integer.parseInt(cmd.getOptionValue(HdfsSnapshotMirrorProperties.DISTCP_MAX_MAPS.getName())));
        distcpOptions.setMapBandwidth(
                Integer.parseInt(cmd.getOptionValue(HdfsSnapshotMirrorProperties.MAP_BANDWIDTH_IN_MB.getName())));
        return distcpOptions;
    }

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private String findLatestReplicatedSnapshot(DistributedFileSystem sourceFs, DistributedFileSystem targetFs,
            String sourceDir, String targetDir) throws FalconException {
        try {
            FileStatus[] sourceSnapshots = sourceFs.listStatus(new Path(getSnapshotDir(sourceDir)));
            Set<String> sourceSnapshotNames = new HashSet<String>();
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
                for (int i = 0; i < targetSnapshots.length; i++) {
                    String name = targetSnapshots[i].getPath().getName();
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
        return storageUrl + dir;
    }

    private String getSnapshotDir(String dirName) {
        return dirName + Path.SEPARATOR + ".snapshot" + Path.SEPARATOR;
    }

    protected CommandLine getCommand(String[] args) throws FalconException {
        Options options = new Options();

        Option opt = new Option(HdfsSnapshotMirrorProperties.DISTCP_MAX_MAPS.getName(),
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

        opt = new Option(HdfsSnapshotMirrorProperties.JOB_NN.getName(),
                true, "Replication instance job NN");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.JOB_EXEC_URL.getName(),
                true, "Replication instance job Exec Url");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.JOB_NN_KERBEROS_PRINCIPAL.getName(),
                true, "Replication instance job NN Kerberos Principal");
        opt.setRequired(false);
        options.addOption(opt);

        try {
            return new GnuParser().parse(options, args);
        } catch (ParseException pe) {
            LOG.info("Unabel to parse commad line arguments for HdfsSnapshotReplicator " + pe.getMessage());
            throw  new FalconException(pe.getMessage());
        }
    }

}

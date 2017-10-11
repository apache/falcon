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

package org.apache.falcon.snapshots.retention;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.falcon.FalconException;
import org.apache.falcon.extensions.mirroring.hdfsSnapshot.HdfsSnapshotMirrorProperties;
import org.apache.falcon.retention.EvictionHelper;
import org.apache.falcon.snapshots.util.HdfsSnapshotUtil;
import org.apache.falcon.workflow.util.OozieActionConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * HDFS snapshot evictor.
 */
public class HdfsSnapshotEvictor extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsSnapshotEvictor.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = OozieActionConfigurationHelper.createActionConf();
        int ret = ToolRunner.run(conf, new HdfsSnapshotEvictor(), args);
        if (ret != 0) {
            throw new Exception("Unable to perform eviction action args: " + Arrays.toString(args));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmd = getCommand(args);
        DistributedFileSystem sourceFs = HdfsSnapshotUtil.getSourceFileSystem(cmd,
                new Configuration(getConf()));
        DistributedFileSystem targetFs = HdfsSnapshotUtil.getTargetFileSystem(cmd,
                new Configuration(getConf()));

        String sourceDir = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName());
        String targetDir = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName());

        // evict on source
        String retPolicy = cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_POLICY.getName());
        String ageLimit = cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
        int numSnapshots = Integer.parseInt(
                cmd.getOptionValue(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName()));
        if (retPolicy.equalsIgnoreCase("delete")) {
            evictSnapshots(sourceFs, sourceDir, ageLimit, numSnapshots);
        } else {
            LOG.warn("Unsupported source retention policy {}", retPolicy);
            throw new FalconException("Unsupported source retention policy " + retPolicy);
        }

        // evict on target
        retPolicy = cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_POLICY.getName());
        ageLimit = cmd.getOptionValue(
                HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName());
        numSnapshots = Integer.parseInt(
                cmd.getOptionValue(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName()));
        if (retPolicy.equalsIgnoreCase("delete")) {
            evictSnapshots(targetFs, targetDir, ageLimit, numSnapshots);
        } else {
            LOG.warn("Unsupported target retention policy {}", retPolicy);
            throw new FalconException("Unsupported target retention policy " + retPolicy);
        }

        LOG.info("Completed HDFS Snapshot Eviction.");
        return 0;
    }

    protected static void evictSnapshots(DistributedFileSystem fs, String dirName, String ageLimit,
                                         int numSnapshots) throws FalconException {
        try {
            LOG.info("Started evicting snapshots on dir {}{} using policy {}, agelimit {}, numSnapshot {}",
                    fs.getUri(), dirName, ageLimit, numSnapshots);

            long evictionTime = System.currentTimeMillis() - EvictionHelper.evalExpressionToMilliSeconds(ageLimit);

            dirName = StringUtils.removeEnd(dirName, Path.SEPARATOR);
            String snapshotDir = dirName + Path.SEPARATOR + HdfsSnapshotUtil.SNAPSHOT_DIR_PREFIX + Path.SEPARATOR;
            FileStatus[] snapshots = fs.listStatus(new Path(snapshotDir));
            if (snapshots.length <= numSnapshots) {
                // no eviction needed
                return;
            }

            // Sort by last modified time, ascending order.
            Arrays.sort(snapshots, new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus f1, FileStatus f2) {
                    return Long.compare(f1.getModificationTime(), f2.getModificationTime());
                }
            });

            for (int i = 0; i < (snapshots.length - numSnapshots); i++) {
                // delete if older than ageLimit while retaining numSnapshots
                if (snapshots[i].getModificationTime() < evictionTime) {
                    fs.deleteSnapshot(new Path(dirName), snapshots[i].getPath().getName());
                }
            }

        } catch (ELException ele) {
            LOG.warn("Unable to parse retention age limit {} {}", ageLimit, ele.getMessage());
            throw new FalconException("Unable to parse retention age limit " + ageLimit, ele);
        } catch (IOException ioe) {
            LOG.warn("Unable to evict snapshots from dir {} {}", dirName, ioe);
            throw new FalconException("Unable to evict snapshots from dir " + dirName, ioe);
        }

    }

    private CommandLine getCommand(String[] args) throws org.apache.commons.cli.ParseException {
        Options options = new Options();

        Option opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_NN.getName(), true, "Source Cluster");
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

        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_NN.getName(), true, "Target Cluster");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName(),
                true, "Target snapshot-able dir to replicate");
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

        opt = new Option(HdfsSnapshotMirrorProperties.SNAPSHOT_JOB_NAME.getName(), true,
                "Replication instance job name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_POLICY.getName(),
                true, "Source retention policy");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                true, "Source delete snapshots older than agelimit");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_RETENTION_NUMBER.getName(),
                true, "Source number of snapshots to retain");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_POLICY.getName(),
                true, "Target retention policy");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_AGE_LIMIT.getName(),
                true, "Target delete snapshots older than agelimit");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option(HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_RETENTION_NUMBER.getName(),
                true, "Target number of snapshots to retain");
        opt.setRequired(true);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }
}

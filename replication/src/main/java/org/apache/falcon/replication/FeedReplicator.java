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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.job.JobCountersHandler;
import org.apache.falcon.job.JobType;
import org.apache.falcon.job.JobCounters;
import org.apache.falcon.util.DistCPOptionsUtil;
import org.apache.falcon.util.ReplicationDistCpOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A tool for feed replication that uses DistCp tool to replicate.
 */
public class FeedReplicator extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(FeedReplicator.class);
    private static final String IGNORE = "IGNORE";
    private static final String TDE_ENCRYPTION_ENABLED = "tdeEncryptionEnabled";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new FeedReplicator(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmd = getCommand(args);
        DistCpOptions options = getDistCpOptions(cmd);

        Configuration conf = this.getConf();
        // inject wf configs
        Path confPath = new Path("file:///"
                + System.getProperty("oozie.action.conf.xml"));

        LOG.info("{} found conf ? {}", confPath, confPath.getFileSystem(conf).exists(confPath));
        conf.addResource(confPath);

        String includePathConf = conf.get("falcon.include.path");
        final boolean includePathSet = (includePathConf != null)
                && !IGNORE.equalsIgnoreCase(includePathConf);

        String availabilityFlagOpt = cmd.getOptionValue("availabilityFlag");
        if (StringUtils.isEmpty(availabilityFlagOpt)) {
            availabilityFlagOpt = "NA";
        }
        String availabilityFlag = EntityUtil.SUCCEEDED_FILE_NAME;
        if (cmd.getOptionValue("falconFeedStorageType").equals(Storage.TYPE.FILESYSTEM.name())) {
            availabilityFlag = "NA".equals(availabilityFlagOpt)
                    ? availabilityFlag : availabilityFlagOpt;
        }

        conf.set("falcon.feed.availability.flag", availabilityFlag);
        DistCp distCp = (includePathSet)
                ? new CustomReplicator(conf, options)
                : new DistCp(conf, options);
        LOG.info("Started DistCp");
        Job job = distCp.execute();

        if (cmd.hasOption("counterLogDir")
                && job.getStatus().getState() == JobStatus.State.SUCCEEDED) {
            LOG.info("Gathering counters for the the Feed Replication job");
            Path counterFile = new Path(cmd.getOptionValue("counterLogDir"), "counter.txt");
            JobCounters fsReplicationCounters = JobCountersHandler.getCountersType(JobType.FSREPLICATION.name());
            if (fsReplicationCounters != null) {
                fsReplicationCounters.obtainJobCounters(conf, job, true);
                fsReplicationCounters.storeJobCounters(conf, counterFile);
            }
        }

        if (includePathSet) {
            executePostProcessing(conf, options);  // this only applies for FileSystem Storage.
        }

        LOG.info("Completed DistCp");
        return 0;
    }

    protected CommandLine getCommand(String[] args) throws ParseException {
        Options options = new Options();
        Option opt = new Option("maxMaps", true,
                "max number of maps to use for this copy");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("mapBandwidth", true,
                "bandwidth per map (in MB) to use for this copy");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("sourcePaths", true,
                "comma separtated list of source paths to be copied");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("targetPath", true, "target path");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("falconFeedStorageType", true, "feed storage type");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("availabilityFlag", true, "availability flag");
        opt.setRequired(false);
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
                        + "were removed from the source directory");
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

        opt = new Option("counterLogDir", true, "log directory to store job counter file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(TDE_ENCRYPTION_ENABLED, true, "TDE encryption enabled");
        opt.setRequired(false);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    protected DistCpOptions getDistCpOptions(CommandLine cmd) throws FalconException, IOException {
        String[] paths = cmd.getOptionValue("sourcePaths").trim().split(",");
        List<Path> srcPaths = getPaths(paths);
        String targetPathString = cmd.getOptionValue("targetPath").trim();
        Path targetPath = new Path(targetPathString);

        return DistCPOptionsUtil.getDistCpOptions(cmd, srcPaths, targetPath, false, getConf());
    }

    private List<Path> getPaths(String[] paths) {
        List<Path> listPaths = new ArrayList<>();
        for (String path : paths) {
            listPaths.add(new Path(path));
        }
        return listPaths;
    }

    private void executePostProcessing(Configuration conf, DistCpOptions options) throws IOException, FalconException {
        Path targetPath = options.getTargetPath();
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                targetPath.toUri(), getConf());
        List<Path> inPaths = options.getSourcePaths();
        assert inPaths.size() == 1 : "Source paths more than 1 can't be handled";

        Path sourcePath = inPaths.get(0);
        Path includePath = new Path(getConf().get("falcon.include.path"));
        assert includePath.toString().substring(0, sourcePath.toString().length()).
                equals(sourcePath.toString()) : "Source path is not a subset of include path";

        String relativePath = includePath.toString().substring(sourcePath.toString().length());
        String fixedPath = getFixedPath(relativePath);

        fixedPath = StringUtils.stripStart(fixedPath, "/");
        Path finalOutputPath;
        if (StringUtils.isNotEmpty(fixedPath)) {
            finalOutputPath = new Path(targetPath, fixedPath);
        } else {
            finalOutputPath = targetPath;
        }

        final String availabilityFlag = conf.get("falcon.feed.availability.flag");
        FileStatus[] files = fs.globStatus(finalOutputPath);
        if (files != null) {
            for (FileStatus file : files) {
                fs.create(new Path(file.getPath(), availabilityFlag)).close();
                LOG.info("Created {}", new Path(file.getPath(), availabilityFlag));
            }
        } else {
            // As distcp is not copying empty directories we are creating availabilityFlag file here
            fs.create(new Path(finalOutputPath, availabilityFlag)).close();
            LOG.info("No files present in path: {}", finalOutputPath);
        }
    }

    private String getFixedPath(String relativePath) throws IOException {
        String[] patterns = relativePath.split("/");
        int part = patterns.length - 1;
        for (int index = patterns.length - 1; index >= 0; index--) {
            String pattern = patterns[index];
            if (pattern.isEmpty()) {
                continue;
            }
            Pattern r = FilteredCopyListing.getRegEx(pattern);
            if (!r.toString().equals("(" + pattern + "/)|(" + pattern + "$)")) {
                continue;
            }
            part = index;
            break;
        }
        StringBuilder resultBuffer = new StringBuilder();
        for (int index = 0; index <= part; index++) {
            resultBuffer.append(patterns[index]).append("/");
        }
        String result = resultBuffer.toString();
        return result.substring(0, result.lastIndexOf('/'));
    }
}

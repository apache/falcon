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

package org.apache.falcon.hive;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.falcon.hive.mapreduce.CopyMapper;
import org.apache.falcon.hive.mapreduce.CopyReducer;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.DelimiterUtils;
import org.apache.falcon.hive.util.EventSourcerUtils;
import org.apache.falcon.hive.util.FileUtils;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.falcon.hive.util.HiveDRUtils;
import org.apache.falcon.hive.util.HiveMetastoreUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.HCatClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * DR Tool Driver.
 */
public class HiveDRTool extends Configured implements Tool {

    private static final String META_PATH_FILE_SUFFIX = ".metapath";

    private FileSystem jobFS;
    private FileSystem sourceClusterFS;
    private FileSystem targetClusterFS;

    private HiveDROptions inputOptions;
    private DRStatusStore drStore;
    private String eventsMetaFile;
    private EventSourcerUtils eventSoucerUtil;
    private Configuration jobConf;
    private String executionStage;

    public static final FsPermission STAGING_DIR_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private static final Logger LOG = LoggerFactory.getLogger(HiveDRTool.class);

    public HiveDRTool() {
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return -1;
        }

        try {
            init(args);
        } catch (Throwable e) {
            LOG.error("Invalid arguments: ", e);
            System.err.println("Invalid arguments: " + e.getMessage());
            usage();
            return -1;
        }

        try {
            execute();
        } catch (Exception e) {
            System.err.println("Exception encountered " + e.getMessage());
            e.printStackTrace();
            LOG.error("Exception encountered, cleaning up staging dirs", e);
            cleanup();
            return -1;
        }

        if (inputOptions.getExecutionStage().equalsIgnoreCase(HiveDRUtils.ExecutionStage.IMPORT.name())) {
            cleanup();
        }

        return 0;
    }

    private void init(String[] args) throws Exception {
        LOG.info("Initializing HiveDR");
        inputOptions = parseOptions(args);
        LOG.info("Input Options: {}", inputOptions);

        Configuration sourceConf = FileUtils.getConfiguration(inputOptions.getSourceWriteEP(),
                inputOptions.getSourceNNKerberosPrincipal());
        sourceClusterFS = FileSystem.get(sourceConf);
        Configuration targetConf = FileUtils.getConfiguration(inputOptions.getTargetWriteEP(),
                inputOptions.getTargetNNKerberosPrincipal());
        targetClusterFS = FileSystem.get(targetConf);
        jobConf = FileUtils.getConfiguration(inputOptions.getJobClusterWriteEP(),
                inputOptions.getJobClusterNNPrincipal());
        jobFS = FileSystem.get(jobConf);

        // init DR status store
        drStore = new HiveDRStatusStore(targetClusterFS);
        eventSoucerUtil = new EventSourcerUtils(jobConf, inputOptions.shouldKeepHistory(), inputOptions.getJobName());
    }

    private HiveDROptions parseOptions(String[] args) throws ParseException {
        return HiveDROptions.create(args);
    }

    public Job execute() throws Exception {
        assert inputOptions != null;
        assert getConf() != null;
        executionStage = inputOptions.getExecutionStage();
        LOG.info("Executing Workflow stage : {}", executionStage);
        if (executionStage.equalsIgnoreCase(HiveDRUtils.ExecutionStage.LASTEVENTS.name())) {
            String lastEventsIdFile = getLastEvents(jobConf);
            LOG.info("Last successfully replicated Event file : {}", lastEventsIdFile);
            return null;
        } else if (executionStage.equalsIgnoreCase(HiveDRUtils.ExecutionStage.EXPORT.name())) {
            createStagingDirectory();
            eventsMetaFile = sourceEvents();
            LOG.info("Sourced Events meta file : {}", eventsMetaFile);
            if (StringUtils.isEmpty(eventsMetaFile)) {
                LOG.info("No events to process");
                return null;
            } else {
                /*
                 * eventsMetaFile contains the events to be processed by HiveDr. This file should be available
                 * for the import action as well. Persist the file at a location common to both export and import.
                 */
                persistEventsMetafileLocation(eventsMetaFile);
            }
        } else if (executionStage.equalsIgnoreCase(HiveDRUtils.ExecutionStage.IMPORT.name())) {
            // read the location of eventsMetaFile from hdfs
            eventsMetaFile = getEventsMetaFileLocation();
            if (StringUtils.isEmpty(eventsMetaFile)) {
                LOG.info("No events to process");
                return null;
            }
        } else {
            throw new HiveReplicationException("Invalid Execution stage : " + inputOptions.getExecutionStage());
        }

        Job job = createJob();
        job.submit();

        String jobID = job.getJobID().toString();
        job.getConfiguration().set("HIVEDR_JOB_ID", jobID);

        LOG.info("HiveDR job-id: {}", jobID);
        if (inputOptions.shouldBlock() && !job.waitForCompletion(true)) {
            throw new IOException("HiveDR failure: Job " + jobID + " has failed: "
                    + job.getStatus().getFailureInfo());
        }

        return job;
    }

    private Job createJob() throws Exception {
        String jobName = "hive-dr" + executionStage;
        String userChosenName = getConf().get(JobContext.JOB_NAME);
        if (userChosenName != null) {
            jobName += ": " + userChosenName;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName(jobName);
        job.setJarByClass(CopyMapper.class);
        job.setMapperClass(CopyMapper.class);
        job.setReducerClass(CopyReducer.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
        job.getConfiguration().set(JobContext.NUM_MAPS,
                String.valueOf(inputOptions.getReplicationMaxMaps()));

        for (HiveDRArgs args : HiveDRArgs.values()) {
            if (inputOptions.getValue(args) != null) {
                job.getConfiguration().set(args.getName(), inputOptions.getValue(args));
            } else {
                job.getConfiguration().set(args.getName(), "null");
            }
        }
        job.getConfiguration().set(FileInputFormat.INPUT_DIR, eventsMetaFile);

        return job;
    }

    private void createStagingDirectory() throws IOException, HiveReplicationException {
        Path sourceStagingPath = new Path(inputOptions.getSourceStagingPath());
        Path targetStagingPath = new Path(inputOptions.getTargetStagingPath());
        LOG.info("Source staging path: {}", sourceStagingPath);
        if (!FileSystem.mkdirs(sourceClusterFS, sourceStagingPath, STAGING_DIR_PERMISSION)) {
            throw new IOException("mkdir failed for " + sourceStagingPath);
        }

        LOG.info("Target staging path: {}", targetStagingPath);
        if (!FileSystem.mkdirs(targetClusterFS, targetStagingPath, STAGING_DIR_PERMISSION)) {
            throw new IOException("mkdir failed for " + targetStagingPath);
        }
    }

    private void cleanStagingDirectory() throws HiveReplicationException {
        LOG.info("Cleaning staging directories");
        Path sourceStagingPath = new Path(inputOptions.getSourceStagingPath());
        Path targetStagingPath = new Path(inputOptions.getTargetStagingPath());
        try {
            if (sourceClusterFS.exists(sourceStagingPath)) {
                sourceClusterFS.delete(sourceStagingPath, true);
            }

            if (targetClusterFS.exists(targetStagingPath)) {
                targetClusterFS.delete(targetStagingPath, true);
            }
        } catch (IOException e) {
            LOG.error("Unable to cleanup staging dir:", e);
        }
    }

    private String sourceEvents() throws Exception {
        MetaStoreEventSourcer defaultSourcer = null;
        String inputFilename = null;
        String lastEventsIdFile = FileUtils.DEFAULT_EVENT_STORE_PATH +File.separator+inputOptions.getJobName()+"/"
                +inputOptions.getJobName()+".id";
        Map<String, Long> lastEventsIdMap = getLastDBTableEvents(new Path(lastEventsIdFile));
        try {
            HCatClient sourceMetastoreClient = HiveMetastoreUtils.initializeHiveMetaStoreClient(
                    inputOptions.getSourceMetastoreUri(),
                    inputOptions.getSourceMetastoreKerberosPrincipal(),
                    inputOptions.getSourceHive2KerberosPrincipal());
            defaultSourcer = new MetaStoreEventSourcer(sourceMetastoreClient,
                    new DefaultPartitioner(drStore, eventSoucerUtil), eventSoucerUtil, lastEventsIdMap);
            inputFilename = defaultSourcer.sourceEvents(inputOptions);
        } finally {
            if (defaultSourcer != null) {
                defaultSourcer.cleanUp();
            }
        }

        return inputFilename;
    }

    private String getLastEvents(Configuration conf) throws Exception {
        LastReplicatedEvents lastEvents = new LastReplicatedEvents(conf,
                inputOptions.getTargetMetastoreUri(),
                inputOptions.getTargetMetastoreKerberosPrincipal(),
                inputOptions.getTargetHive2KerberosPrincipal(),
                drStore, inputOptions);
        String eventIdFile = lastEvents.getLastEvents(inputOptions);
        lastEvents.cleanUp();
        return eventIdFile;
    }

    private Map<String, Long> getLastDBTableEvents(Path lastEventIdFile) throws Exception {
        Map<String, Long> lastEventsIdMap = new HashMap<String, Long>();
        BufferedReader in = new BufferedReader(new InputStreamReader(jobFS.open(lastEventIdFile)));
        try {
            String line;
            while ((line=in.readLine())!=null) {
                String[] field = line.trim().split(DelimiterUtils.TAB_DELIM, -1);
                lastEventsIdMap.put(field[0], Long.parseLong(field[1]));
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }

        return lastEventsIdMap;
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            HiveDRTool hiveDRTool = new HiveDRTool();
            exitCode = ToolRunner.run(HiveDRUtils.getDefaultConf(), hiveDRTool, args);
        } catch (Exception e) {
            LOG.error("Couldn't complete HiveDR operation: ", e);
            exitCode = -1;
        }

        System.exit(exitCode);
    }

    private void cleanInputDir() {
        eventSoucerUtil.cleanUpEventInputDir();
    }

    private synchronized void cleanup() throws HiveReplicationException {
        cleanStagingDirectory();
        cleanInputDir();
        cleanTempFiles();
    }

    private void cleanTempFiles() {
        Path eventsDirPath = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH, inputOptions.getJobName());
        Path metaFilePath = new Path(eventsDirPath.toString(), inputOptions.getJobName() + META_PATH_FILE_SUFFIX);
        Path eventsFilePath = new Path(eventsDirPath.toString(), inputOptions.getJobName() + ".id");

        try {
            if (jobFS.exists(metaFilePath)) {
                jobFS.delete(metaFilePath, true);
            }
            if (jobFS.exists(eventsFilePath)) {
                jobFS.delete(eventsFilePath, true);
            }
        } catch (IOException e) {
            LOG.error("Deleting Temp files failed", e);
        }
    }

    public void persistEventsMetafileLocation(final String eventMetaFilePath) throws IOException {
        Path eventsDirPath = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH, inputOptions.getJobName());
        Path metaFilePath = new Path(eventsDirPath.toString(), inputOptions.getJobName() + META_PATH_FILE_SUFFIX);

        OutputStream out = null;
        try {
            out = FileSystem.create(jobFS, metaFilePath, FileUtils.FS_PERMISSION_700);
            out.write(eventMetaFilePath.getBytes());
            out.flush();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    private String getEventsMetaFileLocation() throws IOException {
        Path eventsDirPath = new Path(FileUtils.DEFAULT_EVENT_STORE_PATH, inputOptions.getJobName());
        Path metaFilePath = new Path(eventsDirPath.toString(), inputOptions.getJobName() + META_PATH_FILE_SUFFIX);
        String line = null;
        if (jobFS.exists(metaFilePath)) {
            BufferedReader in = new BufferedReader(new InputStreamReader(jobFS.open(metaFilePath)));
            line = in.readLine();
            in.close();
        }
        return line;
    }


    public static void usage() {
        System.out.println("Usage: hivedrtool -option value ....");
    }
}

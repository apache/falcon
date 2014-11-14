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

package org.apache.falcon.latedata;

import org.apache.commons.cli.*;
import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogPartition;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.util.OozieActionConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A tool for late data handling.
 */
public class LateDataHandler extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(LateDataHandler.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = OozieActionConfigurationHelper.createActionConf();
        ToolRunner.run(conf, new LateDataHandler(), args);
    }

    private static CommandLine getCommand(String[] args) throws ParseException {
        Options options = new Options();

        Option opt = new Option("out", true, "Out file name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("paths", true,
                "Comma separated path list, further separated by #");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(WorkflowExecutionArgs.INPUT_NAMES.getName(), true,
                "Input feed names, further separated by #");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName(), true,
                "Feed storage types corresponding to Input feed names, separated by #");
        opt.setRequired(true);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine command = getCommand(args);

        String pathStr = getOptionValue(command, "paths");
        if (pathStr == null) {
            return 0;
        }

        String[] inputFeeds = getOptionValue(command, WorkflowExecutionArgs.INPUT_NAMES.getName()).split("#");
        String[] pathGroups = pathStr.split("#");
        String[] inputFeedStorageTypes =
            getOptionValue(command, WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName()).split("#");

        Map<String, Long> metrics = computeMetrics(inputFeeds, pathGroups, inputFeedStorageTypes);

        Path file = new Path(command.getOptionValue("out"));
        LOG.info("Persisting late data metrics: {} to file: {}", metrics, file);
        persistMetrics(metrics, file);

        return 0;
    }

    private String getOptionValue(CommandLine command, String option) {
        String value = command.getOptionValue(option);
        if (value.equals("null")) {
            return null;
        }
        return value;
    }

    private Map<String, Long> computeMetrics(String[] inputFeeds, String[] pathGroups,
                                             String[] inputFeedStorageTypes)
        throws IOException, FalconException, URISyntaxException {

        Map<String, Long> computedMetrics = new LinkedHashMap<String, Long>();
        for (int index = 0; index < pathGroups.length; index++) {
            long storageMetric = computeStorageMetric(pathGroups[index], inputFeedStorageTypes[index], getConf());
            computedMetrics.put(inputFeeds[index], storageMetric);
        }

        return computedMetrics;
    }

    private void persistMetrics(Map<String, Long> metrics,
                                Path file) throws IOException, FalconException {
        OutputStream out = null;
        try {  // created in a map job
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(file.toUri());
            out = fs.create(file);

            for (Map.Entry<String, Long> entry : metrics.entrySet()) {
                out.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes());
            }
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ignore) {
                    // ignore
                }
            }
        }
    }

    /**
     * This method computes the storage metrics for a given feed's instance or partition.
     * It uses size on disk as the metric for File System Storage.
     * It uses create time as the metric for Catalog Table Storage.
     *
     * The assumption is that if a partition has changed or reinstated, the underlying
     * metric would change, either size or create time.
     *
     * @param feedUriTemplate URI for the feed storage, filesystem path or table uri
     * @param feedStorageType feed storage type
     * @param conf configuration
     * @return computed metric
     * @throws IOException
     * @throws FalconException
     * @throws URISyntaxException
     */
    public long computeStorageMetric(String feedUriTemplate, String feedStorageType, Configuration conf)
        throws IOException, FalconException, URISyntaxException {

        Storage.TYPE storageType = Storage.TYPE.valueOf(feedStorageType);

        if (storageType == Storage.TYPE.FILESYSTEM) {
            // usage on file system is the metric
            return getFileSystemUsageMetric(feedUriTemplate, conf);
        } else if (storageType == Storage.TYPE.TABLE) {
            // todo: this should have been done in oozie mapper but el ${coord:dataIn('input')} returns hcat scheme
            feedUriTemplate = feedUriTemplate.replace("hcat", "thrift");
            // creation time of the given partition is the metric
            return getTablePartitionCreateTimeMetric(feedUriTemplate);
        }

        throw new IllegalArgumentException("Unknown storage type: " + feedStorageType);
    }

    /**
     * The storage metric for File System Storage is the size of content
     * this feed's instance represented by the path uses on the file system.
     *
     * If this instance was reinstated, the assumption is that the size of
     * this instance on disk would change.
     *
     * @param pathGroup path on file system
     * @param conf configuration
     * @return metric as the size of data on file system
     * @throws IOException
     */
    private long getFileSystemUsageMetric(String pathGroup, Configuration conf)
        throws IOException, FalconException {
        long usage = 0;
        for (String pathElement : pathGroup.split(",")) {
            Path inPath = new Path(pathElement);
            usage += usage(inPath, conf);
        }

        return usage;
    }

    private long usage(Path inPath, Configuration conf) throws IOException, FalconException {
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(inPath.toUri(), conf);
        FileStatus[] fileStatuses = fs.globStatus(inPath);
        if (fileStatuses == null || fileStatuses.length == 0) {
            return 0;
        }
        long totalSize = 0;
        for (FileStatus fileStatus : fileStatuses) {
            totalSize += fs.getContentSummary(fileStatus.getPath()).getLength();
        }
        return totalSize;
    }

    /**
     * The storage metric for Table Storage is the create time of the given partition
     * since there is API in Hive nor HCatalog to find if a partition has changed.
     *
     * If this partition was reinstated, the assumption is that the create time of
     * this partition would change.
     *
     * @param feedUriTemplate catalog table uri
     * @return metric as creation time of the given partition
     * @throws IOException
     * @throws URISyntaxException
     * @throws FalconException
     */
    private long getTablePartitionCreateTimeMetric(String feedUriTemplate)
        throws IOException, URISyntaxException, FalconException {

        CatalogStorage storage = (CatalogStorage)
                FeedHelper.createStorage(Storage.TYPE.TABLE.name(), feedUriTemplate, getConf());
        CatalogPartition partition = CatalogServiceFactory.getCatalogService().getPartition(
                getConf(), storage.getCatalogUrl(), storage.getDatabase(),
                storage.getTable(), storage.getPartitions());
        return partition == null ? 0 : partition.getCreateTime();
    }

    /**
     * This method compares the recorded metrics persisted in file against
     * the recently computed metrics and returns the list of feeds that has changed.
     *
     * @param file persisted metrics from the first run
     * @param metrics newly computed metrics
     * @param conf configuration
     * @return list if feed names which has changed, empty string is none has changed
     * @throws Exception
     */
    public String detectChanges(Path file, Map<String, Long> metrics, Configuration conf)
        throws Exception {

        StringBuilder buffer = new StringBuilder();
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(file.toUri(), conf);
        BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(file)));
        String line;
        try {
            Map<String, Long> recordedMetrics = new LinkedHashMap<String, Long>();
            while ((line = in.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                int index = line.indexOf('=');
                String key = line.substring(0, index);
                long size = Long.parseLong(line.substring(index + 1));
                recordedMetrics.put(key, size);
            }

            for (Map.Entry<String, Long> entry : metrics.entrySet()) {
                if (recordedMetrics.get(entry.getKey()) == null) {
                    LOG.info("No matching key {}", entry.getKey());
                    continue;
                }
                if (!recordedMetrics.get(entry.getKey()).equals(entry.getValue())) {
                    LOG.info("Recorded size: {} is different from new size {}",
                            recordedMetrics.get(entry.getKey()), entry.getValue());
                    buffer.append(entry.getKey()).append(',');
                }
            }
            if (buffer.length() == 0) {
                return "";
            } else {
                return buffer.substring(0, buffer.length() - 1);
            }

        } finally {
            in.close();
        }
    }
}

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

package org.apache.falcon.retention;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.catalog.CatalogPartition;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.FileSystemStorage;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.common.FeedDataPath.VARS;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Feed Evictor is called only if the retention policy that applies
 * to the feed is that of delete.
 */
public class FeedEvictor extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(FeedEvictor.class);

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    private static final String FORMAT = "yyyyMMddHHmm";

    // constants to be used while preparing HCatalog partition filter query
    private static final String FILTER_ST_BRACKET = "(";
    private static final String FILTER_END_BRACKET = ")";
    private static final String FILTER_QUOTE = "'";
    private static final String FILTER_AND = " and ";
    private static final String FILTER_OR = " or ";
    private static final String FILTER_LESS_THAN = " < ";
    private static final String FILTER_EQUALS = " = ";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path confPath = new Path("file:///" + System.getProperty("oozie.action.conf.xml"));

        LOG.info("{} found ? {}", confPath, confPath.getFileSystem(conf).exists(confPath));
        conf.addResource(confPath);
        int ret = ToolRunner.run(conf, new FeedEvictor(), args);
        if (ret != 0) {
            throw new Exception("Unable to perform eviction action args: " + Arrays.toString(args));
        }
    }

    private final Map<VARS, String> map = new TreeMap<VARS, String>();
    private final StringBuffer instancePaths = new StringBuffer();
    private final StringBuffer buffer = new StringBuffer();

    @Override
    public int run(String[] args) throws Exception {

        CommandLine cmd = getCommand(args);
        String feedPattern = cmd.getOptionValue("feedBasePath")
                .replaceAll(Storage.QUESTION_EXPR_START_REGEX, Storage.DOLLAR_EXPR_START_REGEX);
        String retentionType = cmd.getOptionValue("retentionType");
        String retentionLimit = cmd.getOptionValue("retentionLimit");
        String timeZone = cmd.getOptionValue("timeZone");
        String frequency = cmd.getOptionValue("frequency"); //to write out smart path filters
        String logFile = cmd.getOptionValue("logFile");
        String feedStorageType = cmd.getOptionValue("falconFeedStorageType");

        LOG.info("Applying retention on {} type: {}, Limit: {}, timezone: {}, frequency: {}, storage: {}",
                feedPattern, retentionType, retentionLimit, timeZone, frequency, feedStorageType);

        Storage storage = FeedHelper.createStorage(feedStorageType, feedPattern);
        evict(storage, retentionLimit, timeZone);

        Path path = new Path(logFile);
        EvictedInstanceSerDe.serializeEvictedInstancePaths(
                path.getFileSystem(getConf()), path, instancePaths);

        int len = buffer.length();
        if (len > 0) {
            OUT.get().println("instances=" + buffer.substring(0, len - 1));
        } else {
            OUT.get().println("instances=NULL");
        }

        return 0;
    }

    private void evict(Storage storage, String retentionLimit, String timeZone)
        throws Exception {

        if (storage.getType() == Storage.TYPE.FILESYSTEM) {
            evictFS((FileSystemStorage) storage, retentionLimit, timeZone);
        } else if (storage.getType() == Storage.TYPE.TABLE) {
            evictTable((CatalogStorage) storage, retentionLimit, timeZone);
        }
    }

    private void evictFS(FileSystemStorage storage, String retentionLimit, String timeZone)
        throws Exception {

        for (Location location : storage.getLocations()) {
            fileSystemEvictor(storage.getUriTemplate(location.getType()), retentionLimit, timeZone);
        }
    }

    private void fileSystemEvictor(String feedPath, String retentionLimit, String timeZone)
        throws IOException, ELException {

        Path normalizedPath = new Path(feedPath);
        FileSystem fs = normalizedPath.getFileSystem(getConf());
        feedPath = normalizedPath.toUri().getPath();
        LOG.info("Normalized path: {}", feedPath);

        Pair<Date, Date> range = getDateRange(retentionLimit);
        String dateMask = getDateFormatInPath(feedPath);

        List<Path> toBeDeleted = discoverInstanceToDelete(feedPath, timeZone, dateMask, range.first, fs);
        if (toBeDeleted.isEmpty()) {
            LOG.info("No instances to delete.");
            return;
        }

        DateFormat dateFormat = new SimpleDateFormat(FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        Path feedBasePath = getFeedBasePath(feedPath);
        for (Path path : toBeDeleted) {
            deleteInstance(fs, path, feedBasePath);
            Date date = getDate(new Path(path.toUri().getPath()), feedPath, dateMask, timeZone);
            buffer.append(dateFormat.format(date)).append(',');
            instancePaths.append(path).append(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR);
        }
    }

    private Path getFeedBasePath(String feedPath) throws IOException {
        Matcher matcher = FeedDataPath.PATTERN.matcher(feedPath);
        if (matcher.find()) {
            return new Path(feedPath.substring(0, matcher.start()));
        } else {
            throw new IOException("Unable to resolve pattern for feedPath: " + feedPath);
        }

    }

    private Pair<Date, Date> getDateRange(String period) throws ELException {
        Long duration = (Long) EVALUATOR.evaluate("${" + period + "}",
                Long.class, RESOLVER, RESOLVER);
        Date end = new Date();
        Date start = new Date(end.getTime() - duration);
        return Pair.of(start, end);
    }

    private List<Path> discoverInstanceToDelete(String inPath, String timeZone, String dateMask,
                                                Date start, FileSystem fs) throws IOException {

        FileStatus[] files = findFilesForFeed(fs, inPath);
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }

        List<Path> toBeDeleted = new ArrayList<Path>();
        for (FileStatus file : files) {
            Date date = getDate(new Path(file.getPath().toUri().getPath()),
                    inPath, dateMask, timeZone);
            LOG.debug("Considering {}", file.getPath().toUri().getPath());
            LOG.debug("Date: {}", date);
            if (date != null && !isDateInRange(date, start)) {
                toBeDeleted.add(file.getPath());
            }
        }
        return toBeDeleted;
    }

    private String getDateFormatInPath(String inPath) {
        String mask = extractDatePartFromPathMask(inPath, inPath);
        //yyyyMMddHHmm
        return mask.replaceAll(VARS.YEAR.regex(), "yyyy")
                .replaceAll(VARS.MONTH.regex(), "MM")
                .replaceAll(VARS.DAY.regex(), "dd")
                .replaceAll(VARS.HOUR.regex(), "HH")
                .replaceAll(VARS.MINUTE.regex(), "mm");
    }

    private FileStatus[] findFilesForFeed(FileSystem fs, String feedBasePath) throws IOException {

        Matcher matcher = FeedDataPath.PATTERN.matcher(feedBasePath);
        while (matcher.find()) {
            String var = feedBasePath.substring(matcher.start(), matcher.end());
            feedBasePath = feedBasePath.replaceAll(Pattern.quote(var), "*");
            matcher = FeedDataPath.PATTERN.matcher(feedBasePath);
        }
        LOG.info("Searching for {}", feedBasePath);
        return fs.globStatus(new Path(feedBasePath));
    }

    private String extractDatePartFromPathMask(String mask, String inPath) {
        String[] elements = FeedDataPath.PATTERN.split(mask);

        String out = inPath;
        for (String element : elements) {
            out = out.replaceFirst(element, "");
        }
        return out;
    }

    //consider just the first occurrence of the pattern
    private Date getDate(Path file, String inMask,
                         String dateMask, String timeZone) {
        String path = extractDatePartFromPathMask(inMask, file.toString());
        populateDatePartMap(path, dateMask);

        String errArg = file + "(" + inMask + ")";
        if (map.isEmpty()) {
            LOG.warn("No date present in {}", errArg);
            return null;
        }

        StringBuilder date = new StringBuilder();
        int ordinal = 0;
        for (VARS var : map.keySet()) {
            if (ordinal++ == var.ordinal()) {
                date.append(map.get(var));
            } else {
                LOG.warn("Prior element to {} is missing {}", var, errArg);
                return null;
            }
        }

        try {
            DateFormat dateFormat = new SimpleDateFormat(FORMAT.
                    substring(0, date.length()));
            dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
            return dateFormat.parse(date.toString());
        } catch (ParseException e) {
            LOG.warn("Unable to parse date: {}, {}", date, errArg);
            return null;
        }
    }

    private void populateDatePartMap(String path, String mask) {
        map.clear();
        Matcher matcher = FeedDataPath.DATE_FIELD_PATTERN.matcher(mask);
        int start = 0;
        while (matcher.find(start)) {
            String subMask = mask.substring(matcher.start(), matcher.end());
            String subPath = path.substring(matcher.start(), matcher.end());
            VARS var = VARS.from(subMask);
            if (!map.containsKey(var)) {
                map.put(var, subPath);
            }
            start = matcher.start() + 1;
        }
    }

    private boolean isDateInRange(Date date, Date start) {
        //ignore end ( && date.compareTo(end) <= 0 )
        return date.compareTo(start) >= 0;
    }

    private void deleteInstance(FileSystem fs, Path path, Path feedBasePath) throws IOException {
        if (fs.delete(path, true)) {
            LOG.info("Deleted instance: {}", path);
        }else{
            throw new IOException("Unable to delete instance: " + path);
        }
        deleteParentIfEmpty(fs, path.getParent(), feedBasePath);
    }

    private CommandLine getCommand(String[] args) throws org.apache.commons.cli.ParseException {
        Options options = new Options();

        Option opt = new Option("feedBasePath", true,
                "base path for feed, ex /data/feed/${YEAR}-${MONTH}");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("falconFeedStorageType", true, "feed storage type, FileSystem or Table");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("retentionType", true,
                "type of retention policy like delete, archive etc");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("retentionLimit", true,
                "time limit for retention, ex hours(5), months(2), days(90)");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("timeZone", true, "timezone for feed, ex UTC");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("frequency", true,
                "frequency of feed,  ex hourly, daily, monthly, minute, weekly, yearly");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("logFile", true, "log file for capturing size of feed");
        opt.setRequired(true);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    private void evictTable(CatalogStorage storage, String retentionLimit, String timeZone)
        throws Exception {

        LOG.info("Applying retention on {}, Limit: {}, timezone: {}",
                storage.getTable(), retentionLimit, timeZone);

        // get sorted date partition keys and values
        List<String> datedPartKeys = new ArrayList<String>();
        List<String> datedPartValues = new ArrayList<String>();
        fillSortedDatedPartitionKVs(storage, datedPartKeys, datedPartValues, retentionLimit, timeZone);

        List<CatalogPartition> toBeDeleted = discoverPartitionsToDelete(
                storage, datedPartKeys, datedPartValues);
        if (toBeDeleted.isEmpty()) {
            LOG.info("No partitions to delete.");
            return;
        }

        final boolean isTableExternal = CatalogServiceFactory.getCatalogService().isTableExternal(
                storage.getCatalogUrl(), storage.getDatabase(), storage.getTable());

        dropPartitions(storage, toBeDeleted, datedPartKeys, isTableExternal);
    }

    private List<CatalogPartition> discoverPartitionsToDelete(CatalogStorage storage,
        List<String> datedPartKeys, List<String> datedPartValues) throws FalconException, ELException {

        final String filter = createFilter(datedPartKeys, datedPartValues);
        return CatalogServiceFactory.getCatalogService().listPartitionsByFilter(
                storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(), filter);
    }

    private void fillSortedDatedPartitionKVs(CatalogStorage storage, List<String> sortedPartKeys,
        List<String> sortedPartValues, String retentionLimit, String timeZone) throws ELException {
        Pair<Date, Date> range = getDateRange(retentionLimit);

        // sort partition keys and values by the date pattern present in value
        Map<VARS, String> sortedPartKeyMap = new TreeMap<VARS, String>();
        Map<VARS, String> sortedPartValueMap = new TreeMap<VARS, String>();
        for (Entry<String, String> entry : storage.getPartitions().entrySet()) {
            String datePattern = entry.getValue();
            String mask = datePattern.replaceAll(VARS.YEAR.regex(), "yyyy")
                .replaceAll(VARS.MONTH.regex(), "MM")
                .replaceAll(VARS.DAY.regex(), "dd")
                .replaceAll(VARS.HOUR.regex(), "HH")
                .replaceAll(VARS.MINUTE.regex(), "mm");

            // find the first date pattern present in date mask
            VARS vars = VARS.presentIn(mask);
            // skip this partition if date mask doesn't contain any date format
            if (vars == null) {
                continue;
            }

            // construct dated partition value as per format
            DateFormat dateFormat = new SimpleDateFormat(mask);
            dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
            String partitionValue = dateFormat.format(range.first);

            // add partition key and value in their sorted maps
            if (!sortedPartKeyMap.containsKey(vars)) {
                sortedPartKeyMap.put(vars, entry.getKey());
            }

            if (!sortedPartValueMap.containsKey(vars)) {
                sortedPartValueMap.put(vars, partitionValue);
            }
        }

        // add map entries to lists of partition keys and values
        sortedPartKeys.addAll(sortedPartKeyMap.values());
        sortedPartValues.addAll(sortedPartValueMap.values());
    }

    private String createFilter(List<String> datedPartKeys, List<String> datedPartValues)
        throws ELException {

        int numPartitions = datedPartKeys.size();

        /* Construct filter query string. As an example, suppose the dated partition keys
         * are: [year, month, day, hour] and dated partition values are [2014, 02, 24, 10].
         * Then the filter query generated is of the format:
         * "(year < '2014') or (year = '2014' and month < '02') or
         * (year = '2014' and month = '02' and day < '24') or
         * or (year = '2014' and month = '02' and day = '24' and hour < '10')"
         */
        StringBuilder filterBuffer = new StringBuilder();
        for (int curr = 0; curr < numPartitions; curr++) {
            if (curr > 0) {
                filterBuffer.append(FILTER_OR);
            }
            filterBuffer.append(FILTER_ST_BRACKET);
            for (int prev = 0; prev < curr; prev++) {
                filterBuffer.append(datedPartKeys.get(prev))
                    .append(FILTER_EQUALS)
                    .append(FILTER_QUOTE)
                    .append(datedPartValues.get(prev))
                    .append(FILTER_QUOTE)
                    .append(FILTER_AND);
            }
            filterBuffer.append(datedPartKeys.get(curr))
                  .append(FILTER_LESS_THAN)
                  .append(FILTER_QUOTE)
                  .append(datedPartValues.get(curr))
                  .append(FILTER_QUOTE)
                  .append(FILTER_END_BRACKET);
        }

        return filterBuffer.toString();
    }

    private void dropPartitions(CatalogStorage storage, List<CatalogPartition> partitionsToDelete,
        List<String> datedPartKeys, boolean isTableExternal) throws FalconException, IOException {

        // get table partition columns
        List<String> partColumns = CatalogServiceFactory.getCatalogService().getTablePartitionCols(
            storage.getCatalogUrl(), storage.getDatabase(), storage.getTable());

        /* In case partition columns are a super-set of dated partitions, there can be multiple
         * partitions that share the same set of date-partition values. All such partitions can
         * be deleted by issuing a single HCatalog dropPartition call per date-partition values.
         * Arrange the partitions grouped by each set of date-partition values.
         */
        Map<Map<String, String>, List<CatalogPartition>> dateToPartitionsMap = new HashMap<
            Map<String, String>, List<CatalogPartition>>();
        for (CatalogPartition partitionToDrop : partitionsToDelete) {
            // create a map of name-values of all columns of this partition
            Map<String, String> partitions = new HashMap<String, String>();
            for (int i = 0; i < partColumns.size(); i++) {
                partitions.put(partColumns.get(i), partitionToDrop.getValues().get(i));
            }

            // create a map of name-values of dated sub-set of this partition
            Map<String, String> datedPartitions = new HashMap<String, String>();
            for (String datedPart : datedPartKeys) {
                datedPartitions.put(datedPart, partitions.get(datedPart));
            }

            // add a map entry of this catalog partition corresponding to its date-partition values
            List<CatalogPartition> catalogPartitions;
            if (dateToPartitionsMap.containsKey(datedPartitions)) {
                catalogPartitions = dateToPartitionsMap.get(datedPartitions);
            } else {
                catalogPartitions = new ArrayList<CatalogPartition>();
            }
            catalogPartitions.add(partitionToDrop);
            dateToPartitionsMap.put(datedPartitions, catalogPartitions);
        }

        // delete each entry within dateToPartitions Map
        for (Entry<Map<String, String>, List<CatalogPartition>> entry : dateToPartitionsMap.entrySet()) {
            dropPartitionInstances(storage, entry.getValue(), entry.getKey(), isTableExternal);
        }
    }

    private void dropPartitionInstances(CatalogStorage storage, List<CatalogPartition> partitionsToDrop,
        Map<String, String> partSpec, boolean isTableExternal) throws FalconException, IOException {

        boolean deleted = CatalogServiceFactory.getCatalogService().dropPartitions(
                storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(), partSpec);

        if (!deleted) {
            return;
        }

        for (CatalogPartition partitionToDrop : partitionsToDrop) {
            if (isTableExternal) { // nuke the dirs if an external table
                final String location = partitionToDrop.getLocation();
                final Path path = new Path(location);
                deleted = path.getFileSystem(new Configuration()).delete(path, true);
            }
            if (!isTableExternal || deleted) {
                // replace ',' with ';' since message producer splits instancePaths string by ','
                String partitionInfo = partitionToDrop.getValues().toString().replace("," , ";");
                LOG.info("Deleted partition: " + partitionInfo);
                buffer.append(partSpec).append(',');
                instancePaths.append(getEvictedPartitionPath(storage, partitionToDrop))
                        .append(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR);
            }
        }
    }

    private static String getEvictedPartitionPath(final CatalogStorage storage,
                                                  final CatalogPartition partitionToDrop) {
        String uriTemplate = storage.getUriTemplate(); // no need for location type for table
        List<String> values = partitionToDrop.getValues();
        StringBuilder partitionPath = new StringBuilder();
        int index = 0;
        for (String partitionKey : storage.getDatedPartitionKeys()) {
            String dateMask = storage.getPartitionValue(partitionKey);
            String date = values.get(index);

            partitionPath.append(uriTemplate.replace(dateMask, date));
            partitionPath.append(CatalogStorage.PARTITION_SEPARATOR);
            LOG.info("partitionPath: " + partitionPath);
        }
        partitionPath.setLength(partitionPath.length() - 1);

        LOG.info("Return partitionPath: " + partitionPath);
        return partitionPath.toString();
    }

    private void deleteParentIfEmpty(FileSystem fs, Path parent, Path feedBasePath) throws IOException {
        if (feedBasePath.equals(parent)) {
            LOG.info("Not deleting feed base path: {}", parent);
        } else {
            FileStatus[] files = fs.listStatus(parent);
            if (files != null && files.length == 0) {
                LOG.info("Parent path: {} is empty, deleting path", parent);
                if (fs.delete(parent, true)) {
                    LOG.info("Deleted empty dir: {}", parent);
                } else {
                    throw new IOException("Unable to delete parent path:" + parent);
                }
                deleteParentIfEmpty(fs, parent.getParent(), feedBasePath);
            }
        }
    }
}

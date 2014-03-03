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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;

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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Feed Evictor is called only if the retention policy that applies
 * to the feed is that of delete.
 */
public class FeedEvictor extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(FeedEvictor.class);

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    private static final String FORMAT = "yyyyMMddHHmm";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path confPath = new Path("file:///" + System.getProperty("oozie.action.conf.xml"));

        LOG.info(confPath + " found ? " + confPath.getFileSystem(conf).exists(confPath));
        conf.addResource(confPath);
        int ret = ToolRunner.run(conf, new FeedEvictor(), args);
        if (ret != 0) {
            throw new Exception("Unable to perform eviction action args: " + Arrays.toString(args));
        }
    }

    private final Map<VARS, String> map = new TreeMap<VARS, String>();
    private final StringBuffer instancePaths = new StringBuffer("instancePaths=");
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

        LOG.info("Applying retention on " + feedPattern + " type: " + retentionType
                + ", Limit: " + retentionLimit + ", timezone: " + timeZone
                + ", frequency: " + frequency + ", storage" + feedStorageType);

        Storage storage = FeedHelper.createStorage(feedStorageType, feedPattern);
        evict(storage, retentionLimit, timeZone);

        logInstancePaths(new Path(logFile));

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
        LOG.info("Normalized path : " + feedPath);

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
            Date date = getDate(path, feedPath, dateMask, timeZone);
            buffer.append(dateFormat.format(date)).append(',');
            instancePaths.append(path).append(",");
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

    private void logInstancePaths(Path path) throws IOException {
        LOG.info("Writing deleted instances to path " + path);
        FileSystem logfs = path.getFileSystem(getConf());
        OutputStream out = logfs.create(path);
        out.write(instancePaths.toString().getBytes());
        out.close();
        if (LOG.isDebugEnabled()) {
            debug(logfs, path);
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
            LOG.debug("Considering " + file.getPath().toUri().getPath());
            LOG.debug("Date : " + date);
            if (date != null && !isDateInRange(date, start)) {
                toBeDeleted.add(new Path(file.getPath().toUri().getPath()));
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
        LOG.info("Searching for " + feedBasePath);
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
            LOG.warn("No date present in " + errArg);
            return null;
        }

        String date = "";
        int ordinal = 0;
        for (VARS var : map.keySet()) {
            if (ordinal++ == var.ordinal()) {
                date += map.get(var);
            } else {
                LOG.warn("Prior element to " + var + " is missing " + errArg);
                return null;
            }
        }

        try {
            DateFormat dateFormat = new SimpleDateFormat(FORMAT.
                    substring(0, date.length()));
            dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
            return dateFormat.parse(date);
        } catch (ParseException e) {
            LOG.warn("Unable to parse date : " + date + ", " + errArg);
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
            LOG.info("Deleted instance :" + path);
        }else{
            throw new IOException("Unable to delete instance: " + path);
        }
        deleteParentIfEmpty(fs, path.getParent(), feedBasePath);
    }

    private void debug(FileSystem fs, Path outPath) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream instance = fs.open(outPath);
        IOUtils.copyBytes(instance, writer, 4096, true);
        LOG.debug("Instance Paths copied to " + outPath);
        LOG.debug("Written " + writer);
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

        LOG.info("Applying retention on " + storage.getTable()
                + ", Limit: " + retentionLimit + ", timezone: " + timeZone);

        String datedPartitionKey = storage.getDatedPartitionKey();
        String datePattern = storage.getPartitionValue(datedPartitionKey);
        String dateMask = datePattern.replaceAll(VARS.YEAR.regex(), "yyyy")
                .replaceAll(VARS.MONTH.regex(), "MM")
                .replaceAll(VARS.DAY.regex(), "dd")
                .replaceAll(VARS.HOUR.regex(), "HH")
                .replaceAll(VARS.MINUTE.regex(), "mm");

        List<CatalogPartition> toBeDeleted = discoverPartitionsToDelete(
                storage, retentionLimit, timeZone, dateMask);
        if (toBeDeleted.isEmpty()) {
            LOG.info("No partitions to delete.");
            return;
        }

        final boolean isTableExternal = CatalogServiceFactory.getCatalogService().isTableExternal(
                storage.getCatalogUrl(), storage.getDatabase(), storage.getTable());

        dropPartitions(storage, toBeDeleted, isTableExternal);
    }

    private List<CatalogPartition> discoverPartitionsToDelete(CatalogStorage storage, String retentionLimit,
                                                           String timeZone, String dateMask)
        throws FalconException, ELException {

        final String filter = createFilter(storage, retentionLimit, timeZone, dateMask);
        return CatalogServiceFactory.getCatalogService().listPartitionsByFilter(
                storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(), filter);
    }

    private String createFilter(CatalogStorage storage, String retentionLimit,
                                String timeZone, String dateMask) throws ELException {

        Pair<Date, Date> range = getDateRange(retentionLimit);
        DateFormat dateFormat = new SimpleDateFormat(dateMask);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        String beforeDate = dateFormat.format(range.first);

        String datedPartitionKey = storage.getDatedPartitionKey();

        StringBuilder filterBuffer = new StringBuilder();
        filterBuffer.append(datedPartitionKey)
                .append(" < ")
                .append("'")
                .append(beforeDate)
                .append("'");

        return filterBuffer.toString();
    }

    private void dropPartitions(CatalogStorage storage, List<CatalogPartition> partitionsToDelete,
                                boolean isTableExternal) throws FalconException, IOException {

        for (CatalogPartition partitionToDrop : partitionsToDelete) {
            if (dropPartition(storage, partitionToDrop, isTableExternal)) {
                LOG.info("Deleted partition: " + partitionToDrop.getValues());
                buffer.append(partitionToDrop.getValues().get(0)).append(',');
                instancePaths.append(partitionToDrop.getValues()).append(",");
            }
        }
    }

    private boolean dropPartition(CatalogStorage storage, CatalogPartition partitionToDrop,
                                  boolean isTableExternal) throws FalconException, IOException {

        String datedPartitionKey = storage.getDatedPartitionKey();

        Map<String, String> partitions = new HashMap<String, String>();
        partitions.put(datedPartitionKey, partitionToDrop.getValues().get(0));

        boolean dropped = CatalogServiceFactory.getCatalogService().dropPartitions(
                storage.getCatalogUrl(), storage.getDatabase(), storage.getTable(), partitions);

        boolean deleted = true;
        if (isTableExternal) { // nuke the dirs if an external table
            final String location = partitionToDrop.getLocation();
            final Path path = new Path(location);
            deleted = path.getFileSystem(new Configuration()).delete(path, true);
        }

        return dropped && deleted;
    }

    private void deleteParentIfEmpty(FileSystem fs, Path parent, Path feedBasePath) throws IOException {
        if (feedBasePath.equals(parent)) {
            LOG.info("Not deleting feed base path:" + parent);
        } else {
            FileStatus[] files = fs.listStatus(parent);
            if (files != null && files.length == 0) {
                LOG.info("Parent path: " + parent + " is empty, deleting path");
                if (fs.delete(parent, true)) {
                    LOG.info("Deleted empty dir: " + parent);
                } else {
                    throw new IOException("Unable to delete parent path:" + parent);
                }
                deleteParentIfEmpty(fs, parent.getParent(), feedBasePath);
            }
        }
    }

}

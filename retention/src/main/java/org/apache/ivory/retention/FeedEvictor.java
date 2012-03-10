/*
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

package org.apache.ivory.retention;

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ivory.Pair;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.log4j.Logger;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Feed Evictor is called only if the retention policy that applies
 * to the feed is that of delete.
 */
public class FeedEvictor extends Configured implements Tool {

    private static Logger LOG = Logger.getLogger(FeedEvictor.class);

    private static final ExpressionEvaluator EVALUATOR = new
            ExpressionEvaluatorImpl();
    private static final ExpressionHelper resolver = ExpressionHelper.get();

    static PrintStream stream = System.out;
    static PrintStream instancePathsStream =System.out;

    private enum VARS {
        YEAR("yyyy"), MONTH("MM"), DAY("dd"), HOUR("HH"), MINUTE("mm");

        private final Pattern pattern;
        private final String datePattern;

        private VARS(String datePattern) {
            pattern = Pattern.compile("\\$\\{" + name() + "\\}");
            this.datePattern = datePattern;
        }

        public String regex() {
            return pattern.pattern();
        }

        public static VARS from(String str) {
            for (VARS var : VARS.values()) {
                if (var.datePattern.equals(str)) {
                    return var;
                }
            }
            return null;
        }
    }

    private static Pattern pattern = Pattern.compile
            (VARS.YEAR.regex() + "|" + VARS.MONTH.regex() + "|" +
                    VARS.DAY.regex() + "|" + VARS.HOUR.regex() + "|" +
                    VARS.MINUTE.regex());

    private static final Pattern dateFieldPattern = Pattern.
            compile("yyyy|MM|dd|HH|mm");
    private static final String format = "yyyyMMddHHmm";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path confPath = new Path("file://" + System.getProperty("oozie.action.conf.xml"));

        LOG.info(confPath + " found ? " +
                confPath.getFileSystem(conf).exists(confPath));
        conf.addResource(confPath);
        int ret = ToolRunner.run(conf, new FeedEvictor(), args);
        if (ret != 0) {
            throw new Exception("Unable to perform eviction action args: " +
                    Arrays.toString(args));
        }
    }

    private FileSystem fs;
    private Map<VARS, String> map = new TreeMap<VARS, String>();

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            printUsage();
            for(int i=0;i<args.length;i++){
            	System.out.println("Args: "+args[i]);
            }
            return -1;
        }
        String feedBasePath = args[0].replaceAll("\\?\\{", "\\$\\{");
        String retentionType = args[1];
        String retentionLimit = args[2];
        String timeZone = args[3];
        String frequency = args[4]; //to write out smart path filters
        String logFile=args[5];

        Path normalizedPath = new Path(feedBasePath);
        fs = normalizedPath.getFileSystem(getConf());
        feedBasePath = normalizedPath.toUri().getPath();
        LOG.info("Normalized path : " + feedBasePath);
        Pair<Date, Date> range = getDateRange(retentionLimit);
        String dateMask = getDateFormatInPath(feedBasePath);
        List<Path> toBeDeleted = discoverInstanceToDelete(feedBasePath,
                timeZone, dateMask, range.first, range.second);

        LOG.info("Applying retention on " + feedBasePath + " type: " +
                retentionType + ", Limit: " + retentionLimit + ", timezone: " +
                timeZone + ", frequency: " + frequency);

		DateFormat dateFormat = new SimpleDateFormat(format);
		dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		StringBuffer buffer = new StringBuffer();
		StringBuffer instancePaths = new StringBuffer("instancePaths=");
		for (Path path : toBeDeleted) {
			if (deleteInstance(path)) {
				LOG.info("Deleted instance " + path);
				Date date = getDate(path, feedBasePath, dateMask, timeZone);
				buffer.append(dateFormat.format(date)).append(',');
				instancePaths.append(path).append(",");
			}
		}
		
		logInstancePaths(new Path(logFile),instancePaths.toString());
	
		int len = buffer.length();
        if (len > 0) {
            stream.println("instances=" + buffer.substring(0, len -1));
        } else {
            stream.println("instances=NULL");
        }

        return 0;
    }

    private void logInstancePaths(Path path, String instancePaths) throws IOException {
		LOG.info("Writing deleted instances to path " + path);
		OutputStream out = fs.create(path);
		out.write(instancePaths.getBytes());
		out.close();
		if(LOG.isDebugEnabled()){
			debug(path, fs);
		}
	}

	private void printUsage() {
        LOG.info("Usage: org.apache.ivory.retention.FeedEvictor " +
                "<feedBasePath> <instance|age> <limit> <timezone> <frequency> <logDir> <timeStamp>");
        LOG.info("\tfeedBasePath: ex /data/feed/${YEAR}-${MONTH}");
        LOG.info("\tlimit: ex hours(5), months(2), days(90)");
        LOG.info("\ttimezone: ex UTC");
        LOG.info("\tfrequency: ex hourly, daily, monthly, minute, weekly, yearly");
        LOG.info("\tlogDir: ex /ivory/staging/feed");
        LOG.info("\ttimeStamp: ex 2012-11-28-14-00");
    }

    private Pair<Date, Date> getDateRange(String period) throws ELException {
        Long duration = (Long) EVALUATOR.evaluate("${" + period + "}",
                Long.class, resolver, resolver);
        Date end = new Date();
        Date start = new Date(end.getTime() - duration);
        return Pair.of(start, end);
    }

    private List<Path> discoverInstanceToDelete(String inPath, String timeZone,
                                                String dateMask, Date start,
                                                Date end)
            throws IOException {

        FileStatus[] files = findFilesForFeed(inPath);
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }

        List<Path> toBeDeleted = new ArrayList<Path>();
        for (FileStatus file : files) {
            Date date = getDate(new Path(file.getPath().toUri().getPath()),
                    inPath, dateMask, timeZone);
            LOG.debug("Considering " + file.getPath().toUri().getPath());
            LOG.debug("Date : " + date);
            if (date != null && !isDateInRange(date, start, end)) {
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

    private FileStatus[] findFilesForFeed(String feedBasePath)
            throws IOException {

        Matcher matcher = pattern.matcher(feedBasePath);
        while (matcher.find()) {
            String var = feedBasePath.substring(matcher.start(), matcher.end());
            feedBasePath = feedBasePath.replaceAll(Pattern.quote(var), "*");
            matcher = pattern.matcher(feedBasePath);
        }
        LOG.info("Searching for " + feedBasePath);
        return fs.globStatus(new Path(feedBasePath));
    }

    private String extractDatePartFromPathMask(String mask, String inPath) {
        String[] elements = pattern.split(mask);

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
            DateFormat dateFormat = new SimpleDateFormat(format.
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
        Matcher matcher = dateFieldPattern.matcher(mask);
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

    private boolean isDateInRange(Date date, Date start, Date end) {
        return date.compareTo(start) >= 0 && date.compareTo(end) <= 0;
    }

    private boolean deleteInstance(Path path) throws IOException {
        return fs.delete(path, true);
    }
    
	private void debug(Path outPath, FileSystem fs) throws IOException {
		ByteArrayOutputStream writer = new ByteArrayOutputStream();
		InputStream instance = fs.open(outPath);
		IOUtils.copyBytes(instance, writer, 4096, true);
		LOG.debug("Instance Paths copied to " + outPath );
		LOG.debug("Written "+writer);
	}
}

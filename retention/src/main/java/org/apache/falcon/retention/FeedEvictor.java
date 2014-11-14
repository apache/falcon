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
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.workflow.util.OozieActionConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Feed Evictor is called only if the retention policy that applies
 * to the feed is that of delete.
 */
public class FeedEvictor extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(FeedEvictor.class);

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    public static void main(String[] args) throws Exception {
        Configuration conf = OozieActionConfigurationHelper.createActionConf();
        int ret = ToolRunner.run(conf, new FeedEvictor(), args);
        if (ret != 0) {
            throw new Exception("Unable to perform eviction action args: " + Arrays.toString(args));
        }
    }

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

        Storage storage = FeedHelper.createStorage(feedStorageType, feedPattern, getConf());
        Path path = new Path(logFile);
        StringBuilder buffer = storage.evict(retentionLimit, timeZone, path);

        int len = buffer.length();
        if (len > 0) {
            OUT.get().println("instances=" + buffer.substring(0, len - 1));
        } else {
            OUT.get().println("instances=NULL");
        }

        return 0;
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
}

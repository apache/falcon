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
package org.apache.ivory.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FeedReplicator extends Configured implements Tool {

	private static Logger LOG = Logger.getLogger(FeedReplicator.class);

    public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new FeedReplicator(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

        DistCpOptions options = getDistCpOptions(args);
        
        Configuration conf = this.getConf();
		// inject wf configs
		Path confPath = new Path("file:///"
				+ System.getProperty("oozie.action.conf.xml"));

		LOG.info(confPath + " found conf ? "
				+ confPath.getFileSystem(conf).exists(confPath));
		conf.addResource(confPath);
        
		DistCp distCp = new CustomReplicator(conf, options);
		LOG.info("Started DistCp");
		distCp.execute();

        Path targetPath = options.getTargetPath();
        FileSystem fs = targetPath.getFileSystem(getConf());
        List<Path> inPaths = options.getSourcePaths();
        assert inPaths.size() == 1 : "Source paths more than 1 can't be handled";

        Path sourcePath = inPaths.get(0);
        Path includePath = new Path(getConf().get("ivory.include.path"));
        assert includePath.toString().substring(0, sourcePath.toString().length()).
                equals(sourcePath.toString()) : "Source path is not a subset of include path";

        String relativePath = includePath.toString().substring(sourcePath.toString().length());
        String fixedPath = getFixedPath(relativePath);

        FileStatus[] files = fs.globStatus(new Path(targetPath.toString() + "/" + fixedPath));
		if (files != null) {
			for (FileStatus file : files) {
            fs.create(new Path(file.getPath(), FileOutputCommitter.SUCCEEDED_FILE_NAME)).close();
            LOG.info("Created " + new Path(file.getPath(), FileOutputCommitter.SUCCEEDED_FILE_NAME));
			}
		} else {
			LOG.info("No files present in path: "
					+ new Path(targetPath.toString() + "/" + fixedPath)
							.toString());
		}
		LOG.info("Completed DistCp");
		return 0;
	}

    private String getFixedPath(String relativePath) throws IOException {
        String[] patterns = relativePath.split("/");
        int part = patterns.length - 1;
        for (int index = patterns.length - 1; index >= 0; index--) {
            String pattern = patterns[index];
            if (pattern.isEmpty()) continue;
            Pattern r = FilteredCopyListing.getRegEx(pattern);
            if (!r.toString().equals("(" + pattern + "/)|(" + pattern + "$)"))  {
                continue;
            }
            part = index;
            break;
        }
        String result = "";
        for (int index = 0; index <= part; index++) {
            result += (patterns[index] + "/");
        }
        return result.substring(0, result.lastIndexOf('/'));
    }

    public DistCpOptions getDistCpOptions(String[] args) throws ParseException {
		Options options = new Options();
		Option opt;
		opt = new Option("update", false,
				"specify update for synching folders");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("blocking", true,
				"should DistCp be running in blocking mode");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("maxMaps", true,
				"max number of maps to use for this copy");
		opt.setRequired(true);
		options.addOption(opt);

        opt = new Option("sourcePaths", true,
				"comma separtated list of source paths to be copied");
		opt.setRequired(true);
		options.addOption(opt);

        opt = new Option("targetPath", true, "target path");
		opt.setRequired(true);
		options.addOption(opt);

		CommandLine cmd = new GnuParser().parse(options, args);
		String[] paths = cmd.getOptionValue("sourcePaths").trim().split(",");
		List<Path> srcPaths = getPaths(paths);
		String trgPath = cmd.getOptionValue("targetPath").trim();

		DistCpOptions distcpOptions = new DistCpOptions(srcPaths, new Path(
				trgPath));
        distcpOptions.setSyncFolder(true);
		distcpOptions.setBlocking(Boolean.valueOf(cmd
				.getOptionValue("blocking")));
		distcpOptions
				.setMaxMaps(Integer.valueOf(cmd.getOptionValue("maxMaps")));

		return distcpOptions;
	}

	private List<Path> getPaths(String[] paths) {
		List<Path> listPaths = new ArrayList<Path>();
		for (String path : paths) {
			listPaths.add(new Path(path));
		}
		return listPaths;
	}
}

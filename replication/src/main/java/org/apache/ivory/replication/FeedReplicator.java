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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
		DistCp distCp = new CustomReplicator(this.getConf(), options);
		LOG.info("Started DistCp");
		distCp.execute();
		LOG.info("Completed DistCp");
		return 0;

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

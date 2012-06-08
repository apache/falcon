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

package org.apache.ivory.latedata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class LateDataHandler extends Configured implements Tool {

	private static Logger LOG = Logger.getLogger(LateDataHandler.class);

	static PrintStream stream = System.out;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path confPath = new Path("file:///"
				+ System.getProperty("oozie.action.conf.xml"));

		LOG.info(confPath + " found ? "
				+ confPath.getFileSystem(conf).exists(confPath));
		conf.addResource(confPath);
		ToolRunner.run(new Configuration(), new LateDataHandler(), args);
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
		opt = new Option("ivoryInputFeeds", true,
				"Input feed names, further separated by #");
		opt.setRequired(true);
		options.addOption(opt);

		return new GnuParser().parse(options, args);
	}

	@Override
	public int run(String[] args) throws Exception {

		CommandLine command = getCommand(args);

		Path file = new Path(command.getOptionValue("out"));
		Map<String, Long> map = new LinkedHashMap<String, Long>();
		String[] pathGroups = command.getOptionValue("paths").split("#");
		String[] inputFeeds = command.getOptionValue("ivoryInputFeeds").split(
				"#");
		for (int index = 0; index < pathGroups.length; index++) {
			long usage = 0;
			for (String pathElement : pathGroups[index].split(",")) {
				Path inPath = new Path(pathElement);
				usage += usage(inPath, getConf());
			}
			map.put(inputFeeds[index], usage);
		}
		LOG.info("MAP data: " + map);

		OutputStream out = file.getFileSystem(getConf()).create(file);
		for (Map.Entry<String, Long> entry : map.entrySet()) {
			out.write((entry.getKey() + "=" + entry.getValue() + "\n")
					.getBytes());
		}
		out.close();
		return 0;
	}

	public String detectChanges(Path file, Map<String, Long> map, Configuration conf)
			throws Exception {

		StringBuffer buffer = new StringBuffer();
		BufferedReader in = new BufferedReader(new InputStreamReader(file
				.getFileSystem(conf).open(file)));
		String line;
		try {
			Map<String, Long> recorded = new LinkedHashMap<String, Long>();
			while ((line = in.readLine()) != null) {
				if (line.isEmpty())
					continue;
				int index = line.indexOf('=');
				String key = line.substring(0, index);
				long size = Long.parseLong(line.substring(index + 1));
				recorded.put(key, size);
			}

			for (Map.Entry<String, Long> entry : map.entrySet()) {
				if (recorded.get(entry.getKey()) == null) {
					LOG.info("No matching key " + entry.getKey());
					continue;
				}
				if (recorded.get(entry.getKey()) != entry.getValue()) {
					LOG.info("Found path to be different for " + entry.getKey());
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

	public long usage(Path inPath, Configuration conf) throws IOException {
		FileSystem fs = inPath.getFileSystem(conf);
		FileStatus status[] = fs.globStatus(inPath);
		if (status == null || status.length == 0) {
			return 0;
		}
		long totalSize = 0;
		for (FileStatus statu : status) {
			totalSize += fs.getContentSummary(statu.getPath()).getLength();
		}
		return totalSize;
	}
}

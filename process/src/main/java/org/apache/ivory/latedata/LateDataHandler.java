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

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.ExternalId;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CustomOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class LateDataHandler extends Configured implements Tool {

    private static Logger LOG = Logger.getLogger(LateDataHandler.class);

    private enum Mode {record, detect}

    static PrintStream stream = System.out;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path confPath = new Path("file:///" +
                System.getProperty("oozie.action.conf.xml"));

        LOG.info(confPath + " found ? " +
                confPath.getFileSystem(conf).exists(confPath));
        conf.addResource(confPath);
        ToolRunner.run(new Configuration(), new LateDataHandler(), args);
    }

    private static CommandLine getCommand(String[] args)
            throws ParseException {
        Options options = new Options();
        Option opt = new Option("mode", true, "Mode. Can be record | detect");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("out", true, "Out file name");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("paths", true, "Comma separated path list, further separated by #");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("oozie", true, "Oozie Url");
        opt.setRequired(true);
        options.addOption(opt);
        opt = new Option("extid", true, "oozie external id");
        opt.setRequired(true);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    @Override
    public int run(String[] args) throws Exception {

        CommandLine command = getCommand(args);

        Mode mode = Mode.valueOf(command.getOptionValue("mode"));
        String extIdStr = command.getOptionValue("extid");
        String oozieUrl = command.getOptionValue("oozie");
        if (!extIdStr.isEmpty()) {
            ExternalId externalId = new ExternalId(extIdStr);
            String errMesg = checkRunningInstances(oozieUrl, externalId);
            if (errMesg != null) {
                if (mode == Mode.detect) {
                    LOG.warn(errMesg);
                    captureOutput("changedPaths=INVALID");
                    return 0;
                } else {
                    throw new IvoryException(errMesg);
                }
            }
        }

        Path file = new Path(command.getOptionValue("out"));
        Map<String, Long> map = new LinkedHashMap<String, Long>();
        String[] pathGroups = command.getOptionValue("paths").split("#");
        for (int index = 0; index < pathGroups.length; index++) {
            long usage = 0;
            for (String pathElement : pathGroups[index].split(",")) {
                Path inPath = new Path(pathElement);
                usage += usage(inPath);
            }
            map.put("Path" + (index + 1), usage);
        }
        LOG.info("MAP data: " + map);
        if (mode == Mode.record) {
            OutputStream out = file.getFileSystem(getConf()).create(file);
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                out.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes());
            }
            out.close();
        } else {
            if (!file.getFileSystem(getConf()).exists(file)) {
                LOG.warn(file + " is not found. Nothing to do");
                captureOutput("changedPaths=INVALID");
                return 0;
            }
            captureOutput("changedPaths=" + detectChanges(file, map));
        }
        return 0;
    }

    private String checkRunningInstances(String oozieUrl, ExternalId externalId)
            throws Exception {

        Tag runningTag = externalId.getTag();

        for (Tag tag : Tag.values()) {
            if (tag != runningTag && tag.getType() == runningTag.getType()) {
                ExternalId newId = new ExternalId(externalId.getName(), tag, externalId.getDate());
                OozieClient client = new CustomOozieClient(oozieUrl);

                String jobId = client.getJobId(newId.getId());
                if (jobId == null || jobId.trim().isEmpty()) continue;

                WorkflowJob job = client.getJobInfo(jobId);
                if (job == null) continue;

                WorkflowJob.Status status = job.getStatus();
                if (!(status == WorkflowJob.Status.SUCCEEDED ||
                        status == WorkflowJob.Status.FAILED ||
                        status == WorkflowJob.Status.KILLED)) {
                    return "Workflow for " + newId.getId() +
                            "(" + jobId + ") is in " + status + " state." ;
                }
            }
        }
        return null;
    }

    private void captureOutput(String keyValue) throws IOException {
        String fileName = System.getProperty("oozie.action.output.properties");
        if (fileName != null && !fileName.isEmpty()) {
            File file = new File(fileName);
            FileOutputStream out = new FileOutputStream(file);
            out.write(keyValue.getBytes());
            out.write('\n');
            out.close();
        }
        stream.println(keyValue);
    }

    private String detectChanges(Path file, Map<String, Long> map) throws Exception {
        StringBuffer buffer = new StringBuffer();
        BufferedReader in = new BufferedReader(new
                InputStreamReader(file.getFileSystem(getConf()).open(file)));
        String line;
        int lines = 0;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty()) continue;
            LOG.info("Processing line " + line);
            lines++;
            int index = line.indexOf('=');
            String key = line.substring(0, index);
            long size = Long.parseLong(line.substring(index+1));
            if (map.get(key) == null) {
                throw new NoSuchElementException("No matching key " + line);
            }
            if (map.get(key) != size) {
                LOG.info("Found path to be different for " + key);
                buffer.append(key).append(',');
            }
        }
        in.close();
        if (lines != map.size()) {
            throw new NotEnoughPathsException("Found fewer paths " + lines);
        }
        if (buffer.length() == 0) {
            return "";
        } else {
            return buffer.substring(0, buffer.length() - 1);
        }
    }

    private long usage(Path inPath) throws IOException {
        FileSystem fs = inPath.getFileSystem(getConf());
        FileStatus status[] = fs.globStatus(inPath);
        if (status==null || status.length==0) {
            return 0;
        }
        long totalSize = 0;
        for (FileStatus statu : status) {
            totalSize += fs.getContentSummary(statu.getPath()).getLength();
        }
        return totalSize;
    }
}

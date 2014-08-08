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

package org.apache.falcon.metadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility called in the post process of oozie workflow to record lineage information.
 */
@Deprecated // delete this class
public class LineageRecorder  extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(LineageRecorder.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new LineageRecorder(), args);
    }

    @Override
    public int run(String[] arguments) throws Exception {
        CommandLine command = getCommand(arguments);

        LOG.info("Parsing lineage metadata from: {}", command);
        Map<String, String> lineageMetadata = getLineageMetadata(command);
        LOG.info("Lineage Metadata: {}", lineageMetadata);

        String lineageFile = getFilePath(command.getOptionValue(LineageArgs.LOG_DIR.getOptionName()),
                command.getOptionValue(LineageArgs.ENTITY_NAME.getOptionName())
        );

        LOG.info("Persisting lineage metadata to: {}", lineageFile);
        persistLineageMetadata(lineageMetadata, lineageFile);

        return 0;
    }

    protected static CommandLine getCommand(String[] arguments) throws ParseException {

        Options options = new Options();

        for (LineageArgs arg : LineageArgs.values()) {
            addOption(options, arg);
        }

        return new GnuParser().parse(options, arguments);
    }

    private static void addOption(Options options, LineageArgs arg) {
        addOption(options, arg, true);
    }

    private static void addOption(Options options, LineageArgs arg, boolean isRequired) {
        Option option = arg.getOption();
        option.setRequired(isRequired);
        options.addOption(option);
    }

    protected Map<String, String> getLineageMetadata(CommandLine command) {
        Map<String, String> lineageMetadata = new HashMap<String, String>();

        for (LineageArgs arg : LineageArgs.values()) {
            lineageMetadata.put(arg.getOptionName(), arg.getOptionValue(command));
        }

        return lineageMetadata;
    }

    public static String getFilePath(String logDir, String entityName) {
        return logDir + entityName + "-lineage.json";
    }

    /**
     * this method is invoked from with in the workflow.
     *
     * @param lineageMetadata metadata to persist
     * @param lineageFile file to serialize the metadata
     * @throws IOException
     * @throws FalconException
     */
    protected void persistLineageMetadata(Map<String, String> lineageMetadata,
                                          String lineageFile) throws IOException, FalconException {
        OutputStream out = null;
        Path file = new Path(lineageFile);
        try {
            FileSystem fs = HadoopClientFactory.get().createFileSystem(file.toUri(), getConf());
            out = fs.create(file);

            // making sure falcon can read this file
            FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fs.setPermission(file, permission);

            out.write(JSONValue.toJSONString(lineageMetadata).getBytes());
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

    public static Map<String, String> parseLineageMetadata(String lineageFile) throws FalconException {
        try {
            Path lineageDataPath = new Path(lineageFile); // file has 777 permissions
            FileSystem fs = HadoopClientFactory.get().createFileSystem(lineageDataPath.toUri());

            BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(lineageDataPath)));
            return (Map<String, String>) JSONValue.parse(in);
        } catch (IOException e) {
            throw new FalconException("Error opening lineage file", e);
        }
    }
}

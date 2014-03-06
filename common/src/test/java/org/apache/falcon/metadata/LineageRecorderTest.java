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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * Unit test for lineage recorder.
 */
public class LineageRecorderTest {

    private static final String LOGS_DIR = "target/log";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";
    private static final String ENTITY_NAME = "test-process-entity";

    private String[] args;

    @BeforeMethod
    public void setUp() throws Exception {
        args = new String[]{
            "-" + LineageArgs.ENTITY_NAME.getOptionName(), ENTITY_NAME,
            "-" + LineageArgs.FEED_NAMES.getOptionName(), "out-click-logs,out-raw-logs",
            "-" + LineageArgs.FEED_INSTANCE_PATHS.getOptionName(),
            "/out-click-logs/10/05/05/00/20,/out-raw-logs/10/05/05/00/20",
            "-" + LineageArgs.WORKFLOW_ID.getOptionName(), "workflow-01-00",
            "-" + LineageArgs.WORKFLOW_USER.getOptionName(), "falcon",
            "-" + LineageArgs.RUN_ID.getOptionName(), "1",
            "-" + LineageArgs.NOMINAL_TIME.getOptionName(), NOMINAL_TIME,
            "-" + LineageArgs.TIMESTAMP.getOptionName(), "2014-01-01-01-00",
            "-" + LineageArgs.ENTITY_TYPE.getOptionName(), ("process"),
            "-" + LineageArgs.OPERATION.getOptionName(), ("GENERATE"),
            "-" + LineageArgs.STATUS.getOptionName(), ("SUCCEEDED"),
            "-" + LineageArgs.CLUSTER.getOptionName(), "corp",
            "-" + LineageArgs.WF_ENGINE_URL.getOptionName(), "http://localhost:11000/oozie/",
            "-" + LineageArgs.LOG_DIR.getOptionName(), LOGS_DIR,
            "-" + LineageArgs.USER_SUBFLOW_ID.getOptionName(), "userflow@wf-id" + "test",
            "-" + LineageArgs.USER_WORKFLOW_ENGINE.getOptionName(), "oozie",
            "-" + LineageArgs.INPUT_FEED_NAMES.getOptionName(), "in-click-logs,in-raw-logs",
            "-" + LineageArgs.INPUT_FEED_PATHS.getOptionName(),
            "/in-click-logs/10/05/05/00/20,/in-raw-logs/10/05/05/00/20",
            "-" + LineageArgs.USER_WORKFLOW_NAME.getOptionName(), "test-workflow",
            "-" + LineageArgs.USER_WORKFLOW_VERSION.getOptionName(), "1.0.0",
        };
    }

    @AfterMethod
    public void tearDown() throws Exception {

    }

    @Test
    public void testMain() throws Exception {
        LineageRecorder lineageRecorder = new LineageRecorder();
        CommandLine command = LineageRecorder.getCommand(args);
        Map<String, String> lineageMetadata = lineageRecorder.getLineageMetadata(command);

        LineageRecorder.main(args);

        String lineageFile = LineageRecorder.getFilePath(LOGS_DIR, ENTITY_NAME);
        Path lineageDataPath = new Path(lineageFile);
        FileSystem fs = lineageDataPath.getFileSystem(new Configuration());
        Assert.assertTrue(fs.exists(lineageDataPath));

        Map<String, String> recordedLineageMetadata =
                LineageRecorder.parseLineageMetadata(lineageFile);

        for (Map.Entry<String, String> entry : lineageMetadata.entrySet()) {
            Assert.assertEquals(lineageMetadata.get(entry.getKey()),
                    recordedLineageMetadata.get(entry.getKey()));
        }
    }

    @Test
    public void testGetFilePath() throws Exception {
        String path = LOGS_DIR + ENTITY_NAME + "-lineage.json";
        Assert.assertEquals(path, LineageRecorder.getFilePath(LOGS_DIR, ENTITY_NAME));
    }
}

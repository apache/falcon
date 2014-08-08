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

package org.apache.falcon.workflow;

import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.process.EngineType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * A test for WorkflowExecutionContext.
 */
public class WorkflowExecutionContextTest {

    private static final String FALCON_USER = "falcon-user";
    private static final String LOGS_DIR = "target/log";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";
    private static final String OPERATION = "GENERATE";

    private static final String CLUSTER_NAME = "primary-cluster";
    private static final String ENTITY_NAME = "sample-process";
    private static final String WORKFLOW_NAME = "imp-click-join-workflow";
    private static final String WORKFLOW_VERSION = "1.0.9";

    private static final String INPUT_FEED_NAMES = "impression-feed#clicks-feed";
    private static final String INPUT_INSTANCE_PATHS =
            "jail://global:00/falcon/impression-feed/2014/01/01,jail://global:00/falcon/impression-feed/2014/01/02"
                    + "#jail://global:00/falcon/clicks-feed/2014-01-01";

    private static final String OUTPUT_FEED_NAMES = "imp-click-join1,imp-click-join2";
    private static final String OUTPUT_INSTANCE_PATHS =
            "jail://global:00/falcon/imp-click-join1/20140101,jail://global:00/falcon/imp-click-join2/20140101";

    private static final String BROKER = "org.apache.activemq.ActiveMQConnectionFactory";

    private static final String ISO8601_TIME = SchemaHelper.formatDateUTCToISO8601(
            NOMINAL_TIME, WorkflowExecutionContext.PROCESS_INSTANCE_FORMAT);

    private WorkflowExecutionContext context;

    @BeforeMethod
    public void setUp() throws Exception {
        context = WorkflowExecutionContext.create(getTestMessageArgs(),
                WorkflowExecutionContext.Type.POST_PROCESSING);
    }

    @Test
    public void testGetValue() throws Exception {
        Assert.assertEquals(context.getValue(WorkflowExecutionArgs.ENTITY_NAME), ENTITY_NAME);
    }

    @Test
    public void testGetValueWithDefault() throws Exception {
        Assert.assertEquals(context.getValue(WorkflowExecutionArgs.TOPIC_NAME, "ABSENT"), "ABSENT");
    }

    @Test
    public void testContainsKey() throws Exception {
        Assert.assertTrue(context.containsKey(WorkflowExecutionArgs.ENTITY_NAME));
        Assert.assertFalse(context.containsKey(WorkflowExecutionArgs.TOPIC_NAME));
    }

    @Test
    public void testEntrySet() throws Exception {
        Assert.assertNotNull(context.entrySet());
    }

    @Test
    public void testHasWorkflowSucceeded() throws Exception {
        Assert.assertTrue(context.hasWorkflowSucceeded());
    }

    @Test
    public void testHasWorkflowFailed() throws Exception {
        Assert.assertFalse(context.hasWorkflowFailed());
    }

    @Test
    public void testGetContextFile() throws Exception {
        Assert.assertEquals(context.getContextFile(),
                WorkflowExecutionContext.getFilePath(context.getLogDir(), context.getEntityName()));
    }

    @Test
    public void testGetLogDir() throws Exception {
        Assert.assertEquals(context.getLogDir(), LOGS_DIR);
    }

    @Test
    public void testGetLogFile() throws Exception {
        Assert.assertEquals(context.getLogFile(), LOGS_DIR + "/log.txt");
    }

    @Test
    public void testGetNominalTime() throws Exception {
        Assert.assertEquals(context.getNominalTime(), NOMINAL_TIME);
    }

    @Test
    public void testGetNominalTimeAsISO8601() throws Exception {
        Assert.assertEquals(context.getNominalTimeAsISO8601(), ISO8601_TIME);
    }

    @Test
    public void testGetTimestamp() throws Exception {
        Assert.assertEquals(context.getTimestamp(), NOMINAL_TIME);
    }

    @Test
    public void testGetTimeStampAsISO8601() throws Exception {
        Assert.assertEquals(context.getTimeStampAsISO8601(), ISO8601_TIME);
    }

    @Test
    public void testGetClusterName() throws Exception {
        Assert.assertEquals(context.getClusterName(), CLUSTER_NAME);
    }

    @Test
    public void testGetEntityName() throws Exception {
        Assert.assertEquals(context.getEntityName(), ENTITY_NAME);
    }

    @Test
    public void testGetEntityType() throws Exception {
        Assert.assertEquals(context.getEntityType(), "process");
    }

    @Test
    public void testGetOperation() throws Exception {
        Assert.assertEquals(context.getOperation().name(), OPERATION);
    }

    @Test
    public void testGetOutputFeedNames() throws Exception {
        Assert.assertEquals(context.getOutputFeedNames(), OUTPUT_FEED_NAMES);
    }

    @Test
    public void testGetOutputFeedNamesList() throws Exception {
        Assert.assertEquals(context.getOutputFeedNamesList(),
                OUTPUT_FEED_NAMES.split(WorkflowExecutionContext.OUTPUT_FEED_SEPARATOR));
    }

    @Test
    public void testGetOutputFeedInstancePaths() throws Exception {
        Assert.assertEquals(context.getOutputFeedInstancePaths(), OUTPUT_INSTANCE_PATHS);
    }

    @Test
    public void testGetOutputFeedInstancePathsList() throws Exception {
        Assert.assertEquals(context.getOutputFeedInstancePathsList(),
                OUTPUT_INSTANCE_PATHS.split(","));
    }

    @Test
    public void testGetInputFeedNames() throws Exception {
        Assert.assertEquals(context.getOutputFeedNames(), OUTPUT_FEED_NAMES);
    }

    @Test
    public void testGetInputFeedNamesList() throws Exception {
        Assert.assertEquals(context.getInputFeedNamesList(),
                INPUT_FEED_NAMES.split(WorkflowExecutionContext.INPUT_FEED_SEPARATOR));
    }

    @Test
    public void testGetInputFeedInstancePaths() throws Exception {
        Assert.assertEquals(context.getInputFeedInstancePaths(), INPUT_INSTANCE_PATHS);
    }

    @Test
    public void testGetInputFeedInstancePathsList() throws Exception {
        Assert.assertEquals(context.getInputFeedInstancePathsList(),
                INPUT_INSTANCE_PATHS.split("#"));
    }

    @Test
    public void testGetWorkflowEngineUrl() throws Exception {
        Assert.assertEquals(context.getWorkflowEngineUrl(), "http://localhost:11000/oozie");
    }

    @Test
    public void testGetUserWorkflowEngine() throws Exception {
        Assert.assertEquals(context.getUserWorkflowEngine(), EngineType.PIG.name());
    }

    @Test
    public void testGetUserWorkflowVersion() throws Exception {
        Assert.assertEquals(context.getUserWorkflowVersion(), WORKFLOW_VERSION);
    }

    @Test
    public void testGetWorkflowId() throws Exception {
        Assert.assertEquals(context.getWorkflowId(), "workflow-01-00");
    }

    @Test
    public void testGetUserSubflowId() throws Exception {
        Assert.assertEquals(context.getUserSubflowId(), "userflow@wf-id");
    }

    @Test
    public void testGetWorkflowRunId() throws Exception {
        Assert.assertEquals(context.getWorkflowRunId(), 1);
    }

    @Test
    public void testGetWorkflowRunIdString() throws Exception {
        Assert.assertEquals(context.getWorkflowRunIdString(), "1");
    }

    @Test
    public void testGetWorkflowUser() throws Exception {
        Assert.assertEquals(context.getWorkflowUser(), FALCON_USER);
    }

    @Test
    public void testGetExecutionCompletionTime() throws Exception {
        Assert.assertNotNull(context.getExecutionCompletionTime());
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        String contextFile = context.getContextFile();
        context.serialize();
        WorkflowExecutionContext newContext = WorkflowExecutionContext.deSerialize(contextFile);
        Assert.assertNotNull(newContext);
        Assert.assertEquals(newContext.entrySet().size(), context.entrySet().size());
    }

    @Test
    public void testSerializeDeserializeWithFile() throws Exception {
        String contextFile = "/tmp/blah.json";
        context.serialize(contextFile);
        WorkflowExecutionContext newContext = WorkflowExecutionContext.deSerialize(contextFile);
        Assert.assertNotNull(newContext);
        Assert.assertEquals(newContext.entrySet().size(), context.entrySet().size());
    }

    @Test
    public void testGetFilePath() throws Exception {
        Assert.assertEquals(WorkflowExecutionContext.getFilePath(LOGS_DIR, ENTITY_NAME),
                LOGS_DIR + "/" + ENTITY_NAME + "-wf-post-exec-context.json");
    }

    private static String[] getTestMessageArgs() {
        return new String[]{
            "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), CLUSTER_NAME,
            "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), "process",
            "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), ENTITY_NAME,
            "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), NOMINAL_TIME,
            "-" + WorkflowExecutionArgs.OPERATION.getName(), OPERATION,

            "-" + WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), INPUT_FEED_NAMES,
            "-" + WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), INPUT_INSTANCE_PATHS,

            "-" + WorkflowExecutionArgs.FEED_NAMES.getName(), OUTPUT_FEED_NAMES,
            "-" + WorkflowExecutionArgs.FEED_INSTANCE_PATHS.getName(), OUTPUT_INSTANCE_PATHS,

            "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
            "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), FALCON_USER,
            "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
            "-" + WorkflowExecutionArgs.STATUS.getName(), "SUCCEEDED",
            "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), NOMINAL_TIME,

            "-" + WorkflowExecutionArgs.WF_ENGINE_URL.getName(), "http://localhost:11000/oozie",
            "-" + WorkflowExecutionArgs.USER_SUBFLOW_ID.getName(), "userflow@wf-id",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(), WORKFLOW_NAME,
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_VERSION.getName(), WORKFLOW_VERSION,
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(), EngineType.PIG.name(),

            "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER,
            "-" + WorkflowExecutionArgs.BRKR_URL.getName(), "tcp://localhost:61616?daemon=true",
            "-" + WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(), BROKER,
            "-" + WorkflowExecutionArgs.USER_BRKR_URL.getName(), "tcp://localhost:61616?daemon=true",
            "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "1000",

            "-" + WorkflowExecutionArgs.LOG_DIR.getName(), LOGS_DIR,
            "-" + WorkflowExecutionArgs.LOG_FILE.getName(), LOGS_DIR + "/log.txt",
        };
    }
}

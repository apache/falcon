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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Properties;

/**
 * A test for WorkflowJobEndNotificationService.
 */
public class WorkflowJobEndNotificationServiceTest implements WorkflowExecutionListener {

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

    private WorkflowJobEndNotificationService service;
    private WorkflowExecutionContext savedContext;

    @BeforeClass
    public void setUp() throws Exception {
        service = new WorkflowJobEndNotificationService();
        savedContext = WorkflowExecutionContext.create(getTestMessageArgs(),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        Assert.assertNotNull(savedContext);
        service.init();
        service.registerListener(this);
    }

    @AfterClass
    public void tearDown() throws Exception {
        service.destroy();
    }

    @Test
    public void testGetName() throws Exception {
        Assert.assertEquals(service.getName(), WorkflowJobEndNotificationService.SERVICE_NAME);
    }

    @Test(priority = -1)
    public void testBasic() throws Exception {
        try {
            notifyFailure(savedContext);
            notifySuccess(savedContext);
        } finally {
            StartupProperties.get().setProperty("workflow.execution.listeners", "");
        }
    }

    @Test
    public void testNotificationsFromEngine() throws FalconException {
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(),
                WorkflowExecutionContext.Type.WORKFLOW_JOB);

        // Pretend the start was already notified
        Properties wfProps = new Properties();
        wfProps.put(WorkflowExecutionArgs.CLUSTER_NAME.name(), CLUSTER_NAME);
        service.getContextMap().put("workflow-01-00", wfProps);

        // Should retrieve from cache.
        service.notifySuspend(context);
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        Assert.assertNotNull(context);
        Assert.assertEquals(context.entrySet().size(), 28);
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        Assert.assertNotNull(context);
        Assert.assertEquals(context.entrySet().size(), 28);
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {

    }

    private void notifyFailure(WorkflowExecutionContext context) throws FalconException {
        service.notifyFailure(context);
    }

    private void notifySuccess(WorkflowExecutionContext context) throws FalconException {
        service.notifySuccess(context);
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

            "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), OUTPUT_FEED_NAMES,
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), OUTPUT_INSTANCE_PATHS,

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
            "-" + WorkflowExecutionArgs.WF_START_TIME.getName(), Long.toString(new Date().getTime()),
            "-" + WorkflowExecutionArgs.WF_END_TIME.getName(), Long.toString(new Date().getTime() + 1000000),
        };
    }
}

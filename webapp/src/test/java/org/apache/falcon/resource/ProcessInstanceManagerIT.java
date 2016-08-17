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

package org.apache.falcon.resource;

import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.OozieTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;

/**
 * Test class for Process Instance REST API.
 */
@Test (enabled = false)
public class ProcessInstanceManagerIT extends AbstractSchedulerManagerJerseyIT {

    private static final String START_INSTANCE = "2012-04-20T00:00Z";
    private static final String SLEEP_WORKFLOW = "sleepWorkflow.xml";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        String version = System.getProperty("project.version");
        String buildDir = System.getProperty("project.build.directory");
        System.setProperty("falcon.libext", buildDir + "/../../unit/target/falcon-unit-" + version + ".jar");
        super.setup();
    }

    @AfterMethod
    @Override
    public void cleanUpActionXml() throws IOException, FalconException {
        //Needed since oozie writes action xml to current directory.
        FileUtils.deleteQuietly(new File("action.xml"));
        FileUtils.deleteQuietly(new File(".action.xml.crc"));
    }

    protected void schedule(TestContext context) throws Exception {
        CurrentUser.authenticate(System.getProperty("user.name"));
        schedule(context, 1);
    }

    protected void schedule(TestContext context, int count) throws Exception {
        for (int i=0; i<count; i++) {
            context.scheduleProcess();
        }
        OozieTestUtils.waitForProcessWFtoStart(context);
    }

    @Test (enabled = false)
    public void testGetRunningInstances() throws Exception {
        TestContext context = new TestContext();
        schedule(context);
        InstancesResult response = context.service.path("api/instance/running/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);

        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    @Test (enabled = false)
    public void testGetRunningInstancesPagination()  throws Exception {
        TestContext context = new TestContext();
        schedule(context, 4);
        InstancesResult response = context.service.path("api/instance/running/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        response = context.service.path("api/instance/running/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("offset", "0")
                .queryParam("numResults", "1")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    private void assertInstance(Instance processInstance, String instance, WorkflowStatus status) {
        Assert.assertNotNull(processInstance);
        Assert.assertNotNull(processInstance.getInstance());
        Assert.assertTrue(processInstance.getInstance().endsWith(instance));
        Assert.assertEquals(processInstance.getStatus(), status);
    }

    @Test (enabled = false)
    public void testGetInstanceStatus() throws Exception {
        UnitTestContext context = new UnitTestContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);
        String endTime = "2012-04-20T00:01Z";
        InstancesResult response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        Assert.assertEquals(response.getInstances()[0].getStatus(), WorkflowStatus.RUNNING);
    }

    @Test (enabled = false)
    public void testGetInstanceStatusPagination() throws Exception {
        UnitTestContext context = new UnitTestContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);
        String endTime = "2012-04-20T00:02Z";
        InstancesResult response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, "STATUS:RUNNING", "startTime",
                "", 0, new Integer(1), null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        Assert.assertEquals(response.getInstances()[0].getStatus(), WorkflowStatus.RUNNING);
    }

    @Test (enabled = false)
    public void testKillInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);
        String endTime = "2012-04-20T00:01Z";
        context.getClient().killInstances(EntityType.PROCESS.name(), context.processName, START_INSTANCE, endTime,
                context.colo, null, null, null, null);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.KILLED);

        InstancesResult response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(), context.processName,
                START_INSTANCE, endTime, context.colo, null, "STATUS:KILLED", "startTime", "", 0, 1, null, null);

        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        Assert.assertEquals(response.getInstances()[0].getStatus(), WorkflowStatus.KILLED);
    }

    @Test (enabled = false)
    public void testReRunInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);
        String endTime = "2012-04-20T00:01Z";
        context.getClient().killInstances(EntityType.PROCESS.name(), context.processName, START_INSTANCE, endTime,
                context.colo, null, null, null, null);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.KILLED);

        InstancesResult response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        context.getClient().rerunInstances(EntityType.PROCESS.name(), context.processName,
                START_INSTANCE, endTime, null, context.colo, null, null, null, true, null);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);

        response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        Assert.assertEquals(response.getInstances()[0].getStatus(), WorkflowStatus.RUNNING);
    }

    @Test (enabled = false)
    public void testSuspendInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);
        String endTime = "2012-04-20T00:01Z";
        context.getClient().suspendInstances(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                endTime, context.colo, context.clusterName, null, null, null);

        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.SUSPENDED);

        InstancesResult response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        Assert.assertEquals(response.getInstances()[0].getStatus(), WorkflowStatus.SUSPENDED);
    }

    @Test (enabled = false)
    public void testResumesInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);
        String endTime = "2012-04-20T00:01Z";
        context.getClient().suspendInstances(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                endTime, context.colo, context.clusterName, null, null, null);

        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.SUSPENDED);

        InstancesResult response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.SUSPENDED);

        context.getClient().resumeInstances(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                endTime, context.colo, context.clusterName, null, null, null);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE, WorkflowStatus.RUNNING);

        response = context.getClient().getStatusOfInstances(EntityType.PROCESS.name(),
                context.processName, START_INSTANCE, endTime, context.colo, null, null, "", "", 0, 1, null, null);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        Assert.assertEquals(response.getInstances()[0].getStatus(), WorkflowStatus.RUNNING);
    }
}

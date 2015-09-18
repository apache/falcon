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

import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ExternalId;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;

/**
 * Test class for Process Instance REST API.
 */
@Test(enabled = false, groups = {"exhaustive"})
public class ProcessInstanceManagerIT {
    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
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

    //@Test
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

    //@Test
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

    //@Test
    public void testGetInstanceStatus() throws Exception {
        TestContext context = new TestContext();
        schedule(context);
        InstancesResult response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    //@Test
    public void testGetInstanceStatusPagination() throws Exception {
        TestContext context = new TestContext();
        schedule(context, 4);

        InstancesResult response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("offset", "0")
                .queryParam("numResults", "1").queryParam("filterBy", "STATUS:RUNNING")
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    public void testReRunInstances() throws Exception {
        testKillInstances();
        TestContext context = new TestContext();
        InstancesResult response = context.service.path("api/instance/rerun/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);

        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.RUNNING);
    }

    public void testKillInstances() throws Exception {
        TestContext context = new TestContext();
        schedule(context);
        InstancesResult response = context.service.path("api/instance/kill/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        response = context.service.path("api/instance/kill/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.FAILED);
        Assert.assertNotNull(response.getMessage());

        response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("filterBy", "STATUS:KILLED")
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("filterBy", "STATUS:KILLED")
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.KILLED);
    }

    public void testSuspendInstances() throws Exception {
        TestContext context = new TestContext();
        schedule(context);
        InstancesResult response = context.service.path("api/instance/suspend/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.SUSPENDED);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.SUSPENDED);
    }

    public void testResumesInstances() throws Exception {
        testSuspendInstances();

        TestContext context = new TestContext();
        InstancesResult response = context.service.path("api/instance/resume/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(response.getInstances().length, 1);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.RUNNING);
    }

    private void waitForWorkflow(String instance, WorkflowJob.Status status) throws Exception {
        TestContext context = new TestContext();
        ExternalId extId = new ExternalId(context.processName, Tag.DEFAULT, EntityUtil.parseDateUTC(instance));
        OozieClient ozClient = OozieClientFactory.get(
                (Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, context.clusterName));
        String jobId = ozClient.getJobId(extId.getId());
        WorkflowJob jobInfo = null;
        for (int i = 0; i < 15; i++) {
            jobInfo = ozClient.getJobInfo(jobId);
            if (jobInfo.getStatus() == status) {
                break;
            }
            Thread.sleep((i + 1) * 1000);
        }

        Assert.assertNotNull(jobInfo);
        Assert.assertEquals(status, jobInfo.getStatus());
    }
}

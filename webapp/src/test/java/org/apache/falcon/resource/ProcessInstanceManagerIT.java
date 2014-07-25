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
import org.apache.falcon.util.OozieTestUtils;
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.oozie.client.ProxyOozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;

/**
 * Test class for Process Instance REST API.
 */
@Test(enabled = false, groups = {"exhaustive"})
public class ProcessInstanceManagerIT {
    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    protected void schedule(TestContext context) throws Exception {
        schedule(context, 1);
    }

    protected void schedule(TestContext context, int count) throws Exception {
        for (int i=0; i<count; i++) {
            context.scheduleProcess();
            Thread.sleep(500);
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
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
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
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(4, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        response = context.service.path("api/instance/running/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("offset", "2")
                .queryParam("numResults", "2")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(2, response.getInstances().length);
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
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    //@Test
    public void testGetInstanceStatusPagination() throws Exception {
        TestContext context = new TestContext();
        schedule(context, 4);

        InstancesResult response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("offset", "2")
                .queryParam("numResults", "2").queryParam("statusFilter", "RUNNING")
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(2, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("offset", "50")
                .queryParam("numResults", "2").queryParam("statusFilter", "RUNNING")
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(0, response.getInstances().length);


    }

    public void testReRunInstances() throws Exception {
        testKillInstances();
        TestContext context = new TestContext();
        InstancesResult response = context.service.path("api/instance/rerun/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);

        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.RUNNING);
    }

    //@Test
    public void testKillInstances() throws Exception {
        TestContext context = new TestContext();
        schedule(context);
        InstancesResult response = context.service.path("api/instance/kill/process/" + context.processName)
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        response = context.service.path("api/instance/status/process/" + context.processName)
                .queryParam("orderBy", "startTime").queryParam("statusFilter", "KILLED")
                .queryParam("start", START_INSTANCE)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
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
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
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
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.RUNNING);
    }

    private void waitForWorkflow(String instance, WorkflowJob.Status status) throws Exception {
        TestContext context = new TestContext();
        ExternalId extId = new ExternalId(context.processName, Tag.DEFAULT, EntityUtil.parseDateUTC(instance));
        ProxyOozieClient ozClient = OozieClientFactory.get(
                (Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, context.clusterName));
        String jobId = ozClient.getJobId(extId.getId());
        WorkflowJob jobInfo = null;
        for (int i = 0; i < 15; i++) {
            jobInfo = ozClient.getJobInfo(jobId);
            if (jobInfo.getStatus() == status) {
                break;
            }
            System.out.println("Waiting for workflow job " + jobId + " status " + status);
            Thread.sleep((i + 1) * 1000);
        }

        Assert.assertNotNull(jobInfo);
        Assert.assertEquals(status, jobInfo.getStatus());
    }
}

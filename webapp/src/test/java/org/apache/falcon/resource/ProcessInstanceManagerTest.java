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
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;

@Test(enabled = false)
public class ProcessInstanceManagerTest extends AbstractTestBase {
    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    protected void schedule() throws Exception {
        scheduleProcess();
        waitForProcessWFtoStart();
    }

    public void testGetRunningInstances() throws Exception {
        schedule();
        InstancesResult response = this.service.path("api/instance/running/process/" + processName)
                .header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON).get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    private void assertInstance(Instance processInstance, String instance, WorkflowStatus status) {
        Assert.assertNotNull(processInstance);
        Assert.assertNotNull(processInstance.getInstance());
        Assert.assertTrue(processInstance.getInstance().endsWith(instance));
        Assert.assertEquals(processInstance.getStatus(), status);
    }

    public void testGetInstanceStatus() throws Exception {
        schedule();
        InstancesResult response = this.service.path("api/instance/status/process/" + processName)
                .queryParam("start", START_INSTANCE).header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .get(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);
    }

    public void testReRunInstances() throws Exception {
        testKillInstances();

        InstancesResult response = this.service.path("api/instance/rerun/process/" + processName)
                .queryParam("start", START_INSTANCE).header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);

        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.RUNNING);
    }

    public void testKillInstances() throws Exception {
        schedule();
        InstancesResult response = this.service.path("api/instance/kill/process/" + processName)
                .queryParam("start", START_INSTANCE).header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.KILLED);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.KILLED);
    }

    public void testSuspendInstances() throws Exception {
        schedule();
        InstancesResult response = this.service.path("api/instance/suspend/process/" + processName)
                .queryParam("start", START_INSTANCE).header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.SUSPENDED);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.SUSPENDED);
    }

    public void testResumesInstances() throws Exception {
        testSuspendInstances();

        InstancesResult response = this.service.path("api/instance/resume/process/" + processName)
                .queryParam("start", START_INSTANCE).header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(InstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        assertInstance(response.getInstances()[0], START_INSTANCE, WorkflowStatus.RUNNING);

        waitForWorkflow(START_INSTANCE, WorkflowJob.Status.RUNNING);
    }

    private void waitForWorkflow(String instance, WorkflowJob.Status status) throws Exception {
        ExternalId extId = new ExternalId(processName, Tag.DEFAULT, EntityUtil.parseDateUTC(instance));
        OozieClient ozClient = OozieClientFactory.get(
                (Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, clusterName));
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
        Assert.assertEquals(status, jobInfo.getStatus());
    }
}

package org.apache.ivory.resource;

import javax.ws.rs.core.MediaType;

import org.apache.ivory.Tag;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.ivory.workflow.engine.OozieClientFactory;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test (enabled = false)
public class ProcessInstanceManagerTest extends AbstractTestBase {

    protected void schedule() throws Exception {
        scheduleProcess();
        waitForProcessStart();
    }

    public void testGetRunningInstances() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/running/" + processName)
                .header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON).get(ProcessInstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        Assert.assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
    }

    public void testGetInstanceStatus() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/status/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .get(ProcessInstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        Assert.assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        Assert.assertEquals(WorkflowStatus.RUNNING, response.getInstances()[0].getStatus());
    }

    public void testReRunInstances() throws Exception {
        testKillInstances();

        ProcessInstancesResult response = this.service.path("api/processinstance/rerun/" + processName)
                .queryParam("start", "2012-04-12T05:30Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);

        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        Assert.assertEquals("2012-04-12T05:30Z", response.getInstances()[0].getInstance());
        Assert.assertEquals(WorkflowStatus.RUNNING, response.getInstances()[0].getStatus());

        waitForWorkflow("2012-04-12T05:30Z", WorkflowJob.Status.RUNNING);
    }

    public void testKillInstances() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/kill/" + processName)
                .queryParam("start", "2012-04-12T05:30Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        Assert.assertEquals("2012-04-12T05:30Z", response.getInstances()[0].getInstance());
        Assert.assertEquals(WorkflowStatus.KILLED, response.getInstances()[0].getStatus());

        waitForWorkflow("2012-04-12T05:30Z", WorkflowJob.Status.KILLED);
    }

    public void testSuspendInstances() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/suspend/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        Assert.assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        Assert.assertEquals(WorkflowStatus.SUSPENDED, response.getInstances()[0].getStatus());

        waitForWorkflow("2010-01-01T01:00Z", WorkflowJob.Status.SUSPENDED);
    }

    public void testResumesInstances() throws Exception {
        testSuspendInstances();
        
        ProcessInstancesResult response = this.service.path("api/processinstance/resume/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        Assert.assertNotNull(response.getInstances());
        Assert.assertEquals(1, response.getInstances().length);
        Assert.assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        Assert.assertEquals(WorkflowStatus.RUNNING, response.getInstances()[0].getStatus());

        waitForWorkflow("2010-01-01T01:00Z", WorkflowJob.Status.RUNNING);
    }
    
    private void waitForWorkflow(String instance, WorkflowJob.Status status) throws Exception {
        ExternalId extId = new ExternalId(processName, Tag.DEFAULT, EntityUtil.parseDateUTC(instance));
        OozieClient ozClient = OozieClientFactory.get((Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, clusterName));
        String jobId = ozClient.getJobId(extId.getId());
        WorkflowJob jobInfo = null;
        for (int i = 0; i < 15; i++) {
            jobInfo = ozClient.getJobInfo(jobId);
            if (jobInfo.getStatus() == status)
                break;
            System.out.println("Waiting for workflow job " + jobId + " status " + status);
            Thread.sleep((i + 1) * 1000);
        }
        Assert.assertEquals(status, jobInfo.getStatus());
    }
}

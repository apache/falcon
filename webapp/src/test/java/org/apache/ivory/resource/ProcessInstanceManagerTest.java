package org.apache.ivory.resource;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Map.Entry;
import java.util.Properties;

import javax.ws.rs.core.MediaType;

import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.resource.ProcessInstancesResult.WorkflowStatus;
import org.apache.ivory.workflow.engine.OozieClientFactory;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.testng.annotations.Test;

@Test(enabled=true)
public class ProcessInstanceManagerTest extends AbstractTestBase {

    protected void schedule() throws Exception {
        scheduleProcess();
        waitForProcessStart();
    }

    public void testGetRunningInstances() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/running/" + processName)
                .header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON).get(ProcessInstancesResult.class);
        assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        assertNotNull(response.getInstances());
        assertEquals(1, response.getInstances().length);
        assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
    }

    public void testGetInstanceStatus() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/status/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .get(ProcessInstancesResult.class);
        assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        assertNotNull(response.getInstances());
        assertEquals(1, response.getInstances().length);
        assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        assertEquals(WorkflowStatus.RUNNING, response.getInstances()[0].getStatus());
    }

    public void testReRunInstances() throws Exception {
        testKillInstances();

        Properties props = new Properties();
        props.put(OozieClient.RERUN_FAIL_NODES, "true");

        StringBuffer buffer = new StringBuffer();
        for (Entry<Object, Object> entry : props.entrySet()) {
            buffer.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }

        this.service.type(MediaType.TEXT_PLAIN);
        ProcessInstancesResult response = this.service.path("api/processinstance/rerun/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class, buffer.toString());

        assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        assertNotNull(response.getInstances());
        assertEquals(1, response.getInstances().length);
        assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        assertEquals(WorkflowStatus.RUNNING, response.getInstances()[0].getStatus());

        waitForWorkflow("2010-01-01T01:00Z", WorkflowJob.Status.RUNNING);
    }

    public void testKillInstances() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/kill/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);
        assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        assertNotNull(response.getInstances());
        assertEquals(1, response.getInstances().length);
        assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        assertEquals(WorkflowStatus.KILLED, response.getInstances()[0].getStatus());

        waitForWorkflow("2010-01-01T01:00Z", WorkflowJob.Status.KILLED);
    }

    public void testSuspendInstances() throws Exception {
        schedule();
        ProcessInstancesResult response = this.service.path("api/processinstance/suspend/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);
        assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        assertNotNull(response.getInstances());
        assertEquals(1, response.getInstances().length);
        assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        assertEquals(WorkflowStatus.SUSPENDED, response.getInstances()[0].getStatus());

        waitForWorkflow("2010-01-01T01:00Z", WorkflowJob.Status.SUSPENDED);
    }

    public void testResumesInstances() throws Exception {
        testSuspendInstances();
        
        ProcessInstancesResult response = this.service.path("api/processinstance/resume/" + processName)
                .queryParam("start", "2010-01-01T01:00Z").header("Remote-User", "guest").accept(MediaType.APPLICATION_JSON)
                .post(ProcessInstancesResult.class);
        assertEquals(APIResult.Status.SUCCEEDED, response.getStatus());
        assertNotNull(response.getInstances());
        assertEquals(1, response.getInstances().length);
        assertEquals("2010-01-01T01:00Z", response.getInstances()[0].getInstance());
        assertEquals(WorkflowStatus.RUNNING, response.getInstances()[0].getStatus());

        waitForWorkflow("2010-01-01T01:00Z", WorkflowJob.Status.RUNNING);
    }
    
    private void waitForWorkflow(String instance, WorkflowJob.Status status) throws Exception {
        ExternalId extId = new ExternalId(processName, EntityUtil.parseDateUTC(instance));
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
        assertEquals(status, jobInfo.getStatus());
    }
}
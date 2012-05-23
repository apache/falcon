package org.apache.ivory.workflow.engine;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.resource.ProcessInstancesResult;
import org.apache.oozie.client.*;
import org.apache.oozie.client.Job.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OozieWorkflowEngineTest extends OozieWorkflowEngine {


    @Test
    public void testActiveCoords1() throws Exception {
        CoordinatorJob killed1 = new MockCoordJob("2012-01-01T05:00Z", "2012-01-01T05:01Z", Status.KILLED);
        CoordinatorJob good1 = new MockCoordJob("2012-01-01T05:02Z", "2012-01-01T05:04Z", Status.SUCCEEDED);
        List<CoordinatorJob> jobs = new ArrayList<CoordinatorJob>();
        jobs.add(good1);
        jobs.add(killed1);
        sortCoordsByStartTime(jobs);
        List<CoordinatorJob> finalJobs = getActiveCoords(jobs);
        Assert.assertTrue(finalJobs.containsAll(jobs));
    }

    @Test
    public void testActiveCoords2() throws Exception {
        CoordinatorJob good2 = new MockCoordJob("2012-01-01T05:00Z", "2012-01-01T05:01Z", Status.SUCCEEDED);
        CoordinatorJob good1 = new MockCoordJob("2012-01-01T05:02Z", "2012-01-01T05:04Z", Status.SUCCEEDED);
        List<CoordinatorJob> jobs = new ArrayList<CoordinatorJob>();
        jobs.add(good2);
        jobs.add(good1);
        sortCoordsByStartTime(jobs);
        List<CoordinatorJob> finalJobs = getActiveCoords(jobs);
        Assert.assertTrue(finalJobs.containsAll(jobs));
    }

    @Test
    public void testActiveCoords3() throws Exception {
        CoordinatorJob good3 = new MockCoordJob("2012-01-01T05:01Z", "2012-01-01T05:03Z", Status.KILLED);
        CoordinatorJob good2 = new MockCoordJob("2012-01-01T05:00Z", "2012-01-01T05:01Z", Status.SUCCEEDED);
        CoordinatorJob good1 = new MockCoordJob("2012-01-01T05:02Z", "2012-01-01T05:04Z", Status.SUCCEEDED);
        List<CoordinatorJob> jobs = new ArrayList<CoordinatorJob>();
        jobs.add(good2);
        jobs.add(good3);
        jobs.add(good1);
        sortCoordsByStartTime(jobs);
        List<CoordinatorJob> finalJobs = getActiveCoords(jobs);
        Assert.assertTrue(finalJobs.contains(good3));
        Assert.assertTrue(finalJobs.contains(good1));
        Assert.assertTrue(finalJobs.contains(good2));
    }

    @Test
    public void testActiveCoords4() throws Exception {
        CoordinatorJob good3 = new MockCoordJob("2012-01-01T05:01Z", "2012-01-01T05:03Z", Status.KILLED);
        CoordinatorJob good2 = new MockCoordJob("2012-01-01T05:00Z", "2012-01-01T05:02Z", Status.SUCCEEDED);
        CoordinatorJob good1 = new MockCoordJob("2012-01-01T05:02Z", "2012-01-01T05:04Z", Status.SUCCEEDED);
        List<CoordinatorJob> jobs = new ArrayList<CoordinatorJob>();
        jobs.add(good2);
        jobs.add(good3);
        jobs.add(good1);
        sortCoordsByStartTime(jobs);
        List<CoordinatorJob> finalJobs = getActiveCoords(jobs);
        Assert.assertFalse(finalJobs.contains(good3));
        Assert.assertTrue(finalJobs.contains(good1));
        Assert.assertTrue(finalJobs.contains(good2));
    }

    @Test (enabled =  false)
    public void getParentId() throws Exception {
        OozieClient client = new OozieClient("http://oozie.red.ua2.inmobi.com:11000/oozie");
        List<WorkflowJob> jobs = client.getJobsInfo("status=" + Job.Status.RUNNING, 1, 1000);
        for (WorkflowJob job : jobs) {
            WorkflowJob wf = client.getJobInfo(job.getId());
            if (StringUtils.isEmpty(wf.getParentId()))
                continue;
            if (!wf.getAppName().equals("IVORY_PROCESS_DEFAULT_download-summary")) continue;
            CoordinatorAction action = client.getCoordActionInfo(wf.getParentId());
            System.out.println(action.getJobId());
            System.out.println(action.getMissingDependencies());
            System.out.println(action.getRunConf());
            //System.out.println(action.getCreatedConf());
        }
    }

    private class MockCoordJob implements CoordinatorJob {

        private final Date start;
        private final Date end;
        private final Status status;

        private MockCoordJob(String start, String end, Status status)
                throws IvoryException {
            this.start = EntityUtil.parseDateUTC(start);
            this.end = EntityUtil.parseDateUTC(end);
            this.status = status;
        }

        private MockCoordJob(Date start, Date end, Status status) {
            this.start = start;
            this.end = end;
            this.status = status;
        }

        @Override
        public String getAppPath() {
            return null;
        }

        @Override
        public String getAppName() {
            return null;
        }

        @Override
        public String getId() {
            return null;
        }

        @Override
        public String getConf() {
            return null;
        }

        @Override
        public Status getStatus() {
            return status;
        }

        @Override
        public int getFrequency() {
            return 0;
        }

        @Override
        public Timeunit getTimeUnit() {
            return null;
        }

        @Override
        public String getTimeZone() {
            return null;
        }

        @Override
        public int getConcurrency() {
            return 0;
        }

        @Override
        public Execution getExecutionOrder() {
            return null;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

        @Override
        public Date getLastActionTime() {
            return null;
        }

        @Override
        public Date getNextMaterializedTime() {
            return null;
        }

        @Override
        public Date getStartTime() {
            return start;
        }

        @Override
        public Date getEndTime() {
            return end;
        }

        @Override
        public void setStatus(Status status) {
        }

        @Override
        public void setPending() {
        }

        @Override
        public void resetPending() {
        }

        @Override
        public Date getPauseTime() {
            return null;
        }

        @Override
        public String getExternalId() {
            return null;
        }

        @Override
        public String getUser() {
            return null;
        }

        @Override
        public String getGroup() {
            return null;
        }

        @Override
        public String getBundleId() {
            return null;
        }

        @Override
        public String getConsoleUrl() {
            return null;
        }

        @Override
        public List<CoordinatorAction> getActions() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockCoordJob that = (MockCoordJob) o;

            if (end != null ? !end.equals(that.end) : that.end != null) return false;
            if (start != null ? !start.equals(that.start) : that.start != null) return false;
            if (status != that.status) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = start != null ? start.hashCode() : 0;
            result = 31 * result + (end != null ? end.hashCode() : 0);
            result = 31 * result + (status != null ? status.hashCode() : 0);
            return result;
        }
    }
}

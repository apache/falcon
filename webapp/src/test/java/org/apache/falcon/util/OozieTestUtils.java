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

package org.apache.falcon.util;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.logging.JobLogMover;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.resource.UnitTestContext;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.falcon.workflow.engine.OozieWorkflowEngine;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.ProxyOozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Oozie Utility class for integration-tests.
 */
public final class OozieTestUtils {

    private OozieTestUtils() {
    }

    public static OozieClient getOozieClient(TestContext context) throws FalconException {
        return getOozieClient(context.getCluster().getCluster());
    }

    public static OozieClient getOozieClient(UnitTestContext context) throws FalconException {
        return OozieClientFactory.get(context.getClusterName());
    }

    public static OozieClient getOozieClient(Cluster cluster) throws FalconException {
        return OozieClientFactory.get(cluster);
    }

    public static List<BundleJob> getBundles(TestContext context) throws Exception {
        List<BundleJob> bundles = new ArrayList<BundleJob>();
        if (context.getClusterName() == null) {
            return bundles;
        }

        OozieClient ozClient = OozieClientFactory.get(context.getCluster().getCluster());
        return ozClient.getBundleJobsInfo("name=FALCON_PROCESS_" + context.getProcessName(), 0, 10);
    }

    public static List<BundleJob> getBundles(UnitTestContext context) throws Exception {
        List<BundleJob> bundles = new ArrayList<BundleJob>();
        if (context.getClusterName() == null) {
            return bundles;
        }

        OozieClient ozClient = OozieClientFactory.get(context.getClusterName());
        return ozClient.getBundleJobsInfo("name=FALCON_PROCESS_" + context.getProcessName(), 0, 10);
    }

    public static boolean killOozieJobs(TestContext context) throws Exception {
        if (context.getCluster() == null) {
            return true;
        }

        OozieClient ozClient = getOozieClient(context);
        List<BundleJob> bundles = getBundles(context);
        if (bundles != null) {
            for (BundleJob bundle : bundles) {
                ozClient.kill(bundle.getId());
            }
        }

        return false;
    }

    public static List<WorkflowJob> waitForProcessWFtoStart(TestContext context) throws Exception {
        return waitForWorkflowStart(context, context.getProcessName());
    }

    public static void waitForInstanceToComplete(TestContext context, String jobId) throws Exception {
        OozieClient ozClient = getOozieClient(context);
        String lastStatus = null;
        for (int i = 0; i < 50; i++) {
            WorkflowJob job = ozClient.getJobInfo(jobId);
            lastStatus = job.getStatus().name();
            if (OozieWorkflowEngine.WF_RERUN_PRECOND.contains(job.getStatus())) {
                return;
            }
            System.out.println("Waiting for workflow to start");
            Thread.sleep(i * 1000);
        }
        throw new Exception("Instance " + jobId + " hans't completed. Last known state " + lastStatus);
    }

    public static List<WorkflowJob> waitForWorkflowStart(TestContext context, String entityName) throws Exception {
        for (int i = 0; i < 10; i++) {
            List<WorkflowJob> jobs = getRunningJobs(context, entityName);
            if (jobs != null && !jobs.isEmpty()) {
                return jobs;
            }

            System.out.println("Waiting for workflow to start");
            Thread.sleep(i * 1000);
        }

        throw new Exception("Workflow for " + entityName + " hasn't started in oozie");
    }

    private static List<WorkflowJob> getRunningJobs(TestContext context, String entityName) throws Exception {
        OozieClient ozClient = getOozieClient(context);
        return ozClient.getJobsInfo(
                ProxyOozieClient.FILTER_STATUS + '=' + Job.Status.RUNNING + ';'
                        + ProxyOozieClient.FILTER_NAME + '=' + "FALCON_PROCESS_DEFAULT_" + entityName);
    }

    public static void waitForBundleStart(TestContext context, Job.Status... status) throws Exception {
        List<BundleJob> bundles = getBundles(context);
        if (bundles.isEmpty()) {
            return;
        }

        waitForBundleStart(context, bundles.get(0).getId(), status);
    }

    public static void waitForBundleStart(UnitTestContext context, Job.Status... status) throws Exception {
        List<BundleJob> bundles = getBundles(context);
        if (bundles.isEmpty()) {
            return;
        }

        waitForBundleStart(context, bundles.get(0).getId(), status);
    }

    public static void waitForBundleStart(TestContext context, String bundleId, Job.Status... status) throws Exception {
        OozieClient ozClient = getOozieClient(context);
        Set<Job.Status> statuses = new HashSet<Job.Status>(Arrays.asList(status));

        Status bundleStatus = null;
        for (int i = 0; i < 15; i++) {
            Thread.sleep(i * 1000);
            BundleJob bundle = ozClient.getBundleJobInfo(bundleId);
            bundleStatus = bundle.getStatus();
            if (statuses.contains(bundleStatus)) {
                if (statuses.contains(Job.Status.FAILED) || statuses.contains(Job.Status.KILLED)) {
                    return;
                }

                boolean done = false;
                for (CoordinatorJob coord : bundle.getCoordinators()) {
                    if (statuses.contains(coord.getStatus())) {
                        done = true;
                    }
                }
                if (done) {
                    return;
                }
            }
            System.out.println("Waiting for bundle " + bundleId + " in " + statuses + " state");
        }
        throw new Exception("Bundle " + bundleId + " is not " + statuses + ". Last seen status " + bundleStatus);
    }

    public static void waitForBundleStart(UnitTestContext context, String bundleId, Job.Status... status) throws
            Exception {
        OozieClient ozClient = getOozieClient(context);
        Set<Job.Status> statuses = new HashSet<Job.Status>(Arrays.asList(status));

        Status bundleStatus = null;
        for (int i = 0; i < 15; i++) {
            Thread.sleep(i * 1000);
            BundleJob bundle = ozClient.getBundleJobInfo(bundleId);
            bundleStatus = bundle.getStatus();
            if (statuses.contains(bundleStatus)) {
                if (statuses.contains(Job.Status.FAILED) || statuses.contains(Job.Status.KILLED)) {
                    return;
                }

                boolean done = false;
                for (CoordinatorJob coord : bundle.getCoordinators()) {
                    if (statuses.contains(coord.getStatus())) {
                        done = true;
                    }
                }
                if (done) {
                    return;
                }
            }
            System.out.println("Waiting for bundle " + bundleId + " in " + statuses + " state");
        }
        throw new Exception("Bundle " + bundleId + " is not " + statuses + ". Last seen status " + bundleStatus);
    }

    public static WorkflowJob getWorkflowJob(Cluster cluster, String filter) throws Exception {
        OozieClient ozClient = getOozieClient(cluster);

        List<WorkflowJob> jobs;
        while (true) {
            jobs = ozClient.getJobsInfo(filter);
            System.out.println("jobs = " + jobs);
            if (jobs.size() > 0) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }

        WorkflowJob jobInfo = jobs.get(0);
        while (true) {
            if (!(jobInfo.getStatus() == WorkflowJob.Status.RUNNING
                    || jobInfo.getStatus() == WorkflowJob.Status.PREP)) {
                break;
            } else {
                Thread.sleep(1000);
                jobInfo = ozClient.getJobInfo(jobInfo.getId());
                System.out.println("jobInfo = " + jobInfo);
            }
        }

        return jobInfo;
    }

    public static Path getOozieLogPath(Cluster cluster, WorkflowJob jobInfo) throws Exception {
        Path stagingPath = EntityUtil.getLogPath(cluster, cluster);
        final Path logPath = new Path(ClusterHelper.getStorageUrl(cluster), stagingPath);
        WorkflowExecutionContext context = WorkflowExecutionContext.create(new String[] {
            "-workflowEngineUrl", ClusterHelper.getOozieUrl(cluster),
            "-subflowId", jobInfo.getId(),
            "-runId", "1",
            "-logDir", logPath.toString() + "/job-2012-04-21-00-00",
            "-status", "SUCCEEDED",
            "-entityType", "process",
            "-userWorkflowEngine", "pig",
        }, WorkflowExecutionContext.Type.POST_PROCESSING);

        new JobLogMover().run(context);

        return new Path(logPath, "job-2012-04-21-00-00/001/oozie.log");
    }
}

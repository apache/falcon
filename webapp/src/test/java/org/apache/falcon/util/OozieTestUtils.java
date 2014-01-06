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

import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.logging.LogMover;
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;

/**
 * Oozie Utility class for integration-tests.
 */
public final class OozieTestUtils {

    private OozieTestUtils() {
    }

    public static WorkflowJob getWorkflowJob(Cluster cluster, String filter) throws Exception {
        OozieClient ozClient = OozieClientFactory.get(cluster);

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
        LogMover.main(new String[] {
            "-workflowEngineUrl", ClusterHelper.getOozieUrl(cluster),
            "-subflowId", jobInfo.getId(), "-runId", "1",
            "-logDir", logPath.toString() + "/job-2012-04-21-00-00",
            "-status", "SUCCEEDED", "-entityType", "process",
            "-userWorkflowEngine", "pig",
        });

        return new Path(logPath, "job-2012-04-21-00-00/001/oozie.log");
    }
}

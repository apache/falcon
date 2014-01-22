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
package org.apache.hadoop.mapred;

import org.apache.falcon.JobTrackerService;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;

/**
 * Hosted Local Job runner.
 * Please note that one of org.apache.hadoop.mapred.LocalRunnerV2 or
 * org.apache.hadoop.mapred.LocalRunnerV2 is active in the project depending
 * on the profile chosen.
 */
public class LocalRunnerV1 implements JobSubmissionProtocol, JobTrackerService {

    private final JobSubmissionProtocol localProxy;
    private final JobConf conf;
    private RPC.Server server;

    public LocalRunnerV1() {
        try {
            conf = new JobConf();
            localProxy = new LocalJobRunner(conf);
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize localRunner");
        }
    }

    @Override
    public void start() throws Exception {
        String[] tracker = conf.get("mapred.job.tracker", "localhost:41021").split(":");
        server = RPC.getServer(this, tracker[0], Integer.parseInt(tracker[1]), conf);
        server.start();
    }

    @Override
    public void stop() throws Exception {
        server.stop();
    }

    @Override
    public JobID getNewJobId() throws IOException {
        return localProxy.getNewJobId();
    }

    @Override
    public JobStatus submitJob(JobID jobName, String jobSubmitDir, Credentials ts) throws IOException {
        return localProxy.submitJob(jobName, jobSubmitDir, ts);
    }

    @Override
    public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
        return localProxy.getClusterStatus(detailed);
    }

    @Override
    public AccessControlList getQueueAdmins(String queueName) throws IOException {
        return localProxy.getQueueAdmins(queueName);
    }

    @Override
    public void killJob(JobID jobid) throws IOException {
        localProxy.killJob(jobid);
    }

    @Override
    public void setJobPriority(JobID jobid, String priority) throws IOException {
        localProxy.setJobPriority(jobid, priority);
    }

    @Override
    public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
        return localProxy.killTask(taskId, shouldFail);
    }

    @Override
    public JobProfile getJobProfile(JobID jobid) throws IOException {
        return localProxy.getJobProfile(jobid);
    }

    @Override
    public JobStatus getJobStatus(JobID jobid) throws IOException {
        return localProxy.getJobStatus(jobid);
    }

    @Override
    public Counters getJobCounters(JobID jobid) throws IOException {
        return localProxy.getJobCounters(jobid);
    }

    @Override
    public TaskReport[] getMapTaskReports(JobID jobid) throws IOException {
        return localProxy.getMapTaskReports(jobid);
    }

    @Override
    public TaskReport[] getReduceTaskReports(JobID jobid) throws IOException {
        return localProxy.getReduceTaskReports(jobid);
    }

    @Override
    public TaskReport[] getCleanupTaskReports(JobID jobid) throws IOException {
        return localProxy.getCleanupTaskReports(jobid);
    }

    @Override
    public TaskReport[] getSetupTaskReports(JobID jobid) throws IOException {
        return localProxy.getSetupTaskReports(jobid);
    }

    @Override
    public String getFilesystemName() throws IOException {
        return localProxy.getFilesystemName();
    }

    @Override
    public JobStatus[] jobsToComplete() throws IOException {
        return localProxy.jobsToComplete();
    }

    @Override
    public JobStatus[] getAllJobs() throws IOException {
        return localProxy.getAllJobs();
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid, int fromEventId, int maxEvents)
        throws IOException {
        return localProxy.getTaskCompletionEvents(jobid, fromEventId, maxEvents);
    }

    @Override
    public String[] getTaskDiagnostics(TaskAttemptID taskId) throws IOException {
        return localProxy.getTaskDiagnostics(taskId);
    }

    @Override
    public String getSystemDir() {
        return localProxy.getSystemDir();
    }

    @Override
    public String getStagingAreaDir() throws IOException {
        return localProxy.getStagingAreaDir();
    }

    @Override
    public JobQueueInfo[] getQueues() throws IOException {
        return localProxy.getQueues();
    }

    @Override
    public JobQueueInfo getQueueInfo(String queue) throws IOException {
        return localProxy.getQueueInfo(queue);
    }

    @Override
    public JobStatus[] getJobsFromQueue(String queue) throws IOException {
        return localProxy.getJobsFromQueue(queue);
    }

    @Override
    public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException {
        return localProxy.getQueueAclsForCurrentUser();
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException, InterruptedException {
        return new Token<DelegationTokenIdentifier>(null, null, null, null);
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException, InterruptedException {
        return localProxy.renewDelegationToken(token);
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException, InterruptedException {
        localProxy.cancelDelegationToken(token);
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return localProxy.getProtocolVersion(protocol, clientVersion);
    }
}

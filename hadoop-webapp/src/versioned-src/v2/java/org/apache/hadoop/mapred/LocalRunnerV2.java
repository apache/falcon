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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;

/**
 * Local Job Runner for Hadoop v2.
 * Please note that one of org.apache.hadoop.mapred.LocalRunnerV2 or
 * org.apache.hadoop.mapred.LocalRunnerV2 is active in the project depending
 * on the profile chosen.
 */
public class LocalRunnerV2 implements ClientProtocol, JobTrackerService {

    private final ClientProtocol localProxy;
    private final Configuration conf;
    private Server server;

    public LocalRunnerV2() {
        try {
            conf = new Configuration();
            localProxy = new LocalJobRunner(conf);
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize localRunner");
        }
    }

    @Override
    public void start() throws Exception {
        server = new RPC.Builder(conf).setBindAddress("0.0.0.0").setPort(41021).setInstance(this).
                setProtocol(ClientProtocol.class).build();
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    @Override
    public JobID getNewJobID() throws IOException, InterruptedException {
        return localProxy.getNewJobID();
    }

    @Override
    public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
            throws IOException, InterruptedException {
        return localProxy.submitJob(jobId, jobSubmitDir, ts);
    }

    @Override
    public ClusterMetrics getClusterMetrics() throws IOException, InterruptedException {
        return localProxy.getClusterMetrics();
    }

    @Override
    public Cluster.JobTrackerStatus getJobTrackerStatus() throws IOException, InterruptedException {
        return localProxy.getJobTrackerStatus();
    }

    @Override
    public long getTaskTrackerExpiryInterval() throws IOException, InterruptedException {
        return localProxy.getTaskTrackerExpiryInterval();
    }

    @Override
    public AccessControlList getQueueAdmins(String queueName) throws IOException {
        return localProxy.getQueueAdmins(queueName);
    }

    @Override
    public void killJob(JobID jobid) throws IOException, InterruptedException {
        localProxy.killJob(jobid);
    }

    @Override
    public void setJobPriority(JobID jobid, String priority) throws IOException, InterruptedException {
        localProxy.setJobPriority(jobid, priority);
    }

    @Override
    public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException, InterruptedException {
        return localProxy.killTask(taskId, shouldFail);
    }

    @Override
    public JobStatus getJobStatus(JobID jobid) throws IOException, InterruptedException {
        return localProxy.getJobStatus(jobid);
    }

    @Override
    public Counters getJobCounters(JobID jobid) throws IOException, InterruptedException {
        return localProxy.getJobCounters(jobid);
    }

    @Override
    public TaskReport[] getTaskReports(JobID jobid, TaskType type) throws IOException, InterruptedException {
        return localProxy.getTaskReports(jobid, type);
    }

    @Override
    public String getFilesystemName() throws IOException, InterruptedException {
        return localProxy.getFilesystemName();
    }

    @Override
    public JobStatus[] getAllJobs() throws IOException, InterruptedException {
        return localProxy.getAllJobs();
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid, int fromEventId, int maxEvents)
            throws IOException, InterruptedException {
        return localProxy.getTaskCompletionEvents(jobid, fromEventId, maxEvents);
    }

    @Override
    public String[] getTaskDiagnostics(TaskAttemptID taskId) throws IOException, InterruptedException {
        return localProxy.getTaskDiagnostics(taskId);
    }

    @Override
    public TaskTrackerInfo[] getActiveTrackers() throws IOException, InterruptedException {
        return localProxy.getActiveTrackers();
    }

    @Override
    public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException, InterruptedException {
        return localProxy.getBlacklistedTrackers();
    }

    @Override
    public String getSystemDir() throws IOException, InterruptedException {
        return localProxy.getSystemDir();
    }

    @Override
    public String getStagingAreaDir() throws IOException, InterruptedException {
        return localProxy.getStagingAreaDir();
    }

    @Override
    public String getJobHistoryDir() throws IOException, InterruptedException {
        return localProxy.getJobHistoryDir();
    }

    @Override
    public QueueInfo[] getQueues() throws IOException, InterruptedException {
        return localProxy.getQueues();
    }

    @Override
    public QueueInfo getQueue(String queueName) throws IOException, InterruptedException {
        return localProxy.getQueue(queueName);
    }

    @Override
    public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException, InterruptedException {
        return localProxy.getQueueAclsForCurrentUser();
    }

    @Override
    public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
        return localProxy.getRootQueues();
    }

    @Override
    public QueueInfo[] getChildQueues(String queueName) throws IOException, InterruptedException {
        return localProxy.getChildQueues(queueName);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException, InterruptedException {
        return localProxy.getDelegationToken(renewer);
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
    public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID)
            throws IOException, InterruptedException {
        return localProxy.getLogFileParams(jobID, taskAttemptID);
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return localProxy.getProtocolVersion(protocol, clientVersion);
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
            throws IOException {
        return localProxy.getProtocolSignature(protocol, clientVersion, clientMethodsHash);
    }
}

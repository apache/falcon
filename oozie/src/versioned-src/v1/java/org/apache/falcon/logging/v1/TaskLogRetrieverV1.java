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

package org.apache.falcon.logging.v1;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.falcon.logging.DefaultTaskLogRetriever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hadoop v1 task log retriever based on job history
 */
public final class TaskLogRetrieverV1 extends DefaultTaskLogRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(TaskLogRetrieverV1.class);

    @Override
    public String getFromHistory(String jobId) throws IOException {
        Configuration conf = getConf();
        String file = getHistoryFile(conf, jobId);
        if (file == null) return null;
        JobHistory.JobInfo jobInfo = new JobHistory.JobInfo(jobId);
        DefaultJobHistoryParser.parseJobTasks(file, jobInfo, new Path(file).getFileSystem(conf));
        LOG.info("History file: {}", file);
        LOG.debug("Number of tasks in the history file: {}", jobInfo.getAllTasks().size());
        for (JobHistory.Task task : jobInfo.getAllTasks().values()) {
            if (task.get(JobHistory.Keys.TASK_TYPE).equals(JobHistory.Values.MAP.name()) &&
                    task.get(JobHistory.Keys.TASK_STATUS).equals(JobHistory.Values.SUCCESS.name())) {
                for (JobHistory.TaskAttempt attempt : task.getTaskAttempts().values()) {
                    if (attempt.get(JobHistory.Keys.TASK_STATUS).equals(JobHistory.Values.SUCCESS.name())) {
                        return JobHistory.getTaskLogsUrl(attempt);
                    }
                }
            }
        }
        LOG.warn("Unable to find successful map task attempt");
        return null;
    }

    private String getHistoryFile(Configuration conf, String jobId) throws IOException {
        String jtAddress = "scheme://" + conf.get("mapred.job.tracker");
        String jtHttpAddr = "scheme://" + conf.get("mapred.job.tracker.http.address");
        try {
            String host = new URI(jtAddress).getHost();
            int port = new URI(jtHttpAddr).getPort();
            HttpClient client = new HttpClient();
            String jobUrl = "http://" + host + ":" + port + "/jobdetails.jsp";
            GetMethod get = new GetMethod(jobUrl);
            get.setQueryString("jobid=" + jobId);
            get.setFollowRedirects(false);
            int status = client.executeMethod(get);
            String file = null;
            if (status == HttpStatus.SC_MOVED_PERMANENTLY || status == HttpStatus.SC_MOVED_TEMPORARILY) {
                file = get.getResponseHeader("Location").toString();
                file = file.substring(file.lastIndexOf('=') + 1);
                file = JobHistory.JobInfo.decodeJobHistoryFileName(file);
            } else {
                LOG.warn("JobURL {} for id: {} returned {}", jobUrl, jobId, status);
            }
            return file;
        } catch (URISyntaxException e) {
            throw new IOException("JT Address: " + jtAddress + ", http Address: " + jtHttpAddr, e);
        }
    }
}

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
package org.apache.falcon.logging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Task log retriever for jobs running in yarn.
 */
public class TaskLogRetrieverYarn extends DefaultTaskLogRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(TaskLogRetrieverYarn.class);
    protected static final String SCHEME = "http://";
    protected static final String YARN_LOG_SERVER_URL = "yarn.log.server.url";

    @Override
    public List<String> retrieveTaskLogURL(String jobIdStr) throws IOException {
        List<String> taskLogUrls = new ArrayList<String>();
        Configuration conf = getConf();
        Cluster cluster = getCluster(conf);
        JobID jobID = JobID.forName(jobIdStr);
        if (jobID == null) {
            LOG.warn("External id for workflow action is null");
            return null;
        }

        if (conf.get(YARN_LOG_SERVER_URL) == null) {
            LOG.warn("YARN log Server is null");
            return null;
        }

        try {
            Job job = cluster.getJob(jobID);
            if (job != null) {
                TaskCompletionEvent[] events = job.getTaskCompletionEvents(0);
                for (TaskCompletionEvent event : events) {
                    LogParams params = cluster.getLogParams(jobID, event.getTaskAttemptId());
                    String url = (conf.get(YARN_LOG_SERVER_URL).startsWith(SCHEME)
                            ? conf.get(YARN_LOG_SERVER_URL)
                            : SCHEME + conf.get(YARN_LOG_SERVER_URL))
                            + "/"
                            + event.getTaskTrackerHttp() + "/"
                            + params.getContainerId() + "/"
                            + params.getApplicationId() + "/"
                            + params.getOwner() + "?start=0";
                    LOG.info("Task Log URL for the job {} is {}" + jobIdStr, url);
                    taskLogUrls.add(url);
                }
                return taskLogUrls;
            }
            LOG.warn("Unable to find the job in cluster {}" + jobIdStr);
            return null;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected Cluster getCluster(Configuration conf) throws IOException {
        return new Cluster(conf);
    }
}

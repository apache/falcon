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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default task log retriever. By default checks the hadoop runningJob.
 */
public class DefaultTaskLogRetriever extends Configured implements TaskLogURLRetriever {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskLogRetriever.class);

    @Override
    public String retrieveTaskLogURL(String jobId) throws IOException {
        JobConf jobConf = new JobConf(getConf());
        JobClient jobClient = new JobClient(jobConf);

        RunningJob job = jobClient.getJob(JobID.forName(jobId));
        if (job == null) {
            LOG.warn("No running job for job id: {}", jobId);
            return getFromHistory(jobId);
        }
        TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0);
        // 0th even is setup, 1 event is launcher, 2 event is cleanup
        if (tasks != null && tasks.length == 3 && tasks[1] != null) {
            return tasks[1].getTaskTrackerHttp() + "/tasklog?attemptid="
                    + tasks[1].getTaskAttemptId() + "&all=true";
        } else {
            LOG.warn("No running task for job: {}", jobId);
            return getFromHistory(jobId);
        }
    }

    protected String getFromHistory(String jodId) throws IOException {
        return null;
    }
}

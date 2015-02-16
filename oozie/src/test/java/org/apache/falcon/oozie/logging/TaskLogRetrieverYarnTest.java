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

package org.apache.falcon.oozie.logging;

import org.apache.falcon.logging.TaskLogRetrieverYarn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for TaskLog Retrieval in Yarn.
 */
public class TaskLogRetrieverYarnTest extends TaskLogRetrieverYarn {

    private Cluster mockcluster;
    private Configuration conf = null;
    private static Random random = new Random();


    @DataProvider(name = "testData")
    public Object[][] testData() throws IOException, InterruptedException {
        int samples = getRandomValueInRange(10) + 1;
        Object[][] resultSet = new Object[samples][2];
        for (int count = 0; count < samples; count++) {
            List<String> expectedResult = new ArrayList<String>();
            Cluster cluster = getCluster(getConf());
            String jobId = new JobID("job", random.nextInt(1000)).toString();
            boolean success = random.nextBoolean();
            JobID jobID = JobID.forName(jobId);
            int numEvents = getRandomValueInRange(10) + 1;
            TaskCompletionEvent[] events = getTaskCompletionEvents(numEvents, jobID);
            Job job = mock(Job.class);
            when(cluster.getJob(jobID)).thenReturn(job);
            when(job.getTaskCompletionEvents(0)).thenReturn(events);
            for (TaskCompletionEvent event : events) {
                if (success) {
                    LogParams params = getLogParams();
                    when(cluster.getLogParams(jobID, event.getTaskAttemptId())).thenReturn(params);
                    String url = SCHEME + getConf().get(YARN_LOG_SERVER_URL) + "/"
                            + event.getTaskTrackerHttp() + "/"
                            + params.getContainerId() + "/"
                            + params.getApplicationId() + "/"
                            + params.getOwner() + "?start=0";
                    expectedResult.add(url);
                } else {
                    when(cluster.getJob(jobID)).thenReturn(null);
                    expectedResult = null;
                }
                resultSet[count] = new Object[]{jobId, expectedResult};
            }
        }
        return resultSet;
    }

    @Test(dataProvider = "testData")
    public void testSuccess(String jobId, List<String> expectedResult) throws Exception {
        List<String> actual = this.retrieveTaskLogURL(jobId);
        Assert.assertEquals(actual, expectedResult);
    }

    @Override
    protected Cluster getCluster(Configuration configuration) {
        if (mockcluster == null) {
            this.mockcluster = mock(Cluster.class);
        }
        return mockcluster;
    }

    @Override
    public Configuration getConf() {
        if (conf == null) {
            conf = new Configuration();
            conf.set(YARN_LOG_SERVER_URL, "host:4000");
        }
        return conf;
    }


    private TaskCompletionEvent[] getTaskCompletionEvents(int numEvents, JobID jobID) {
        TaskCompletionEvent[] taskCompletionEvents = new TaskCompletionEvent[numEvents];
        for (int i = 0; i < numEvents; i++) {
            TaskAttemptID taskAttemptID = new TaskAttemptID(new TaskID(jobID, true, 0), i);
            TaskCompletionEvent taskCompletionEvent = new TaskCompletionEvent(0, taskAttemptID, 0,
                    true, TaskCompletionEvent.Status.SUCCEEDED, "tracker:0");
            taskCompletionEvents[i] = taskCompletionEvent;
        }
        return taskCompletionEvents;
    }

    private LogParams getLogParams() {
        int containerIndex = getRandomValueInRange(10);
        return new LogParams("c" + containerIndex, "a1", "n1", "own1");
    }

    private int getRandomValueInRange(int range) {
        return random.nextInt(range);
    }
}
